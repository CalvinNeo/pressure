use std::{
    fs::File,
    io::{self, BufRead, BufReader},
    sync::{atomic::AtomicBool, Arc, RwLock},
    thread::{self, JoinHandle},
};

use clap::Parser;
use crossbeam_channel::{bounded, select, Receiver};
use mysql::{prelude::*, *};
use rand::{prelude::IteratorRandom, Rng};
use threadpool::ThreadPool;

trait Sampler<T: Clone + Sync>: Send + Sync {
    fn sample(&self, n: usize) -> Vec<T>;
}
struct Slot<T: Clone + Sync> {
    items: Vec<T>,
}

unsafe impl<T: Clone + Sync> Send for Slot<T> {}

impl<T: Clone + Sync> Default for Slot<T> {
    fn default() -> Self {
        Slot { items: vec![] }
    }
}

impl<T: Clone + Sync> Slot<T> {
    fn new(sampler: Arc<dyn Sampler<T>>, size: usize) -> Slot<T> {
        let mut s = Slot::default();
        s.items.append(&mut sampler.sample(size));
        s
    }

    fn sample(&self, n: usize) -> Vec<T> {
        assert!(n <= self.items.len());
        let mut rng = rand::thread_rng();
        let v: Vec<&T> = self.items.iter().choose_multiple(&mut rng, n);
        let v: Vec<T> = v.into_iter().map(|e| e.clone()).collect();
        v
    }
}

struct Feeder<T: Clone + Sync> {
    slot_count: usize,
    slot_size: usize,
    slots: Vec<RwLock<Slot<T>>>,
    sampler: Arc<dyn Sampler<T>>,
    update_millis: u64,
}

unsafe impl<T: Clone + Sync> Send for Feeder<T> {}

impl<T: Clone + Sync> Feeder<T> {
    fn new(
        slot_count: usize,
        slot_size: usize,
        sampler: Arc<dyn Sampler<T>>,
        update_millis: u64,
    ) -> Self {
        let mut feeder = Feeder {
            slot_count,
            slot_size,
            slots: vec![],
            sampler,
            update_millis,
        };
        for _i in 0..slot_count {
            feeder.slots.push(RwLock::new(Slot::default()));
        }
        for i in 0..slot_count {
            feeder.update_cell(i);
        }
        feeder
    }

    fn update_cell(&self, i: usize) -> usize {
        let slot = Slot::new(self.sampler.clone(), self.slot_size);
        let size = slot.items.len();
        *self.slots[i].write().expect("update lock error") = slot;
        size
    }

    fn sample(&self, n: usize) -> Vec<T> {
        let mut rng = rand::thread_rng();
        let slot_id: usize = rng.gen_range(0usize..self.slot_count);
        assert!(n <= self.slot_count);
        self.slots[slot_id]
            .read()
            .expect("read lock error")
            .sample(n)
    }
}

fn background_update<T: Clone + Sync + 'static>(
    feeder: Arc<Feeder<T>>,
) -> (Arc<AtomicBool>, JoinHandle<()>) {
    let finished = Arc::new(AtomicBool::new(false));
    (
        finished.clone(),
        thread::spawn(move || {
            let finished = finished.clone();
            let update_millis = feeder.update_millis.clone();
            let mut rng = rand::thread_rng();
            loop {
                if finished.load(std::sync::atomic::Ordering::SeqCst) {
                    println!("finish");
                    return;
                }
                let slot_id: usize = rng.gen_range(0usize..feeder.slot_count);
                let now = std::time::Instant::now();
                println!("start update slot {}", slot_id);
                let new_size = feeder.update_cell(slot_id);
                println!(
                    "end update slot {} elapsed millis {} new_size {}",
                    slot_id,
                    now.elapsed().as_millis(),
                    new_size
                );
                thread::sleep(std::time::Duration::from_millis(update_millis));
            }
        }),
    )
}

struct PKSampler {
    file_names: Vec<String>,
}

fn read_lines_lazy(file_path: &str) -> impl Iterator<Item = io::Result<String>> {
    let file = File::open(file_path).unwrap();
    let reader = BufReader::new(file);
    reader.lines().map(|line| {
        line.map(|l| {
            let v: Vec<String> = l.trim().split(" ").take(1).map(|e| e.to_string()).collect();
            v.into_iter().nth(0).unwrap()
        })
    })
}

impl Sampler<String> for PKSampler {
    fn sample(&self, n: usize) -> Vec<String> {
        // Reservoir Sampling
        let mut res: Vec<String> = vec![];
        let mut iter_set: Vec<Box<dyn Iterator<Item = io::Result<String>>>> = vec![];

        for i in self.file_names.iter() {
            iter_set.push(Box::new(read_lines_lazy(i.as_str())));
        }

        let final_iter = iter_set.into_iter().flatten();

        let mut c: usize = 0;
        for s in final_iter {
            let s = s.unwrap();
            // println!("see {} sample n = {}", s, n);
            if c < n {
                res.push(s);
            } else {
                let survive = n as f64 / c as f64;
                let mut rng = rand::thread_rng();
                let random_float: f64 = rng.gen_range(0.0..1.0);
                if random_float > survive {
                    // Not Survive, Replace a random one.
                    let index = rng.gen_range(0..res.len());
                    res[index] = s;
                }
            }
            c += 1;
        }
        res
    }
}

struct MySQLIssuer {
    urls: Vec<String>,
    pools: Arc<RwLock<Vec<Pool>>>,
    feeder: Arc<Feeder<String>>,
    dry_run: bool,
}

impl MySQLIssuer {
    fn new(urls: Vec<String>, feeder: Arc<Feeder<String>>, dry_run: bool) -> Self {
        let r = Self {
            urls: urls.clone(),
            pools: Arc::new(RwLock::new(vec![])),
            feeder,
            dry_run,
        };
        if !dry_run {
            for url in urls.iter() {
                let pool = Pool::new(url.as_str()).unwrap();
                r.pools.write().expect("update lock").push(pool);
            }
        }
        r
    }

    fn start(&self, n_threads: usize) -> (Arc<AtomicBool>, ThreadPool) {
        let thread_pool = ThreadPool::with_name("issuer".into(), n_threads);
        let finished = Arc::new(AtomicBool::new(false));
        for thid in 0..n_threads {
            let finished = finished.clone();
            let thread_id = thid.clone();
            let urls = self.urls.clone();
            let pools = self.pools.clone();
            let feeder = self.feeder.clone();
            let dry_run = self.dry_run.clone();
            thread_pool.execute(move || {
                let mut count = 0;
                loop {
                    if finished.load(std::sync::atomic::Ordering::SeqCst) {
                        break;
                    }
                    let mut rng = rand::thread_rng();
                    let tidb_id: usize = rng.gen_range(0usize..urls.len());
                    // Let's create a table for payments.
                    let random_string: String = (0..5).map(|_| rng.gen_range(b'a'..=b'z') as char).collect();
                    let billcode = feeder.sample(1).into_iter().nth(0).unwrap();
                    let s = format!("update rtdb.zto_ssmx_bill_detail set forecast_stat_day = '{}' where bill_code='{}'", random_string, billcode);
                    if dry_run {
                        println!("thread_id {} tidb_id {} sql {}", thread_id, tidb_id, s);
                    } else {
                        let mut conn = pools.read().expect("read lock")[tidb_id].get_conn().unwrap();
                        conn.query_drop(s).unwrap();
                    }
                    count += 1;
                    if count % 10000 == 0 {
                        println!("thread_id {} finished {}", thread_id, count);
                    }
                }
            });
        }
        (finished, thread_pool)
    }
}

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct PKIssueArgs {
    /// TiDB endpoints splitted by `,`, like mysql://root@127.0.0.1:4000/
    #[arg(short, long)]
    tidb_addrs: String,

    /// Input files splitted by `,`.
    #[arg(short, long)]
    input_files: String,

    /// MySQL workers.
    #[arg(short, long, default_value_t = 6)]
    workers: usize,

    /// Interval to update a random slot.
    #[arg(long, default_value_t = 10000)]
    update_interval_millis: u64,

    /// Perform dry run
    #[arg(long, default_value_t = false)]
    dry_run: bool,

    /// How many slots.
    #[arg(long, default_value_t = 100)]
    slot_count: usize,

    /// How many items in one slot.
    #[arg(short, long)]
    slot_size: usize,
}

fn ctrl_channel() -> Result<Receiver<()>, ctrlc::Error> {
    let (sender, receiver) = bounded(100);
    ctrlc::set_handler(move || {
        let _ = sender.send(());
    })?;

    Ok(receiver)
}

/// ./target/debug/pressure --dry-run --tidb-addrs mysql://root@127.0.0.1:4000/ --input-files /Users/calvin/pressure/pressure/a,/Users/calvin/pressure/pressure/b,/Users/calvin/pressure/pressure/c -s 1000 --update-interval-millis 100
fn main() {
    let args = PKIssueArgs::parse();
    let file_names = args.input_files.split(",").map(|e| e.to_string()).collect();
    let pk = PKSampler { file_names };
    let feeder: Arc<Feeder<String>> = Arc::new(Feeder::new(
        args.slot_count,
        args.slot_size,
        Arc::new(pk),
        args.update_interval_millis,
    ));
    let (f, j) = background_update(feeder.clone());
    let feed = feeder.clone();

    let addrs: Vec<String> = args.tidb_addrs.split(",").map(|e| e.to_string()).collect();
    let iss: MySQLIssuer = MySQLIssuer::new(addrs, feed, args.dry_run);
    let (f2, thread_pool) = iss.start(args.workers);

    let ctrl_c_events = ctrl_channel().unwrap();
    loop {
        select! {
            recv(ctrl_c_events) -> _ => {
                println!("Goodbye!");
                f.store(true, std::sync::atomic::Ordering::SeqCst);
                f2.store(true, std::sync::atomic::Ordering::SeqCst);
                j.join().unwrap();
                thread_pool.join();
                println!("Quit!");
                break;
            }
        }
    }
}
