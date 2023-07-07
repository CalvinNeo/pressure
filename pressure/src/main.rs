#![feature(iter_intersperse)]
use std::{
    fs::File,
    future::Future,
    io::{self, BufRead, BufReader},
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc, RwLock,
    },
    thread::{self, JoinHandle},
};

use clap::Parser;
use crossbeam::epoch::Atomic;
use crossbeam_channel::{bounded, select, Receiver};
use itertools::Itertools;
use mysql::{prelude::*, *};
use rand::{prelude::IteratorRandom, Rng};
use threadpool::ThreadPool;
use tokio::{runtime::Runtime, sync::oneshot};

const STEP: u64 = 100;

trait Sampler<T: Clone + Sync>: Send + Sync {
    fn sample(&self, n: usize, _: bool) -> Vec<T>;
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
        s.items.append(&mut sampler.sample(size, false));
        s
    }

    fn new_with_prefetched(prefetched: Vec<T>) -> Slot<T> {
        let mut s = Slot::default();
        let mut prefetched = prefetched;
        s.items.append(&mut prefetched);
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

impl<T: Clone + Sync + 'static> Feeder<T> {
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
        let expect_len = slot_count * slot_size;
        println!("start prefetch of total {}", expect_len);
        let prefetched = feeder.sampler.as_ref().sample(expect_len, true);
        println!("finished prefetch of total {}", prefetched.len());
        for i in 0..slot_count {
            println!("start create slot {}", i);
            let slot =
                Slot::new_with_prefetched(prefetched[i * slot_size..(i + 1) * slot_size].to_vec());
            *feeder.slots[i].write().expect("update lock error") = slot;
        }
        feeder
    }

    fn update_cell(&self, i: usize) -> usize {
        let slot_size = self.slot_size.clone();
        let sampler = self.sampler.clone();
        let slot = Slot::new(sampler, slot_size);
        let size = slot.items.len();
        *self.slots[i].write().expect("update lock error") = slot;
        size
    }

    fn sample(&self, n: usize) -> Vec<T> {
        let mut rng = rand::thread_rng();
        let slot_id: usize = rng.gen_range(0usize..self.slot_count);
        // We don't sample more than a slots's maximum items.
        assert!(n <= self.slot_count);
        self.slots[slot_id]
            .read()
            .expect("read lock error")
            .sample(n)
    }
}

async fn background_update<T: Clone + Sync + 'static>(feeder: Arc<Feeder<T>>) {
    let update_millis = feeder.update_millis.clone();
    let mut rng = rand::thread_rng();
    loop {
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
    fn sample(&self, n: usize, should_print: bool) -> Vec<String> {
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
            if should_print && c % 1000000 == 0 {
                println!("sampler load {}", c);
            }
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
    acc: Arc<AtomicU64>,
    no_print_data: bool,
    batch_size: u64,
}

impl MySQLIssuer {
    fn new(
        urls: Vec<String>,
        feeder: Arc<Feeder<String>>,
        dry_run: bool,
        no_print_data: bool,
        batch_size: u64,
    ) -> Self {
        let r = Self {
            urls: urls.clone(),
            pools: Arc::new(RwLock::new(vec![])),
            feeder,
            dry_run,
            acc: Arc::new(AtomicU64::new(0)),
            no_print_data,
            batch_size,
        };
        if !dry_run {
            for url in urls.iter() {
                let pool = Pool::new(url.as_str()).unwrap();
                r.pools.write().expect("update lock").push(pool);
            }
        }
        r
    }

    fn start(&self, n_threads: usize) -> (Arc<AtomicBool>, Vec<JoinHandle<()>>) {
        let finished = Arc::new(AtomicBool::new(false));
        let mut tv: Vec<JoinHandle<()>> = vec![];
        for thid in 0..n_threads {
            let finished = finished.clone();
            let thread_id = thid.clone();
            let urls = self.urls.clone();
            let pools = self.pools.clone();
            let feeder = self.feeder.clone();
            let dry_run = self.dry_run.clone();
            let no_print_data = self.no_print_data.clone();
            let acc = self.acc.clone();
            let batch_size = self.batch_size.clone();
            let f = move || {
                let mut count = 0;
                loop {
                    if finished.load(std::sync::atomic::Ordering::SeqCst) {
                        break;
                    }
                    let mut rng = rand::thread_rng();
                    let tidb_id: usize = rng.gen_range(0usize..urls.len());
                    // Let's create a table for payments.
                    let random_string: String = (0..5).map(|_| rng.gen_range(b'a'..=b'z') as char).collect();
                    let s: String = Iterator::intersperse(feeder.sample(batch_size as usize).into_iter().map(|e| {
                        format!("update rtdb.zto_ssmx_bill_detail set forecast_stat_day = '{}' where bill_code='{}';", random_string, e)
                    }), "\n".to_string()).collect();
                    if dry_run {
                        if !no_print_data {
                            println!("thread_id {} tidb_id {} sql {}", thread_id, tidb_id, s);
                        }
                    } else {
                        let mut conn = pools.read().expect("read lock")[tidb_id].get_conn().unwrap();
                        conn.query_drop(s).unwrap();
                    }
                    count += 1;
                    if count % 10000 == 0 {
                        println!("thread_id {} finished {}", thread_id, count);
                    }
                    if count % STEP == 0 {
                        acc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                }
            };
            tv.push(thread::spawn(f));
        }
        (finished, tv)
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

    #[arg(long, default_value_t = false)]
    no_print_data: bool,

    #[arg(long, default_value_t = 1)]
    batch_size: u64,
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
    let rt = Runtime::new().unwrap();
    let _g = rt.enter();
    let feed = feeder.clone();

    let bg_fut = rt.spawn(background_update(feed));

    let feed = feeder.clone();
    let addrs: Vec<String> = args.tidb_addrs.split(",").map(|e| e.to_string()).collect();
    let iss: MySQLIssuer = MySQLIssuer::new(
        addrs,
        feed,
        args.dry_run,
        args.no_print_data,
        args.batch_size,
    );
    let iss_acc = iss.acc.clone();
    let bg_qps = rt.spawn(async move {
        let mut prev = 0;
        loop {
            let i = std::time::Instant::now();
            tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
            let c = iss_acc.load(std::sync::atomic::Ordering::SeqCst) * STEP;
            let d = (c - prev) as f64 / 2.0;
            prev = c;
            println!("QPS {}", d / i.elapsed().as_secs_f64());
        }
    });

    let (f2, tvs) = iss.start(args.workers);

    let ctrl_c_events = ctrl_channel().unwrap();
    loop {
        select! {
            recv(ctrl_c_events) -> _ => {
                println!("Goodbye!");
                f2.store(true, std::sync::atomic::Ordering::SeqCst);
                for x in tvs.into_iter() {
                    let _ = x.join();
                }
                bg_fut.abort();
                bg_qps.abort();
                rt.shutdown_timeout(std::time::Duration::from_millis(1));
                println!("Quit!");
                break;
            }
        }
    }
}
