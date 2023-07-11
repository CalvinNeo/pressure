#![feature(iter_intersperse)]
#![feature(iter_array_chunks)]
#![feature(async_closure)]

use std::{
    borrow::BorrowMut,
    collections::HashMap,
    default,
    fs::File,
    future::Future,
    io::{self, BufRead, BufReader},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize},
        Arc, RwLock,
    },
    thread::{self, JoinHandle},
};

use clap::Parser;
use crossbeam::epoch::Atomic;
use crossbeam_channel::{bounded, select, Receiver};
use itertools::Itertools;
// use mysql::{prelude::*, *};
use mysql_async::{prelude::*, *};
use rand::{prelude::IteratorRandom, Rng};
use threadpool::ThreadPool;
use tokio::{runtime::Runtime, sync::oneshot};

const STEP: u64 = 20;

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
        let mut v: Vec<T> = vec![];
        // We don't use iter().choose_multiple() since it is O(n).
        v.reserve(n);
        for i in 0..n {
            let j: usize = rng.gen_range(0usize..self.items.len());
            v.push(self.items[j].clone());
        }
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
        let prefetched: Vec<T> = feeder.sampler.as_ref().sample(expect_len, true);
        println!("finished prefetch of total {}", prefetched.len());
        let mut iter = prefetched.into_iter();
        for i in 0..slot_count {
            println!("start create slot {}", i);
            let mut v: Vec<T> = vec![];
            v.reserve(slot_size);
            for _j in 0..slot_size {
                v.push(iter.next().unwrap());
            }
            let slot = Slot::new_with_prefetched(v);
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
        println!(
            "start update slot {} [thread_id={:?}]",
            slot_id,
            std::thread::current().id()
        );
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
                    // Not Survive
                } else {
                    // Survive, Replace a random one.
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
            feeder,
            dry_run,
            acc: Arc::new(AtomicU64::new(0)),
            no_print_data,
            batch_size,
        };
        r
    }

    fn start(
        &self,
        n_threads: usize,
        rt: &Runtime,
        finished: Arc<AtomicBool>,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        let _g2 = rt.enter();
        let mut rv: Vec<tokio::task::JoinHandle<()>> = vec![];
        for thid in 0..n_threads {
            let thread_id = thid.clone();
            let urls = self.urls.clone();
            let feeder = self.feeder.clone();
            let dry_run = self.dry_run.clone();
            let no_print_data = self.no_print_data.clone();
            let acc = self.acc.clone();
            let batch_size = self.batch_size.clone();
            let finished = finished.clone();
            let f = async move || {
                let mut count = 0;
                let mut pools: Vec<Pool> = vec![];
                if !dry_run {
                    for url in urls.iter() {
                        let pool = Pool::new(url.as_str());
                        pools.push(pool);
                    }
                }
                let mut total_elapsed: u128 = 0;
                let mut total_elapsed_count: usize = 0;
                loop {
                    if finished.load(std::sync::atomic::Ordering::SeqCst) {
                        for pool in pools.into_iter() {
                            pool.disconnect().await;
                        }
                        break;
                    }
                    let (s, tidb_id) = {
                        // let mut rng_inner = tokio::sync::RwLock::new(rand::thread_rng());
                        // let rng = rng_inner.read().await;
                        let mut rng = rand::thread_rng();
                        let tidb_id: usize = rng.gen_range(0usize..urls.len());
                        // Let's create a table for payments.
                        let random_string: String =
                            (0..5).map(|_| rng.gen_range(b'a'..=b'z') as char).collect();
                        (Iterator::intersperse(feeder.sample(batch_size as usize).into_iter().map(|e| {
                            format!("update rtdb.zto_ssmx_bill_detail set forecast_stat_day = '{}' where bill_code='{}';", random_string, e)
                        }), "\n".to_string()).collect(), tidb_id)
                    };
                    if dry_run {
                        if !no_print_data {
                            println!(
                                "task_id {} tidb_id {} sql {} [thread_id={:?}]",
                                thread_id,
                                tidb_id,
                                s,
                                std::thread::current().id()
                            );
                        }
                    } else {
                        let start = std::time::Instant::now();
                        let s: String = s;
                        let mut conn = pools[tidb_id].get_conn().await.unwrap();
                        conn.query_drop(s).await.unwrap();
                        drop(conn);
                        total_elapsed += start.elapsed().as_millis();
                        total_elapsed_count += 1;
                    }
                    count += 1;
                    if count % 2000 == 0 {
                        println!(
                            "task_id {} finished {} average delay {} [thread_id={:?}]",
                            thread_id,
                            count,
                            total_elapsed as f64 / total_elapsed_count as f64,
                            std::thread::current().id()
                        );
                        total_elapsed = 0;
                        total_elapsed_count = 0;
                    }
                    if count % STEP == 0 {
                        acc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                }
                ()
            };
            rv.push(rt.spawn(f()));
        }
        rv
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
    #[arg(short, long, default_value_t = 7)]
    workers: usize,

    /// MySQL tasks.
    #[arg(long, default_value_t = 7)]
    tasks: usize,

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

fn ctrl_channel() -> std::result::Result<Receiver<()>, ctrlc::Error> {
    let (sender, receiver) = bounded(100);
    ctrlc::set_handler(move || {
        let _ = sender.send(());
    })?;

    Ok(receiver)
}

/// Consider b regions and k samples, then estimation of coverred regions is b * (1 - ((b - 1) / b) ** k).
/// ./target/release/pressure --tidb-addrs mysql://root@172.31.7.1:4000/,mysql://root@172.31.7.2:4000/,mysql://root@172.31.7.3:4000/,mysql://root@172.31.7.4:4000/ --input-files /home/ubuntu/tiflash-u2/pk_0,/home/ubuntu/tiflash-u2/pk_1,/home/ubuntu/tiflash-u2/pk_2,/home/ubuntu/tiflash-u2/pk_3,/home/ubuntu/tiflash-u2/pk_4,/home/ubuntu/tiflash-u2/pk_5 -s 5000000 --update-interval-millis 5000 --batch-size 1 --slot-count 50 --workers 100
/// ./target/debug/pressure --tidb-addrs mysql://root@127.0.0.1:4000/ --input-files /Users/calvin/pressure/pressure/a,/Users/calvin/pressure/pressure/b,/Users/calvin/pressure/pressure/c -s 1 --update-interval-millis 100 --slot-count 1

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
    let addrs: Vec<String> = args.tidb_addrs.split(",").map(|e| e.to_string()).collect();
    let iss: MySQLIssuer = MySQLIssuer::new(
        addrs,
        feeder.clone(),
        args.dry_run,
        args.no_print_data,
        args.batch_size,
    );
    let rt = Runtime::new().unwrap();
    let iss_acc = iss.acc.clone();
    let (bg_qps, bg_fut) = {
        let _g = rt.enter();
        let feed = feeder.clone();
        let bg_qps = rt.spawn(async move {
            let mut prev = 0;
            loop {
                let i = std::time::Instant::now();
                tokio::time::sleep(std::time::Duration::from_millis(5000)).await;
                let c = iss_acc.load(std::sync::atomic::Ordering::SeqCst) * STEP;
                let d = (c - prev) as f64 / 5.0;
                prev = c;
                println!(
                    "QPS {} [thread_id={:?}]",
                    d / i.elapsed().as_secs_f64(),
                    std::thread::current().id()
                );
            }
        });
        let bg_fut = rt.spawn(background_update(feed));
        (bg_qps, bg_fut)
    };

    let thread_count = Arc::new(AtomicUsize::new(0));
    let thread_count_c = thread_count.clone();
    let rt2 = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.workers)
        .enable_all()
        .on_thread_start(move || {
            thread_count_c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        })
        .thread_name("issuer")
        .build()
        .unwrap();
    /// The following not works.
    // let rt2 = tokio::runtime::Builder::new_current_thread()
    //     .enable_all()
    //     .build()
    //     .unwrap();
    let f2 = Arc::new(AtomicBool::new(false));
    let tvs = iss.start(args.tasks, &rt2, f2.clone());

    println!(
        "====== task count {} exes {} =====",
        tvs.len(),
        thread_count.load(std::sync::atomic::Ordering::SeqCst)
    );

    let ctrl_c_events = ctrl_channel().unwrap();
    loop {
        select! {
            recv(ctrl_c_events) -> _ => {
                println!("Goodbye!");
                f2.store(true, std::sync::atomic::Ordering::SeqCst);
                bg_fut.abort();
                bg_qps.abort();
                for k in tvs.into_iter() {
                    rt2.block_on(k);
                }
                rt.shutdown_timeout(std::time::Duration::from_millis(1));
                rt2.shutdown_timeout(std::time::Duration::from_millis(1));
                println!("Quit!");
                break;
            }
        }
    }
}
