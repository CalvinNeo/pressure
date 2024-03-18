#![feature(iter_intersperse)]
#![feature(iter_array_chunks)]
#![feature(async_closure)]

mod feeder;
mod issuer;
use std::{
    fs::File,
    io::{self, BufRead, BufReader, Write},
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc,
    },
};
use std::collections::HashMap;
use clap::Parser;
use crossbeam_channel::{bounded, select, Receiver};
use feeder::*;
use issuer::*;
// use mysql::{prelude::*, *};
use mysql_async::prelude::*;
use rand::Rng;
use tokio::runtime::Runtime;

struct PKSampler {
    file_names: Vec<String>,
}

fn read_lines_lazy(file_path: &str) -> impl Iterator<Item = io::Result<String>> {
    let file = File::open(file_path).unwrap();
    let reader = BufReader::new(file);
    reader.lines().map(|line| {
        line.map(|l| {
            let v: Vec<String> = l.trim().split(' ').take(1).map(|e| e.to_string()).collect();
            v.into_iter().next().unwrap()
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
                let survive = n as f64 / c as f64; // growing smaller
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

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct PKIssueArgs {
    #[arg(short, long)]
    mode: String,

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

#[test]
fn test_pk_sampler() {
    let mut temps = vec![];
    let mut file_names = vec![];
    let distinct = 4;
    let count = 1000;
    let check_count = 10000;
    for i in 0..distinct {
        let temp = tempfile::Builder::new()
            .prefix("f")
            .suffix(".txt")
            .tempfile()
            .unwrap();
        let s = String::from(temp.path().to_str().unwrap());
        let mut file = File::create(temp.path()).unwrap();
        for _j in 0..count {
            file.write_all(format!("{}\n", i).as_bytes()).unwrap();
        }
        temps.push(temp);
        file_names.push(s);
    }
    println!("file_names {:?}", file_names);
    let pk = PKSampler { file_names };
    let mut x = vec![];
    for _i in 0..check_count {
        let mut z = pk.sample(1, false);
        x.append(&mut z);
    }
    let mut m: HashMap<String, usize> = Default::default();
    for i in x.iter() {
        if m.contains_key(i) {
            *m.get_mut(i).unwrap() += 1;
        } else {
            m.insert(i.clone(), 1);
        }
    }
    println!("m {:?}", m);
    let low = check_count / (distinct + 1);
    for (_k, v) in m.iter() {
        assert!(v > &low)
    }
}

/// Consider b regions and k samples, then estimation of coverred regions is b * (1 - ((b - 1) / b) ** k).
/// ./target/release/pressure --tidb-addrs mysql://root@172.31.7.1:4000/,mysql://root@172.31.7.2:4000/,mysql://root@172.31.7.3:4000/,mysql://root@172.31.7.4:4000/ --input-files /home/ubuntu/tiflash-u2/pk_0,/home/ubuntu/tiflash-u2/pk_1,/home/ubuntu/tiflash-u2/pk_2,/home/ubuntu/tiflash-u2/pk_3,/home/ubuntu/tiflash-u2/pk_4,/home/ubuntu/tiflash-u2/pk_5 -s 5000000 --update-interval-millis 5000 --batch-size 1 --slot-count 50 --workers 100
/// ./target/debug/pressure --tidb-addrs mysql://root@127.0.0.1:4000/ --input-files /Users/calvin/pressure/pressure/a,/Users/calvin/pressure/pressure/b,/Users/calvin/pressure/pressure/c -s 1 --update-interval-millis 100 --slot-count 1

fn main() {
    let args = PKIssueArgs::parse();
    let file_names = args.input_files.split(',').map(|e| e.to_string()).collect();
    let pk = PKSampler { file_names };
    let feeder: Arc<Feeder<String>> = Arc::new(Feeder::new(
        args.slot_count,
        args.slot_size,
        Arc::new(pk),
        args.update_interval_millis,
    ));
    let addrs: Vec<String> = args.tidb_addrs.split(',').map(|e| e.to_string()).collect();
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
        let feed = feeder;
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
