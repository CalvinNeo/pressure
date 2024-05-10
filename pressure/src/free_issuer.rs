use std::sync::{
    atomic::{AtomicBool, AtomicU64},
    Arc,
};

// use mysql::{prelude::*, *};
use mysql_async::{prelude::*, *};
use rand::Rng;
use tokio::runtime::Runtime;

use crate::feeder::Feeder;

const STEP: u64 = 20;

pub struct FreeIssuer {
    urls: Vec<String>,
    dry_run: bool,
    pub acc: Arc<AtomicU64>,
    no_print_data: bool,
    batch_size: u64,
}

impl FreeIssuer {
    pub fn new(
        urls: Vec<String>,
        dry_run: bool,
        no_print_data: bool,
        batch_size: u64,
    ) -> Self {
        Self {
            urls,
            dry_run,
            acc: Arc::new(AtomicU64::new(0)),
            no_print_data,
            batch_size,
        }
    }

    pub fn start(
        &self,
        n_threads: usize,
        rt: &Runtime,
        finished: Arc<AtomicBool>,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        let _g2 = rt.enter();
        let mut rv: Vec<tokio::task::JoinHandle<()>> = vec![];
        for thid in 0..n_threads {
            let thread_id = thid;
            let urls = self.urls.clone();
            let dry_run = self.dry_run;
            let no_print_data = self.no_print_data;
            let acc = self.acc.clone();
            let batch_size = self.batch_size;
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
                        let mut rng = rand::thread_rng();
                        let tidb_id: usize = rng.gen_range(0usize..urls.len());
                        // Let's create a table for payments.
                        let s = Iterator::intersperse((0..batch_size).into_iter().map(|_| {
                            let random_time = rng.gen_range(1..100000);
                            format!("insert into narrow.t(ins,ts,r,s,v,incr) values ('',{},'','',1,2);", random_time)
                        }), "\n".to_string()).collect();
                        (s, tidb_id)
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
            };
            rv.push(rt.spawn(f()));
        }
        rv
    }
}
