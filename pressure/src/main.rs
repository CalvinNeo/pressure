use rand::prelude::IteratorRandom;
use rand::Rng;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::RwLock;
use std::thread::{self, JoinHandle};

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
}

unsafe impl<T: Clone + Sync> Send for Feeder<T> {}

impl<T: Clone + Sync> Feeder<T> {
    fn new(slot_count: usize, slot_size: usize, sampler: Arc<dyn Sampler<T>>) -> Self {
        let mut feeder = Feeder {
            slot_count,
            slot_size,
            slots: vec![],
            sampler,
        };
        for _i in 0..slot_count {
            feeder.slots.push(RwLock::new(Slot::default()));
        }
        for i in 0..slot_count {
            feeder.update_cell(i);
        }
        feeder
    }

    fn update_cell(&self, i: usize) {
        let slot = Slot::new(self.sampler.clone(), self.slot_size);
        *self.slots[i].write().expect("update lock error") = slot;
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
            let mut rng = rand::thread_rng();
            loop {
                if finished.load(std::sync::atomic::Ordering::SeqCst) {
                    return;
                }
                let slot_id: usize = rng.gen_range(0usize..feeder.slot_count);
                feeder.update_cell(slot_id);
                println!("update {}", slot_id);
                thread::sleep(std::time::Duration::from_millis(500));
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

fn main() {
    let pk = PKSampler {
        file_names: vec![
            "/Users/calvin/pressure/pressure/a".to_string(),
            "/Users/calvin/pressure/pressure/b".to_string(),
            "/Users/calvin/pressure/pressure/c".to_string(),
        ],
    };
    let feeder: Arc<Feeder<String>> = Arc::new(Feeder::new(10, 4, Arc::new(pk)));
    let (f, j) = background_update(feeder.clone());
    let feed = feeder.clone();
    let j1 = thread::spawn(move || loop {
        for i in feed.sample(3).into_iter() {
            println!("res1 {}", i);
        }
        thread::sleep(std::time::Duration::from_millis(500));
    });
    let feed = feeder.clone();
    let j2 = thread::spawn(move || loop {
        for i in feed.sample(3).into_iter() {
            println!("res2 {}", i);
        }
        thread::sleep(std::time::Duration::from_millis(500));
    });
    let feed = feeder.clone();
    let j3 = thread::spawn(move || loop {
        for i in feed.sample(3).into_iter() {
            println!("res3 {}", i);
        }
        thread::sleep(std::time::Duration::from_millis(500));
    });
    f.store(true, std::sync::atomic::Ordering::SeqCst);
    j.join().unwrap();
    j1.join();
    j2.join();
    j3.join();
}
