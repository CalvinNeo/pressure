use std::{
    io::Write,
    sync::{Arc, RwLock},
    thread::{self},
};

// use mysql::{prelude::*, *};
use rand::Rng;

pub trait Sampler<T: Clone + Sync>: Send + Sync {
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
        for _i in 0..n {
            let j: usize = rng.gen_range(0usize..self.items.len());
            v.push(self.items[j].clone());
        }
        v
    }
}

pub struct Feeder<T: Clone + Sync> {
    slot_count: usize,
    slot_size: usize,
    slots: Vec<RwLock<Slot<T>>>,
    sampler: Arc<dyn Sampler<T>>,
    update_millis: u64,
}

unsafe impl<T: Clone + Sync> Send for Feeder<T> {}

impl<T: Clone + Sync + 'static> Feeder<T> {
    pub fn new(
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
        let slot_size = self.slot_size;
        let sampler = self.sampler.clone();
        let slot = Slot::new(sampler, slot_size);
        let size = slot.items.len();
        *self.slots[i].write().expect("update lock error") = slot;
        size
    }

    pub fn sample(&self, n: usize) -> Vec<T> {
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

pub async fn background_update<T: Clone + Sync + 'static>(feeder: Arc<Feeder<T>>) {
    let update_millis = feeder.update_millis;
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
