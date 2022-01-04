use std::time::{Duration, Instant};

#[cfg(not(feature = "metrics"))]
use std::marker::PhantomData;

use once_cell::sync::Lazy;

use super::*;
use crate::atomic::*;

/// A metric collector for all pagecache users running in this
/// process.
pub static M: Lazy<Metrics, fn() -> Metrics> = Lazy::new(Metrics::default);

pub(crate) fn clock() -> f64 {
    if cfg!(feature = "metrics") {
        let u = uptime();
        (u.as_secs() * 1_000_000_000) as f64 + f64::from(u.subsec_nanos())
    } else {
        0.
    }
}

// not correct, since it starts counting at the first observance...
pub(crate) fn uptime() -> Duration {
    static START: Lazy<Instant, fn() -> Instant> = Lazy::new(Instant::now);

    if cfg!(feature = "metrics") {
        START.elapsed()
    } else {
        Duration::new(0, 0)
    }
}

/// Measure the duration of an event, and call `Histogram::measure()`.
pub struct Measure<'h> {
    _start: f64,
    #[cfg(feature = "metrics")]
    histo: &'h Histogram,
    #[cfg(not(feature = "metrics"))]
    _pd: PhantomData<&'h ()>,
}

impl<'h> Measure<'h> {
    /// The time delta from ctor to dtor is recorded in `histo`.
    #[inline(always)]
    pub fn new(_histo: &'h Histogram) -> Measure<'h> {
        Measure {
            #[cfg(not(feature = "metrics"))]
            _pd: PhantomData,
            #[cfg(feature = "metrics")]
            histo: _histo,
            _start: clock(),
        }
    }
}

impl<'h> Drop for Measure<'h> {
    #[inline(always)]
    fn drop(&mut self) {
        #[cfg(feature = "metrics")]
        self.histo.measure(clock() - self._start);
    }
}

/// Measure the time spent on calling a given function in a given `Histogram`.
#[cfg_attr(not(feature = "no_inline"), inline)]
pub(crate) fn measure<F: FnOnce() -> R, R>(_histo: &Histogram, f: F) -> R {
    #[cfg(feature = "metrics")]
    let _measure = Measure::new(_histo);
    f()
}

#[derive(Default, Debug)]
pub struct Metrics {
    pub advance_snapshot: Histogram,
    pub tree_set: Histogram,
    pub tree_get: Histogram,
    pub tree_del: Histogram,
    pub tree_cas: Histogram,
    pub tree_scan: Histogram,
    pub tree_reverse_scan: Histogram,
    pub tree_merge: Histogram,
    pub tree_start: Histogram,
    pub tree_traverse: Histogram,
    pub tree_child_split_attempt: CachePadded<AtomicUsize>,
    pub tree_child_split_success: CachePadded<AtomicUsize>,
    pub tree_parent_split_attempt: CachePadded<AtomicUsize>,
    pub tree_parent_split_success: CachePadded<AtomicUsize>,
    pub tree_root_split_attempt: CachePadded<AtomicUsize>,
    pub tree_root_split_success: CachePadded<AtomicUsize>,
    pub get_page: Histogram,
    pub rewrite_page: Histogram,
    pub replace_page: Histogram,
    pub link_page: Histogram,
    pub merge_page: Histogram,
    pub page_out: Histogram,
    pub pull: Histogram,
    pub serialize: Histogram,
    pub deserialize: Histogram,
    pub compress: Histogram,
    pub decompress: Histogram,
    pub make_stable: Histogram,
    pub assign_offset: Histogram,
    pub assign_spinloop: Histogram,
    pub reserve_lat: Histogram,
    pub reserve_sz: Histogram,
    pub reserve_current_condvar_wait: Histogram,
    pub reserve_written_condvar_wait: Histogram,
    pub write_to_log: Histogram,
    pub written_bytes: Histogram,
    pub read: Histogram,
    pub tree_loops: CachePadded<AtomicUsize>,
    pub log_reservations: CachePadded<AtomicUsize>,
    pub log_reservation_attempts: CachePadded<AtomicUsize>,
    pub accountant_lock: Histogram,
    pub accountant_hold: Histogram,
    pub accountant_next: Histogram,
    pub accountant_mark_link: Histogram,
    pub accountant_mark_replace: Histogram,
    pub accountant_bump_tip: Histogram,
    #[cfg(feature = "measure_allocs")]
    pub allocations: CachePadded<AtomicUsize>,
    #[cfg(feature = "measure_allocs")]
    pub allocated_bytes: CachePadded<AtomicUsize>,
}

#[cfg(feature = "metrics")]
impl Metrics {
    #[inline]
    pub fn tree_looped(&self) {
        self.tree_loops.fetch_add(1, Relaxed);
    }

    #[inline]
    pub fn log_reservation_attempted(&self) {
        self.log_reservation_attempts.fetch_add(1, Relaxed);
    }

    #[inline]
    pub fn log_reservation_success(&self) {
        self.log_reservations.fetch_add(1, Relaxed);
    }

    #[inline]
    pub fn tree_child_split_attempt(&self) {
        self.tree_child_split_attempt.fetch_add(1, Relaxed);
    }

    #[inline]
    pub fn tree_child_split_success(&self) {
        self.tree_child_split_success.fetch_add(1, Relaxed);
    }

    #[inline]
    pub fn tree_parent_split_attempt(&self) {
        self.tree_parent_split_attempt.fetch_add(1, Relaxed);
    }

    #[inline]
    pub fn tree_parent_split_success(&self) {
        self.tree_parent_split_success.fetch_add(1, Relaxed);
    }

    #[inline]
    pub fn tree_root_split_attempt(&self) {
        self.tree_root_split_attempt.fetch_add(1, Relaxed);
    }

    #[inline]
    pub fn tree_root_split_success(&self) {
        self.tree_root_split_success.fetch_add(1, Relaxed);
    }

    pub fn print_profile(&self) {
        println!(
            "pagecache profile:\n\
            {0: >17} | {1: >10} | {2: >10} | {3: >10} | {4: >10} | {5: >10} | {6: >10} | {7: >10} | {8: >10} | {9: >10}",
            "op",
            "min (us)",
            "med (us)",
            "90 (us)",
            "99 (us)",
            "99.9 (us)",
            "99.99 (us)",
            "max (us)",
            "count",
            "sum (s)"
        );
        println!("{}", std::iter::repeat("-").take(134).collect::<String>());

        let p = |mut tuples: Vec<(String, _, _, _, _, _, _, _, _, _)>| {
            tuples.sort_by_key(|t| (t.9 * -1. * 1e3) as i64);
            for v in tuples {
                println!(
                    "{0: >17} | {1: >10.1} | {2: >10.1} | {3: >10.1} \
                     | {4: >10.1} | {5: >10.1} | {6: >10.1} | {7: >10.1} \
                     | {8: >10.1} | {9: >10.3}",
                    v.0, v.1, v.2, v.3, v.4, v.5, v.6, v.7, v.8, v.9,
                );
            }
        };

        let lat = |name: &str, histo: &Histogram| {
            (
                name.to_string(),
                histo.percentile(0.) / 1e3,
                histo.percentile(50.) / 1e3,
                histo.percentile(90.) / 1e3,
                histo.percentile(99.) / 1e3,
                histo.percentile(99.9) / 1e3,
                histo.percentile(99.99) / 1e3,
                histo.percentile(100.) / 1e3,
                histo.count(),
                histo.sum() as f64 / 1e9,
            )
        };

        let sz = |name: &str, histo: &Histogram| {
            (
                name.to_string(),
                histo.percentile(0.),
                histo.percentile(50.),
                histo.percentile(90.),
                histo.percentile(99.),
                histo.percentile(99.9),
                histo.percentile(99.99),
                histo.percentile(100.),
                histo.count(),
                histo.sum() as f64,
            )
        };

        println!("tree:");
        p(vec![
            lat("start", &self.tree_start),
            lat("traverse", &self.tree_traverse),
            lat("get", &self.tree_get),
            lat("set", &self.tree_set),
            lat("merge", &self.tree_merge),
            lat("del", &self.tree_del),
            lat("cas", &self.tree_cas),
            lat("scan", &self.tree_scan),
            lat("rev scan", &self.tree_reverse_scan),
        ]);
        println!("tree contention loops: {}", self.tree_loops.load(Acquire));
        println!(
            "tree split success rates: child({}/{}) parent({}/{}) root({}/{})",
            self.tree_child_split_success.load(Acquire),
            self.tree_child_split_attempt.load(Acquire),
            self.tree_parent_split_success.load(Acquire),
            self.tree_parent_split_attempt.load(Acquire),
            self.tree_root_split_success.load(Acquire),
            self.tree_root_split_attempt.load(Acquire),
        );

        println!("{}", std::iter::repeat("-").take(134).collect::<String>());
        println!("pagecache:");
        p(vec![
            lat("snapshot", &self.advance_snapshot),
            lat("get", &self.get_page),
            lat("rewrite", &self.rewrite_page),
            lat("replace", &self.replace_page),
            lat("link", &self.link_page),
            lat("merge", &self.merge_page),
            lat("pull", &self.pull),
            lat("page_out", &self.page_out),
        ]);

        println!("{}", std::iter::repeat("-").take(134).collect::<String>());
        println!("serialization and compression:");
        p(vec![
            lat("serialize", &self.serialize),
            lat("deserialize", &self.deserialize),
            lat("compress", &self.compress),
            lat("decompress", &self.decompress),
        ]);

        println!("{}", std::iter::repeat("-").take(134).collect::<String>());
        println!("log:");
        p(vec![
            lat("make_stable", &self.make_stable),
            lat("read", &self.read),
            lat("write", &self.write_to_log),
            sz("written bytes", &self.written_bytes),
            lat("assign offset", &self.assign_offset),
            lat("assign spinloop", &self.assign_spinloop),
            lat("reserve lat", &self.reserve_lat),
            sz("reserve sz", &self.reserve_sz),
            lat("res cvar r", &self.reserve_current_condvar_wait),
            lat("res cvar w", &self.reserve_written_condvar_wait),
        ]);
        println!("log reservations: {}", self.log_reservations.load(Acquire));
        println!(
            "log res attempts: {}",
            self.log_reservation_attempts.load(Acquire)
        );

        println!("{}", std::iter::repeat("-").take(134).collect::<String>());
        println!("segment accountant:");
        p(vec![
            lat("acquire", &self.accountant_lock),
            lat("hold", &self.accountant_hold),
            lat("next", &self.accountant_next),
            lat("replace", &self.accountant_mark_replace),
            lat("link", &self.accountant_mark_link),
        ]);

        #[cfg(feature = "measure_allocs")]
        {
            println!("{}", std::iter::repeat("-").take(134).collect::<String>());
            println!("allocation statistics:");
            println!(
                "total allocations: {}",
                measure_allocs::ALLOCATIONS.load(Acquire)
            );
            println!(
                "allocated bytes: {}",
                measure_allocs::ALLOCATED_BYTES.load(Acquire)
            );
        }
    }
}

#[cfg(not(feature = "metrics"))]
impl Metrics {
    pub fn log_reservation_attempted(&self) {}

    pub fn log_reservation_success(&self) {}

    pub fn tree_child_split_attempt(&self) {}

    pub fn tree_child_split_success(&self) {}

    pub fn tree_parent_split_attempt(&self) {}

    pub fn tree_parent_split_success(&self) {}

    pub fn tree_root_split_attempt(&self) {}

    pub fn tree_root_split_success(&self) {}

    pub fn tree_looped(&self) {}

    pub fn log_looped(&self) {}

    pub fn print_profile(&self) {}
}
