//! Windowed minimum and maximum.
//!
//! Port of C libudx's `win_filter.c` and `win_filter_f64.c`, which carry
//! Kathleen Nichols' algorithm from the Linux kernel's `lib/minmax.c`. BBR
//! uses the maximum for its bandwidth estimate, and the stream uses the
//! minimum for its lowest round-trip time.

#[derive(Debug, Clone, Copy)]
struct Entry<T> {
    /// When the value was recorded, in the unit the window is measured in.
    t: u64,
    v: T,
}

/// The best value seen over a sliding window.
///
/// It keeps the best, second best and third best samples, each newer than
/// the one before, so that a best that leaves the window has a recent
/// successor instead of whatever sample happens to arrive next.
#[derive(Debug, Clone)]
pub(crate) struct WindowedFilter<T> {
    entries: [Entry<T>; 3],
}

impl<T: Copy + PartialOrd> WindowedFilter<T> {
    /// Creates a filter that holds `v`, recorded at `t`.
    pub(crate) fn new(t: u64, v: T) -> Self {
        Self {
            entries: [Entry { t, v }; 3],
        }
    }

    /// The best value in the window.
    pub(crate) fn get(&self) -> T {
        self.entries[0].v
    }

    /// Forgets every sample and starts again from `v`.
    pub(crate) fn reset(&mut self, t: u64, v: T) -> T {
        self.entries = [Entry { t, v }; 3];
        v
    }

    /// Records `v` at `t` and returns the largest value of the last `win`.
    pub(crate) fn apply_max(&mut self, win: u64, t: u64, v: T) -> T {
        // A new maximum, or nothing left in the window.
        if v >= self.entries[0].v || t.saturating_sub(self.entries[2].t) > win {
            return self.reset(t, v);
        }
        let new = Entry { t, v };
        if v >= self.entries[1].v {
            self.entries[2] = new;
            self.entries[1] = new;
        } else if v >= self.entries[2].v {
            self.entries[2] = new;
        }
        self.apply_common(win, new)
    }

    /// Records `v` at `t` and returns the smallest value of the last `win`.
    pub(crate) fn apply_min(&mut self, win: u64, t: u64, v: T) -> T {
        // A new minimum, or nothing left in the window.
        if v <= self.entries[0].v || t.saturating_sub(self.entries[2].t) > win {
            return self.reset(t, v);
        }
        let new = Entry { t, v };
        if v <= self.entries[1].v {
            self.entries[2] = new;
            self.entries[1] = new;
        } else if v <= self.entries[2].v {
            self.entries[2] = new;
        }
        self.apply_common(win, new)
    }

    fn apply_common(&mut self, win: u64, new: Entry<T>) -> T {
        let dt = new.t.saturating_sub(self.entries[0].t);
        if dt > win {
            // The best has left the window, so its successors move up.
            self.entries[0] = self.entries[1];
            self.entries[1] = self.entries[2];
            self.entries[2] = new;
            if new.t.saturating_sub(self.entries[0].t) > win {
                self.entries[0] = self.entries[1];
                self.entries[1] = self.entries[2];
                self.entries[2] = new;
            }
        } else if self.entries[1].t == self.entries[0].t && dt > win / 4 {
            // A quarter of the window has passed with no second best of its
            // own, so take one from the rest of the window.
            self.entries[2] = new;
            self.entries[1] = new;
        } else if self.entries[2].t == self.entries[1].t && dt > win / 2 {
            // Half the window has passed with no third best of its own.
            self.entries[2] = new;
        }
        self.entries[0].v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_keeps_the_largest_value_in_the_window() {
        let mut filter = WindowedFilter::new(0, 0.0);
        assert_eq!(filter.apply_max(10, 1, 5.0), 5.0);
        assert_eq!(filter.apply_max(10, 2, 3.0), 5.0);
        assert_eq!(filter.apply_max(10, 3, 8.0), 8.0);
        assert_eq!(filter.get(), 8.0);
    }

    #[test]
    fn min_keeps_the_smallest_value_in_the_window() {
        let mut filter = WindowedFilter::new(0, u32::MAX);
        assert_eq!(filter.apply_min(100, 1, 40), 40);
        assert_eq!(filter.apply_min(100, 2, 70), 40);
        assert_eq!(filter.apply_min(100, 3, 25), 25);
        assert_eq!(filter.get(), 25);
    }

    #[test]
    fn an_expired_maximum_gives_way_to_the_best_of_the_rest() {
        let mut filter = WindowedFilter::new(0, 0.0);
        filter.apply_max(10, 0, 100.0);
        // After a quarter of the window, later samples are kept as successors.
        filter.apply_max(10, 4, 60.0);
        filter.apply_max(10, 7, 40.0);
        assert_eq!(filter.get(), 100.0);

        // The maximum leaves the window, and the best of what came after it
        // takes over, not the sample that happens to arrive now.
        assert_eq!(filter.apply_max(10, 11, 10.0), 60.0);
    }

    #[test]
    fn an_expired_minimum_gives_way_to_the_best_of_the_rest() {
        let mut filter = WindowedFilter::new(0, u32::MAX);
        filter.apply_min(100, 0, 5);
        filter.apply_min(100, 30, 9);
        filter.apply_min(100, 60, 20);
        assert_eq!(filter.get(), 5);
        assert_eq!(filter.apply_min(100, 101, 50), 9);
    }

    #[test]
    fn a_filter_left_alone_past_its_window_starts_again() {
        let mut filter = WindowedFilter::new(0, 0.0);
        filter.apply_max(10, 0, 100.0);
        assert_eq!(filter.apply_max(10, 50, 7.0), 7.0);
    }

    #[test]
    fn reset_forgets_earlier_samples() {
        let mut filter = WindowedFilter::new(0, 0.0);
        filter.apply_max(10, 1, 100.0);
        filter.reset(2, 1.0);
        assert_eq!(filter.get(), 1.0);
    }
}
