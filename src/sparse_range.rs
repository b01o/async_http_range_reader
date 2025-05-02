#![allow(dead_code)]
use bisection::{bisect_left, bisect_right};
use itertools::Itertools;
use std::{
    fmt::{Debug, Display, Formatter},
    ops::{Range, RangeInclusive},
};

// A data structure that keeps track of a range of values with potential holes in them.
#[derive(Default, Clone, Eq, PartialEq)]
pub struct SparseRange {
    left: Vec<u64>,
    right: Vec<u64>,
}

impl Display for SparseRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.covered_iranges()
                .format_with(", ", |elt, f| f(&format_args!(
                    "{}..={}",
                    elt.start(),
                    elt.end()
                )))
        )
    }
}

impl Debug for SparseRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}",)
    }
}

impl SparseRange {
    // Construct a new SparseRange from an initial covered range.
    pub fn from_range(range: Range<u64>) -> Self {
        Self {
            left: vec![range.start],
            right: vec![range.end - 1], // -1 because the stored range are inclusive
        }
    }

    pub fn from_ranges(ranges: Vec<Range<u64>>) -> Self {
        if ranges.is_empty() {
            return Self::default();
        }

        let mut srange = Self {
            left: vec![ranges[0].start],
            right: vec![ranges[0].end - 1], // -1 because the stored range are inclusive
        };

        for range in ranges.into_iter().skip(1) {
            srange.update(range);
        }

        srange
    }

    pub fn covered_ranges(&self) -> impl Iterator<Item = Range<u64>> + '_ {
        self.left
            .iter()
            .zip(self.right.iter())
            .map(|(&left, &right)| left..right + 1)
    }

    /// Returns the covered ranges
    pub fn covered_iranges(&self) -> impl Iterator<Item = RangeInclusive<u64>> + '_ {
        self.left
            .iter()
            .zip(self.right.iter())
            .map(|(&left, &right)| RangeInclusive::new(left, right))
    }

    pub fn is_covered(&self, range: Range<u64>) -> bool {
        let range_start = range.start;
        let range_end = range.end - 1;

        // Compute the indices of the ranges that are covered by the request
        let left_index = bisect_left(&self.right, &range_start);
        let right_index = bisect_right(&self.left, &(range_end + 1));

        // Get all the range bounds that are covered
        let left_slice = &self.left[left_index..right_index];
        let right_slice = &self.right[left_index..right_index];

        // Compute the bounds of covered range taking into account existing covered ranges.
        let start = left_slice
            .first()
            .map_or(range_start, |&left_bound| left_bound.min(range_start));

        // Get the ranges that are missing
        let mut bound = start;
        for (&left_bound, &right_bound) in left_slice.iter().zip(right_slice.iter()) {
            if left_bound > bound {
                return false;
            }
            bound = right_bound + 1;
        }

        let end = right_slice
            .last()
            .map_or(range_end, |&right_bound| right_bound.max(range_end));

        bound > end
    }

    /// Updates the current range to also cover the specified range.
    pub fn update(&mut self, range: Range<u64>) {
        if let Some((new_range, _)) = self.cover(range) {
            *self = new_range;
        }
    }

    /// Find the ranges that are uncovered for the specified range together with what the
    /// [`SparseRange`] would look like if we covered that range.
    pub fn cover(&self, range: Range<u64>) -> Option<(SparseRange, Vec<RangeInclusive<u64>>)> {
        let range_start = range.start;
        let range_end = range.end - 1;

        // Compute the indices of the ranges that are covered by the request
        let left_index = bisect_left(&self.right, &range_start);
        let right_index = bisect_right(&self.left, &(range_end + 1));

        // Get all the range bounds that are covered
        let left_slice = &self.left[left_index..right_index];
        let right_slice = &self.right[left_index..right_index];

        // Compute the bounds of covered range taking into account existing covered ranges.
        let start = left_slice
            .first()
            .map_or(range_start, |&left_bound| left_bound.min(range_start));
        let end = right_slice
            .last()
            .map_or(range_end, |&right_bound| right_bound.max(range_end));

        // Get the ranges that are missing
        let mut ranges = Vec::new();
        let mut bound = start;
        for (&left_bound, &right_bound) in left_slice.iter().zip(right_slice.iter()) {
            if left_bound > bound {
                ranges.push(bound..=(left_bound - 1));
            }
            bound = right_bound + 1;
        }
        if bound <= end {
            ranges.push(bound..=end);
        }

        if ranges.is_empty() {
            None
        } else {
            let mut new_left = self.left.clone();
            new_left.splice(left_index..right_index, [start]);
            let mut new_right = self.right.clone();
            new_right.splice(left_index..right_index, [end]);
            Some((Self { left: new_left, right: new_right }, ranges))
        }
    }

    /// total len of the covered ranges addes up
    pub fn len(&self) -> u64 {
        self.covered_iranges()
            .map(|range| range.end() - range.start() + 1)
            .sum()
    }

    // remove the specified range from the current range
    pub fn remove(&mut self, range: Range<u64>) {
        if let Some((new_range, _)) = self.uncover(range) {
            *self = new_range;
        }
    }

    /// unconver_all except the specified range
    pub fn uncover_except(
        &self,
        range: Range<u64>,
    ) -> Option<(SparseRange, Vec<RangeInclusive<u64>>)> {
        let range_start = range.start;
        let range_end = range.end - 1;

        // Compute the indices of the ranges that are covered by the request
        let left_index = bisect_left(&self.right, &range_start);
        let right_index = bisect_right(&self.left, &(range_end + 1));

        // Get the ranges that are missing
        let mut new_left = vec![];
        let mut new_right = vec![];

        let mut ranges = Vec::new();
        for (i, (&l, &r)) in self.left.iter().zip(self.right.iter()).enumerate() {
            // not in left_index..right_index, we can push to ranges
            if i < left_index || i >= right_index {
                ranges.push(l..=r);
                continue;
            }

            if l < range_start {
                // l..=range_start - 1
                ranges.push(l..=range_start - 1);
            }

            if r > range_end {
                ranges.push(range_end + 1..=r);
            }

            if l < range_start && r > range_end {
                // range_start..=range_end
                new_left.push(range_start);
                new_right.push(range_end);
            } else if l < range_start {
                // range_start..=r
                new_left.push(range_start);
                new_right.push(r);
            } else if r > range_end {
                // l..=range_end
                new_left.push(l);
                new_right.push(range_end);
            } else {
                // l..=r
                new_left.push(l);
                new_right.push(r);
            }
        }

        if ranges.is_empty() {
            None
        } else {
            Some((Self { left: new_left, right: new_right }, ranges))
        }
    }

    /// Find the ranges that are covered for the specified range together with what the
    /// [`SparseRange`] would look like if we uncovered that range.
    pub fn uncover(&self, range: Range<u64>) -> Option<(SparseRange, Vec<RangeInclusive<u64>>)> {
        let range_start = range.start; // 0
        let range_end = range.end - 1; // 0

        // Compute the indices of the ranges that are covered by the request
        let left_index = bisect_left(&self.right, &range_start); // 0
        let right_index = bisect_right(&self.left, &(range_end));

        // Get all the range bounds that are covered
        let left_slice = &self.left[left_index..right_index];
        let right_slice = &self.right[left_index..right_index];

        let mut ranges = Vec::with_capacity(left_slice.len());

        let mut left_cut_off = vec![];
        let mut right_cut_off = vec![];
        for (&l, &r) in left_slice.iter().zip(right_slice.iter()) {
            if range_start > l {
                // l..=range_start - 1
                left_cut_off.push(l);
                right_cut_off.push(range_start - 1);
            }
            if range_end < r {
                // range_end + 1..=r
                left_cut_off.push(range_end + 1);
                right_cut_off.push(r);
            }
            ranges.push(l.max(range_start)..=r.min(range_end));
        }

        let mut new_left = self.left.clone();
        // Remove the ranges that are covered by the request
        new_left.splice(left_index..right_index, left_cut_off);
        let mut new_right = self.right.clone();
        new_right.splice(left_index..right_index, right_cut_off);

        if ranges.is_empty() {
            None
        } else {
            Some((Self { left: new_left, right: new_right }, ranges))
        }
    }
}

#[cfg(test)]
mod test {
    use super::SparseRange;

    #[test]
    fn test_uncover_except() {
        let range = SparseRange::from_range(5..15);
        assert!(range.uncover_except(5..15).is_none());
        assert!(range.uncover_except(4..15).is_none());
        assert!(range.uncover_except(5..16).is_none());
        assert!(range.uncover_except(3..16).is_none());

        assert!(range.uncover_except(3..10).is_some());
        assert!(range.uncover_except(7..10).is_some());
        assert!(range.uncover_except(7..20).is_some());
        assert!(range.uncover_except(0..20).is_none());

        let (range_after, uncovered) = range.uncover_except(5..10).unwrap();
        assert_eq!(uncovered, vec![10..=14]);
        assert_eq!(
            range_after.covered_iranges().collect::<Vec<_>>(),
            vec![5..=9]
        );

        let (range_after, uncovered) = range.uncover_except(5..14).unwrap();
        assert_eq!(uncovered, vec![14..=14]);
        assert_eq!(
            range_after.covered_iranges().collect::<Vec<_>>(),
            vec![5..=13]
        );

        let (range_after, uncovered) = range.uncover_except(0..1).unwrap();
        assert_eq!(uncovered, vec![5..=14]);
        assert_eq!(range_after.covered_iranges().collect::<Vec<_>>(), vec![]);

        let (range_after, uncovered) = range.uncover_except(7..10).unwrap();
        assert_eq!(uncovered, vec![5..=6, 10..=14]);
        assert_eq!(
            range_after.covered_iranges().collect::<Vec<_>>(),
            vec![7..=9]
        );

        let (range_after, uncovered) = range.uncover_except(10..20).unwrap();
        assert_eq!(uncovered, vec![5..=9]);
        assert_eq!(
            range_after.covered_iranges().collect::<Vec<_>>(),
            vec![10..=14]
        );

        let mut range = SparseRange::from_range(5..15);
        range.update(20..30);
        // [5..=14], [20..=29]

        let (range_after, uncovered) = range.uncover_except(10..26).unwrap();
        assert_eq!(uncovered, vec![5..=9, 26..=29]);
        assert_eq!(
            range_after.covered_iranges().collect::<Vec<_>>(),
            vec![10..=14, 20..=25]
        );
    }

    #[test]
    fn test_uncover() {
        // [0, 10], [20, 30], [40, 50], [60, 70], [80, 90]
        let mut range = SparseRange::from_range(0..11);
        range.update(20..31);
        range.update(40..51);
        range.update(60..71);
        range.update(80..91);

        assert_eq!(
            range.covered_iranges().collect::<Vec<_>>(),
            vec![0..=10, 20..=30, 40..=50, 60..=70, 80..=90]
        );

        // Uncovering [35, 75] => expecting uncovered [40, 50], [60, 70]
        let (range_after, uncovered) = range.uncover(35..76).unwrap();
        assert_eq!(uncovered, vec![40..=50, 60..=70]);
        assert_eq!(
            range_after.covered_iranges().collect::<Vec<_>>(),
            vec![0..=10, 20..=30, 80..=90]
        );

        // Uncovering [25, 75] => expecting uncovered [25, 30], [40, 50], [60, 70]
        let (range_after, uncovered) = range.uncover(25..76).unwrap();
        assert_eq!(uncovered, vec![25..=30, 40..=50, 60..=70]);
        assert_eq!(
            range_after.covered_iranges().collect::<Vec<_>>(),
            vec![0..=10, 20..=24, 80..=90]
        );

        // uncovering [35, 85] => expecting [40, 50], [60, 70], [80, 85]
        let (range_after, uncovered) = range.uncover(35..86).unwrap();
        assert_eq!(uncovered, vec![40..=50, 60..=70, 80..=85]);
        assert_eq!(
            range_after.covered_iranges().collect::<Vec<_>>(),
            vec![0..=10, 20..=30, 86..=90]
        );

        // uncovering [35, 80] => expecting [40, 50], [60, 70], [80, 80]
        let (range_after, uncovered) = range.uncover(35..81).unwrap();
        assert_eq!(uncovered, vec![40..=50, 60..=70, 80..=80]);
        assert_eq!(
            range_after.covered_iranges().collect::<Vec<_>>(),
            vec![0..=10, 20..=30, 81..=90]
        );

        // 1..=10
        let range = SparseRange::from_range(1..11);
        assert!(range.uncover(11..20).is_none());
        assert!(range.uncover(0..1).is_none());
        assert!(range.uncover(0..2).is_some());
        assert!(range.uncover(2..5).is_some());
        assert!(range.uncover(2..11).is_some());
        assert!(range.uncover(2..15).is_some());
        assert!(range.uncover(11..15).is_none());

        let (range_after, uncovered) = range.uncover(3..6).unwrap();
        assert_eq!(uncovered, vec![3..=5]);
        assert_eq!(
            range_after.covered_iranges().collect::<Vec<_>>(),
            vec![1..=2, 6..=10]
        );

        let (range_after, uncovered) = range.uncover(3..100).unwrap();
        assert_eq!(uncovered, vec![3..=10]);
        assert_eq!(
            range_after.covered_iranges().collect::<Vec<_>>(),
            vec![1..=2]
        );

        let (range_after, uncovered) = range.uncover(0..5).unwrap();
        assert_eq!(uncovered, vec![1..=4]);
        assert_eq!(
            range_after.covered_iranges().collect::<Vec<_>>(),
            vec![5..=10]
        );

        let mut range = SparseRange::from_range(1..11);
        range.update(20..31);
        assert!(range.uncover(11..20).is_none());
        assert!(range.uncover(12..20).is_none());
        assert!(range.uncover(11..19).is_none());

        assert!(range.uncover(11..21).is_some());
        assert!(range.uncover(10..20).is_some());
        assert!(range.uncover(0..20).is_some());
        assert!(range.uncover(0..120).is_some());

        let (range_after, uncovered) = range.uncover(3..6).unwrap();
        assert_eq!(uncovered, vec![3..=5]);
        assert_eq!(
            range_after.covered_iranges().collect::<Vec<_>>(),
            vec![1..=2, 6..=10, 20..=30]
        );

        let (range_after, uncovered) = range.uncover(3..100).unwrap();
        assert_eq!(uncovered, vec![3..=10, 20..=30]);
        assert_eq!(
            range_after.covered_iranges().collect::<Vec<_>>(),
            vec![1..=2]
        );

        let (range_after, uncovered) = range.uncover(15..26).unwrap();
        assert_eq!(uncovered, vec![20..=25]);
        assert_eq!(
            range_after.covered_iranges().collect::<Vec<_>>(),
            vec![1..=10, 26..=30]
        );
    }

    #[test]
    fn test_sparse_range() {
        let range = SparseRange::default();
        assert!(range.covered_iranges().next().is_none());
        assert_eq!(
            range.cover(5..10).unwrap().0,
            SparseRange::from_range(5..10)
        );

        let range = SparseRange::from_range(5..10);
        assert_eq!(range.covered_iranges().collect::<Vec<_>>(), vec![5..=9]);
        assert!(range.is_covered(5..10));
        assert!(range.is_covered(6..9));
        assert!(!range.is_covered(5..11));
        assert!(!range.is_covered(3..8));

        assert_eq!(
            range.cover(3..5),
            Some((SparseRange::from_range(3..10), vec![3..=4]))
        );

        let (range, missing) = range.cover(12..15).unwrap();
        assert_eq!(
            range.covered_iranges().collect::<Vec<_>>(),
            vec![5..=9, 12..=14]
        );
        assert_eq!(missing, vec![12..=14]);
        assert!(range.is_covered(5..10));
        assert!(range.is_covered(12..15));
        assert!(!range.is_covered(5..15));
        assert!(!range.is_covered(11..12));

        let (range, missing) = range.cover(8..14).unwrap();
        assert_eq!(range.covered_iranges().collect::<Vec<_>>(), vec![5..=14]);
        assert_eq!(missing, vec![10..=11]);
    }
}
