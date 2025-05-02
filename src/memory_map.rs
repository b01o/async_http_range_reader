use std::{collections::BTreeMap, ops::RangeBounds, sync::Mutex};

use itertools::Itertools;

use crate::sparse_range::SparseRange;

#[derive(Debug)]
/// a sparse memory map
pub struct MemoryMap {
    pub inner: Mutex<Inner>,
    len: u64,
}

#[derive(Debug)]
pub struct Inner {
    data: BTreeMap<u64, bytes::Bytes>,
    pub data_len: u64,
    pub record: SparseRange,
}

impl MemoryMap {
    /// create a new memory map
    pub fn new(len: u64) -> Self {
        Self {
            inner: Mutex::new(Inner {
                data: BTreeMap::new(),
                data_len: 0,
                record: SparseRange::default(),
            }),
            len,
        }
    }

    /// get the size of the memory map
    pub fn len(&self) -> u64 {
        self.len
    }

    /// store data and free memory that not within the range (inital_pos - back_limit, offset + bytes.len())
    pub fn store_and_free(
        &self,
        offset: u64,
        bytes: bytes::Bytes,
        initial_pos: u64,
        back_limit: u64,
    ) {
        if offset > self.len() {
            return;
        }
        let bytes = bytes.slice(..bytes.len().min((self.len() - offset) as usize));
        let Inner { data, data_len, record } = &mut *self.inner.lock().unwrap();

        if let Some((new_record, iranges)) = record.cover(offset..offset + bytes.len() as u64) {
            // update the data
            for (start, end) in iranges.iter().map(|r| (*r.start(), *r.end())) {
                let (b_start, b_end) = (start - offset, end - offset);
                let new_bytes = bytes.slice(b_start as usize..=b_end as usize);
                data.insert(start, new_bytes);
                // update data len
                *data_len += b_end - b_start + 1;
            }
            *record = new_record;
        }

        // free memory if needed
        let protect_zone = initial_pos.saturating_sub(back_limit)..(offset + bytes.len() as u64);
        if let Some((new_record_after_cleanup, iranges_to_remove)) =
            record.uncover_except(protect_zone)
        {
            for (start, end) in iranges_to_remove.iter().map(|r| (*r.start(), *r.end())) {
                let count = remove_range(data, start..end + 1);
                *data_len -= count;
            }
            // update the record
            *record = new_record_after_cleanup;
        }
    }

    #[allow(dead_code)]
    /// store data in the memory map, update record
    pub fn store(&self, offset: u64, bytes: bytes::Bytes) {
        if offset > self.len() {
            return;
        }
        let bytes = bytes.slice(..bytes.len().min((self.len() - offset) as usize));
        let Inner { data, data_len, record } = &mut *self.inner.lock().unwrap();

        if let Some((new_record, iranges)) = record.cover(offset..offset + bytes.len() as u64) {
            // update the data
            for (start, end) in iranges.iter().map(|r| (*r.start(), *r.end())) {
                let (b_start, b_end) = (start - offset, end - offset);
                let new_bytes = bytes.slice(b_start as usize..=b_end as usize);
                data.insert(start, new_bytes);
                // update data len
                *data_len += b_end - b_start + 1;
            }
            *record = new_record;
        }
    }

    /// get data from the memory map
    // pub fn get(&self, Range { start, end }: Range<u64>) -> Option<impl IntoIterator<Item = u8>> {
    pub fn get<B: RangeBounds<u64>>(&self, bounds: B) -> Option<impl IntoIterator<Item = u8>> {
        let (start, end) = match (bounds.start_bound(), bounds.end_bound()) {
            (std::ops::Bound::Included(s), std::ops::Bound::Included(e)) => (*s, *e + 1),
            (std::ops::Bound::Included(s), std::ops::Bound::Excluded(e)) => (*s, *e),
            _ => unimplemented!(),
        };

        if start > end || start > self.len() {
            return None;
        }
        let expected_len = end - start;

        #[rustfmt::skip]
        let Inner { data, data_len, record } = &*self.inner.lock().unwrap();

        if end > self.len() || *data_len < expected_len || !record.is_covered(start..end) {
            return None;
        }

        let mut arr: Vec<bytes::Bytes> = Vec::new();
        // find the greatest key less than or equal to start
        let start_idx = data.range(..=start).next_back()?.0;
        let iter = data.range(*start_idx..end).map(|(k, v)| (*k, v.clone()));
        for (offset, bytes) in iter {
            if *start_idx == offset {
                if (offset + bytes.len() as u64) < start {
                    continue;
                }
                let bytes_to_skip = (start - offset) as usize;
                let end_pos = end.min(offset + bytes.len() as u64) - offset;

                arr.push(bytes.slice(bytes_to_skip..end_pos as usize));
            } else {
                let end_pos = end.min(offset + bytes.len() as u64) - offset;
                arr.push(bytes.slice(..end_pos as usize));
            }
        }

        if arr.iter().map(|x| x.len() as u64).sum::<u64>() != expected_len {
            return None;
        }

        Some(arr.into_iter().flatten())
    }
}

pub fn remove_range<B: RangeBounds<u64>>(data: &mut BTreeMap<u64, bytes::Bytes>, bounds: B) -> u64 {
    let (remove_start, remove_end) = match (bounds.start_bound(), bounds.end_bound()) {
        (std::ops::Bound::Included(s), std::ops::Bound::Included(e)) => (*s, *e + 1),
        (std::ops::Bound::Included(s), std::ops::Bound::Excluded(e)) => (*s, *e),
        _ => unimplemented!(),
    };

    if remove_start > remove_end {
        return 0;
    }
    let mut removed_len = 0;

    let start_key = data
        .range(..=remove_start)
        .next_back()
        .map(|(k, _)| *k)
        .unwrap_or(0);

    let b_starts = data
        .range(start_key..=remove_end)
        .map(|(&k, _)| k)
        .collect_vec();

    for b_start in b_starts {
        let bytes = data.remove(&b_start).unwrap();
        let b_end = b_start + bytes.len() as u64;

        // bytes ends before remove_start
        if b_end < remove_start {
            continue;
        }

        if remove_start <= b_start && b_end <= remove_end {
            // bytes is fully covered by remove range
            removed_len += bytes.len() as u64;
        } else if remove_start <= b_start && remove_end < b_end {
            // bytes is partially covered by remove range
            // we need to push back the later part of the bytes
            data.insert(remove_end, bytes.slice((remove_end - b_start) as usize..));
            removed_len += remove_end - b_start;
        } else if remove_start > b_start && b_end <= remove_end {
            // bytes is partially covered by remove range
            // we need to push back the earlier part of the bytes
            data.insert(b_start, bytes.slice(..(remove_start - b_start) as usize));
            removed_len += b_end - remove_start;
        } else {
            // bytes is partially covered by remove range
            // we need to push back the earlier part of the bytes
            data.insert(b_start, bytes.slice(..(remove_start - b_start) as usize));
            // and the later part of the bytes
            data.insert(remove_end, bytes.slice((remove_end - b_start) as usize..));
            removed_len += remove_end - remove_start;
        }
    }

    removed_len
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::Bytes;
    use itertools::Itertools;

    #[test]
    fn test_store() {
        let mut map = MemoryMap::new(100);
        let bytes = Bytes::from(vec![1, 2, 3, 4, 5]);
        map.store(0, bytes.clone());
        assert!(map.len() == 100);
        assert!(map.inner.get_mut().unwrap().data_len == 5);
        assert!(&map.get(0..5).unwrap().into_iter().collect_vec() == &bytes);
        assert!(map.inner.get_mut().unwrap().record.is_covered(0..5));

        assert!(map.inner.get_mut().unwrap().record.is_covered(1..5));
        assert!(map.inner.get_mut().unwrap().record.is_covered(0..4));
        assert!(map.inner.get_mut().unwrap().record.is_covered(2..3));
        assert!(!map.inner.get_mut().unwrap().record.is_covered(2..10));

        let bytes2 = Bytes::from(vec![6, 7, 8, 9, 10]);

        map.store(5, bytes2.clone());
        assert!(map.inner.get_mut().unwrap().data_len == 10);
        assert!(
            map.get(0..10).unwrap().into_iter().collect_vec()
                == vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        );
        assert!(map.inner.get_mut().unwrap().record.is_covered(0..10));
        assert!(map.inner.get_mut().unwrap().record.is_covered(1..10));
        assert!(map.inner.get_mut().unwrap().record.is_covered(0..9));
        assert!(map.inner.get_mut().unwrap().record.is_covered(2..3));
        assert!(!map.inner.get_mut().unwrap().record.is_covered(2..20));
    }

    #[test]
    fn test_remove() {
        // remove exact
        let mut data = BTreeMap::from([(5, Bytes::from(vec![5, 6, 7, 8, 9]))]);
        assert_eq!(remove_range(&mut data, 5..=9), 5, "remove exact go wrong");
        assert_eq!(data, BTreeMap::new(), "remove exact go wrong");

        // remove within
        let mut data = BTreeMap::from([(5, Bytes::from(vec![5, 6, 7, 8, 9]))]);
        assert_eq!(remove_range(&mut data, 6..=8), 3, "remove within go wrong");
        assert_eq!(
            data,
            BTreeMap::from([(5, Bytes::from(vec![5])), (9, Bytes::from(vec![9]))]),
            "remove within go wrong"
        );

        // remove within left
        let mut data = BTreeMap::from([(5, Bytes::from(vec![5, 6, 7, 8, 9]))]);
        assert_eq!(
            remove_range(&mut data, 5..=6),
            2,
            "remove within left go wrong"
        );
        assert_eq!(
            data,
            BTreeMap::from([(7, Bytes::from(vec![7, 8, 9]))]),
            "remove within left go wrong"
        );

        // remove within left
        let mut data = BTreeMap::from([(5, Bytes::from(vec![5, 6, 7, 8, 9]))]);
        assert_eq!(
            remove_range(&mut data, 5..=8),
            4,
            "remove within left go wrong 2"
        );
        assert_eq!(
            data,
            BTreeMap::from([(9, Bytes::from(vec![9]))]),
            "remove within left go wrong 2"
        );

        // remove within right
        let mut data = BTreeMap::from([(5, Bytes::from(vec![5, 6, 7, 8, 9]))]);
        assert_eq!(
            remove_range(&mut data, 7..=9),
            3,
            "remove within right go wrong"
        );
        assert_eq!(
            data,
            BTreeMap::from([(5, Bytes::from(vec![5, 6]))]),
            "remove within right go wrong"
        );
        // remove within right
        let mut data = BTreeMap::from([(5, Bytes::from(vec![5, 6, 7, 8, 9]))]);
        assert_eq!(
            remove_range(&mut data, 6..=9),
            4,
            "remove within right go wrong 2"
        );
        assert_eq!(
            data,
            BTreeMap::from([(5, Bytes::from(vec![5]))]),
            "remove within right go wrong 2"
        );

        // remove outside left
        let mut data = BTreeMap::from([(5, Bytes::from(vec![5, 6, 7, 8, 9]))]);
        assert_eq!(
            remove_range(&mut data, 0..=4),
            0,
            "remove outside left go wrong"
        );
        assert_eq!(
            data,
            BTreeMap::from([(5, Bytes::from(vec![5, 6, 7, 8, 9]))]),
            "remove outside left go wrong"
        );

        // remove outside right
        let mut data = BTreeMap::from([(5, Bytes::from(vec![5, 6, 7, 8, 9]))]);
        assert_eq!(
            remove_range(&mut data, 10..=20),
            0,
            "remove outside right go wrong"
        );
        assert_eq!(
            data,
            BTreeMap::from([(5, Bytes::from(vec![5, 6, 7, 8, 9]))]),
            "remove outside right go wrong"
        );

        // remove from [<left, <right] within
        let mut data = BTreeMap::from([(5, Bytes::from(vec![5, 6, 7, 8, 9]))]);
        assert_eq!(
            remove_range(&mut data, 4..=8),
            4,
            "remove from [<left, <right] within go wrong"
        );
        assert_eq!(
            data,
            BTreeMap::from([(9, Bytes::from(vec![9]))]),
            "remove from [<left, <right] within go wrong"
        );

        // remove from [<left, <right] within 2
        let mut data = BTreeMap::from([(5, Bytes::from(vec![5, 6, 7, 8, 9]))]);
        assert_eq!(
            remove_range(&mut data, 4..=5),
            1,
            "remove from [<left, <right] within go wrong 2"
        );
        assert_eq!(
            data,
            BTreeMap::from([(6, Bytes::from(vec![6, 7, 8, 9]))]),
            "remove from [<left, <right] within go wrong 2"
        );

        // remove from [>left, >right] within
        let mut data = BTreeMap::from([(5, Bytes::from(vec![5, 6, 7, 8, 9]))]);
        assert_eq!(
            remove_range(&mut data, 6..=10),
            4,
            "remove from [>left, >right] within go wrong"
        );

        // remove from [<left, >right]
        let mut data = BTreeMap::from([(5, Bytes::from(vec![5, 6, 7, 8, 9]))]);
        assert_eq!(
            remove_range(&mut data, 4..=10),
            5,
            "remove from [<left, >right] go wrong"
        );
        assert_eq!(
            data,
            BTreeMap::new(),
            "remove from [<left, >right] go wrong"
        );

        //    -----  ------
        //  |  |
        let mut data = BTreeMap::from([
            (5, Bytes::from(vec![5, 6, 7, 8, 9])),
            (15, Bytes::from(vec![15, 16, 17, 18, 19])),
        ]);
        assert_eq!(
            remove_range(&mut data, 4..=7),
            3,
            "2 continues range remove case 1"
        );
        assert_eq!(
            data,
            BTreeMap::from([
                (8, Bytes::from(vec![8, 9])),
                (15, Bytes::from(vec![15, 16, 17, 18, 19]))
            ]),
            "2 continues range remove case 1"
        );

        //    -----    ------
        //  |        |
        let mut data = BTreeMap::from([
            (5, Bytes::from(vec![5, 6, 7, 8, 9])),
            (15, Bytes::from(vec![15, 16, 17, 18, 19])),
        ]);
        assert_eq!(
            remove_range(&mut data, 4..=10),
            5,
            "2 continues range remove case 2"
        );
        assert_eq!(
            data,
            BTreeMap::from([(15, Bytes::from(vec![15, 16, 17, 18, 19]))]),
            "2 continues range remove case 2"
        );
        //    -----    ------
        //  |            |
        let mut data = BTreeMap::from([
            (5, Bytes::from(vec![5, 6, 7, 8, 9])),
            (15, Bytes::from(vec![15, 16, 17, 18, 19])),
        ]);
        assert_eq!(
            remove_range(&mut data, 4..=17),
            8,
            "2 continues range remove case 3"
        );
        assert_eq!(
            data,
            BTreeMap::from([(18, Bytes::from(vec![18, 19]))]),
            "2 continues range remove case 3"
        );
        //    -----    ------
        //   |                |
        let mut data = BTreeMap::from([
            (5, Bytes::from(vec![5, 6, 7, 8, 9])),
            (15, Bytes::from(vec![15, 16, 17, 18, 19])),
        ]);
        assert_eq!(
            remove_range(&mut data, 4..=25),
            10,
            "2 continues range remove case 4"
        );
        assert_eq!(data, BTreeMap::new(), "2 continues range remove case 4");

        //   -----    ------
        //    | |

        let mut data = BTreeMap::from([
            (5, Bytes::from(vec![5, 6, 7, 8, 9])),
            (15, Bytes::from(vec![15, 16, 17, 18, 19])),
        ]);
        assert_eq!(
            remove_range(&mut data, 6..=8),
            3,
            "2 continues range remove case 5"
        );
        assert_eq!(
            data,
            BTreeMap::from([
                (5, Bytes::from(vec![5])),
                (9, Bytes::from(vec![9])),
                (15, Bytes::from(vec![15, 16, 17, 18, 19]))
            ]),
            "2 continues range remove case 5"
        );

        //  -----    ------
        //    |   |
        let mut data = BTreeMap::from([
            (5, Bytes::from(vec![5, 6, 7, 8, 9])),
            (15, Bytes::from(vec![15, 16, 17, 18, 19])),
        ]);
        assert_eq!(
            remove_range(&mut data, 8..=12),
            2,
            "2 continues range remove case 6"
        );
        assert_eq!(
            data,
            BTreeMap::from([
                (5, Bytes::from(vec![5, 6, 7])),
                (15, Bytes::from(vec![15, 16, 17, 18, 19]))
            ]),
            "2 continues range remove case 6"
        );
        // -----    ------
        //    |       |
        let mut data = BTreeMap::from([
            (5, Bytes::from(vec![5, 6, 7, 8, 9])),
            (15, Bytes::from(vec![15, 16, 17, 18, 19])),
        ]);
        assert_eq!(
            remove_range(&mut data, 8..=16),
            4,
            "2 continues range remove case 7"
        );
        assert_eq!(
            data,
            BTreeMap::from([
                (5, Bytes::from(vec![5, 6, 7])),
                (17, Bytes::from(vec![17, 18, 19]))
            ]),
            "2 continues range remove case 7"
        );

        // -----    ------
        //    |           |
        let mut data = BTreeMap::from([
            (5, Bytes::from(vec![5, 6, 7, 8, 9])),
            (15, Bytes::from(vec![15, 16, 17, 18, 19])),
        ]);
        assert_eq!(
            remove_range(&mut data, 8..=20),
            7,
            "2 continues range remove case 8"
        );
        assert_eq!(
            data,
            BTreeMap::from([(5, Bytes::from(vec![5, 6, 7]))]),
            "2 continues range remove case 8"
        );

        let mut data = BTreeMap::from([
            (5, Bytes::from(vec![5, 6, 7, 8, 9])),
            (15, Bytes::from(vec![15, 16, 17, 18, 19])),
            (25, Bytes::from(vec![25, 26, 27, 28, 29])),
            (35, Bytes::from(vec![35, 36, 37, 38, 39])),
        ]);
        assert_eq!(
            remove_range(&mut data, 6..=36),
            16,
            "2 continues range remove case 9"
        );
        assert_eq!(
            data,
            BTreeMap::from([
                (5, Bytes::from(vec![5])),
                (37, Bytes::from(vec![37, 38, 39])),
            ]),
            "2 continues range remove case 9"
        );
    }

    #[test]
    fn test_store_and_free() {
        let mut map = MemoryMap::new(100);
        map.store(0, vec![1, 2, 3, 4, 5].into());
        map.store(5, vec![6, 7, 8, 9, 10].into());
        let bytes = vec![42, 42, 42, 42].into();
        map.store_and_free(10, bytes, 10, 2);
        assert_eq!(map.inner.get_mut().unwrap().data_len, 2 + 4);
        assert_eq!(
            map.get(8..14).unwrap().into_iter().collect_vec(),
            vec![9, 10, 42, 42, 42, 42]
        );

        let map = MemoryMap::new(100);
        map.store(0, vec![1, 2, 3, 4, 5].into());
        map.store(6, vec![7, 8, 9, 10, 11].into());
        let bytes = vec![42, 42, 42, 42].into();
        map.store(5, bytes);
        assert_eq!(
            map.get(0..=10).unwrap().into_iter().collect_vec(),
            vec![1, 2, 3, 4, 5, 42, 7, 8, 9, 10, 11]
        );

        let mut map = MemoryMap::new(100);
        map.store(0, vec![0, 1, 2, 3, 4].into());
        map.store(6, vec![6, 7, 8, 9, 10].into());
        let bytes = vec![42, 42, 42, 42].into();
        map.store_and_free(5, bytes, 4, 2);
        // [0, 1, 2, 3, 4, 42, 6, 7, 8, 9, 10]
        //        |     ^   1  2  3  4
        //                              d  d
        assert_eq!(
            map.get(2..=8).unwrap().into_iter().collect_vec(),
            vec![2, 3, 4, 42, 6, 7, 8]
        );
        let bytes_last_pos = 5 + 3;
        let protect_zone_start = 4u64.saturating_sub(2);
        assert_eq!(
            map.inner.get_mut().unwrap().data_len,
            bytes_last_pos - protect_zone_start + 1
        );
    }
}
