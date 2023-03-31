use std::cmp::Ordering;
use std::sync::Arc;
use ahash::AHashMap;

use flume::Receiver;
use log::{error, info};
use rdkafka::consumer::{Consumer, StreamConsumer};

use crate::consumer::Offset;
use crate::consumer_stats::ContextWithStats;

/// Sorted structure with O(1) additions to the end in the most cases, from O(1) to O(log n) removals
/// and O(1) gets minimum
#[derive(Default)]
struct PartitionOffsetManager {
    offsets: Vec<i64>,
    start: usize,
    len: usize,
    count: usize,
    last_added: Option<i64>,
}

impl PartitionOffsetManager {
    pub fn new(capacity: usize) -> Self {
        Self {
            offsets: vec![0; capacity],
            ..Default::default()
        }
    }

    pub fn append(&mut self, offset: i64) {
        assert!(self.len == 0 || offset > self.offset(self.len - 1));
        if self.len == self.offsets.len() {
            if self.count < self.offsets.len() {
                self.optimize();
                assert!(self.len < self.offsets.len());
            } else {
                self.grow();
            }
        }
        self.set_offset(self.len, offset);
        self.last_added = Some(offset);
        self.len += 1;
        self.count += 1;
    }

    pub fn remove(&mut self, offset: i64) -> Option<i64> {
        assert!(self.len > 0);
        let old_len = self.len;
        while self.offsets[self.start] == offset {
            self.start = self.move_right(self.start);
            self.len -= 1;
        }
        if self.len != old_len {
            self.count -= 1;
            if self.len == 0 {
                return self.last_added;
            }
            return Some(self.offsets[self.start] - 1)
        }
        assert!(self.len > 1);
        let mut last_index = self.absolute_index(self.len - 1);
        while self.offsets[last_index] == offset {
            last_index = self.move_left(last_index);
            self.len -= 1;
        }
        if self.len != old_len {
            self.count -= 1;
            return None;
        }
        match self.binary_search(offset) {
            None => panic!("Offset {offset} not found in partition!"),
            Some(index) => {
                let mut start_index = self.absolute_index(index);
                while self.offsets[start_index] == offset {
                    start_index = self.move_left(start_index)
                }
                let fill_value = self.offsets[start_index];
                loop {
                    start_index = self.move_right(start_index);
                    if self.offsets[start_index] != offset {
                        break;
                    }
                    self.offsets[start_index] = fill_value;
                }
            },
        }
        self.count -= 1;
        None
    }

    fn binary_search(&self, offset: i64) -> Option<usize> {
        let mut left = 0;
        let mut right = self.len;
        while left < right {
            let middle = left + (right - left) / 2;
            match self.offset(middle).cmp(&offset) {
                Ordering::Less => left = middle + 1,
                Ordering::Equal => return Some(middle),
                Ordering::Greater => right = middle,
            }
        }
        None
    }

    #[inline]
    fn absolute_index(&self, index: usize) -> usize {
        let mut result = self.start + index;
        if result >= self.offsets.len() {
            result -= self.offsets.len();
        }
        result
    }

    fn optimize(&mut self) {
        if self.len < 2 {
            return;
        }
        let mut dst_index = 1;
        let mut prev_value = self.offsets[self.start];
        for i in 1..self.len {
            let cur_value = self.offset(i);
            if cur_value != prev_value {
                self.set_offset(dst_index, cur_value);
                dst_index += 1;
                prev_value = cur_value;
            }
        }
        self.len = dst_index;
    }

    fn grow(&mut self) {
        let old_capacity = self.offsets.len();
        self.offsets.resize(self.offsets.len() * 2, 0);
        self.offsets.copy_within(0..self.len - (old_capacity - self.start - 1), old_capacity);
    }

    #[inline]
    fn offset(&self, index: usize) -> i64 {
        self.offsets[self.absolute_index(index)]
    }

    #[inline]
    fn set_offset(&mut self, index: usize, offset: i64) {
        let absolute_index = self.absolute_index(index);
        self.offsets[absolute_index] = offset;
    }

    #[inline]
    fn move_left(&self, index: usize) -> usize {
        if index == 0 {
            self.offsets.len() - 1
        } else {
            index - 1
        }
    }

    #[inline]
    fn move_right(&self, mut index: usize) -> usize {
        index += 1;
        if index == self.offsets.len() {
            return 0;
        }
        index
    }
}

pub struct OffsetManager {
    topic: String,
    consumer: Arc<StreamConsumer<ContextWithStats>>,
    partitions: AHashMap<i32, PartitionOffsetManager>,
}

impl OffsetManager {
    const INITIAL_CAPACITY: usize = 1024;

    pub fn new(topic: String, consumer: Arc<StreamConsumer<ContextWithStats>>) -> Self {
        Self { topic, consumer, partitions: AHashMap::new() }
    }

    pub fn append(&mut self, offset: &Offset) {
        self.partitions.entry(offset.partition)
            .or_insert(PartitionOffsetManager::new(Self::INITIAL_CAPACITY))
            .append(offset.offset);
    }

    pub fn remove(&mut self, offset: Offset) {
        let Offset { partition, offset } = offset;
        let partition_manager = self.partitions.get_mut(&partition)
            .unwrap_or_else(|| panic!("Partition manager for partition {} for topic `{}` not found", partition, self.topic));
        if let Some(processed_upto) = partition_manager.remove(offset) {
            self.consumer.store_offset(&self.topic, partition, processed_upto)
                .unwrap_or_else(|err| error!("Failed to update offset for topic `{}`. Kafka error: {err}", self.topic));
        }
    }
}

pub enum OffsetManagerCommand {
    StartProcessing(Offset),
    ProcessedSuccessfully(Offset),
}

pub async fn offset_manager_service(
    topic: String,
    consumer: Arc<StreamConsumer<ContextWithStats>>,
    receiver: Receiver<OffsetManagerCommand>,
) {
    let mut offset_manager = OffsetManager::new(topic.clone(), consumer);
    while let Ok(command) = receiver.recv_async().await {
        match command {
            OffsetManagerCommand::StartProcessing(offset) => offset_manager.append(&offset),
            OffsetManagerCommand::ProcessedSuccessfully(offset) => offset_manager.remove(offset),
        }
    }

    info!("Offset manager service for topic: `{topic}` is shut down");
}

#[cfg(test)]
mod tests {
    use super::PartitionOffsetManager;

    #[test]
    fn test_append() {
        let mut offset_manager = PartitionOffsetManager::new(21);
        fill_20(&mut offset_manager);
    }

    #[test]
    fn test_remove() {
        let mut offset_manager = PartitionOffsetManager::new(21);
        fill_20(&mut offset_manager);
        assert_eq!(offset_manager.remove(0), Some(0));

        assert_eq!(offset_manager.start, 1);
        assert_eq!(offset_manager.len, 19);
        assert_eq!(offset_manager.count, 19);
        assert_eq!(offset_manager.last_added, Some(19));
        assert_eq!(offset_manager.offsets[0..21], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 0]);

        assert_eq!(offset_manager.remove(1), Some(1));

        assert_eq!(offset_manager.start, 2);
        assert_eq!(offset_manager.len, 18);
        assert_eq!(offset_manager.count, 18);
        assert_eq!(offset_manager.last_added, Some(19));
        assert_eq!(offset_manager.offsets[0..21], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 0]);

        assert_eq!(offset_manager.remove(11), None);

        assert_eq!(offset_manager.start, 2);
        assert_eq!(offset_manager.len, 18);
        assert_eq!(offset_manager.count, 17);
        assert_eq!(offset_manager.last_added, Some(19));
        assert_eq!(offset_manager.offsets[0..21], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 12, 13, 14, 15, 16, 17, 18, 19, 0]);

        assert_eq!(offset_manager.remove(13), None);

        assert_eq!(offset_manager.start, 2);
        assert_eq!(offset_manager.len, 18);
        assert_eq!(offset_manager.count, 16);
        assert_eq!(offset_manager.last_added, Some(19));
        assert_eq!(offset_manager.offsets[0..21], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 12, 12, 14, 15, 16, 17, 18, 19, 0]);

        assert_eq!(offset_manager.remove(12), None);

        assert_eq!(offset_manager.start, 2);
        assert_eq!(offset_manager.len, 18);
        assert_eq!(offset_manager.count, 15);
        assert_eq!(offset_manager.last_added, Some(19));
        assert_eq!(offset_manager.offsets[0..21], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 10, 10, 14, 15, 16, 17, 18, 19, 0]);

        assert_eq!(offset_manager.remove(10), None);

        assert_eq!(offset_manager.start, 2);
        assert_eq!(offset_manager.len, 18);
        assert_eq!(offset_manager.count, 14);
        assert_eq!(offset_manager.last_added, Some(19));
        assert_eq!(offset_manager.offsets[0..21], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 9, 9, 9, 14, 15, 16, 17, 18, 19, 0]);

        assert_eq!(offset_manager.remove(18), None);

        assert_eq!(offset_manager.start, 2);
        assert_eq!(offset_manager.len, 18);
        assert_eq!(offset_manager.count, 13);
        assert_eq!(offset_manager.last_added, Some(19));
        assert_eq!(offset_manager.offsets[0..21], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 9, 9, 9, 14, 15, 16, 17, 17, 19, 0]);

        assert_eq!(offset_manager.remove(19), None);

        assert_eq!(offset_manager.start, 2);
        assert_eq!(offset_manager.len, 17);
        assert_eq!(offset_manager.count, 12);
        assert_eq!(offset_manager.last_added, Some(19));
        assert_eq!(offset_manager.offsets[0..21], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 9, 9, 9, 14, 15, 16, 17, 17, 19, 0]);

        assert_eq!(offset_manager.remove(17), None);
        assert_eq!(offset_manager.remove(4), None);
        assert_eq!(offset_manager.remove(2), Some(2));
        assert_eq!(offset_manager.remove(3), Some(4));
        assert_eq!(offset_manager.remove(14), None);
        assert_eq!(offset_manager.remove(7), None);
        assert_eq!(offset_manager.remove(6), None);
        assert_eq!(offset_manager.remove(16), None);
        assert_eq!(offset_manager.remove(5), Some(7));
        assert_eq!(offset_manager.remove(15), None);
        assert_eq!(offset_manager.remove(8), Some(8));
        assert_eq!(offset_manager.remove(9), Some(19));

        assert_eq!(offset_manager.start, 15);
        assert_eq!(offset_manager.len, 0);
        assert_eq!(offset_manager.count, 0);
        assert_eq!(offset_manager.last_added, Some(19));
        assert_eq!(offset_manager.offsets[0..21], [0, 1, 2, 3, 3, 5, 5, 5, 8, 9, 9, 9, 9, 9, 9, 15, 16, 17, 17, 19, 0]);
    }

    #[test]
    fn test_append_after_remove() {
        let mut offset_manager = PartitionOffsetManager::new(21);
        fill_20(&mut offset_manager);
        offset_manager.remove(18);
        offset_manager.remove(19);
        assert_eq!(offset_manager.start, 0);
        assert_eq!(offset_manager.len, 19);
        assert_eq!(offset_manager.count, 18);
        assert_eq!(offset_manager.last_added, Some(19));
        assert_eq!(offset_manager.offsets[0..21], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 17, 19, 0]);

        offset_manager.append(20);
        assert_eq!(offset_manager.start, 0);
        assert_eq!(offset_manager.len, 20);
        assert_eq!(offset_manager.count, 19);
        assert_eq!(offset_manager.last_added, Some(20));
        assert_eq!(offset_manager.offsets[0..21], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 17, 20, 0]);
    }

    #[test]
    fn test_auto_optimize() {
        let mut offset_manager = PartitionOffsetManager::new(8);
        for i in 0..8 {
            offset_manager.append(i as i64);
        }
        assert_eq!(offset_manager.start, 0);
        assert_eq!(offset_manager.len, 8);
        assert_eq!(offset_manager.count, 8);
        assert_eq!(offset_manager.last_added, Some(7));
        assert_eq!(offset_manager.offsets, [0, 1, 2, 3, 4, 5, 6, 7]);

        for i in 2..6 {
            offset_manager.remove(i);
        }

        assert_eq!(offset_manager.start, 0);
        assert_eq!(offset_manager.len, 8);
        assert_eq!(offset_manager.count, 4);
        assert_eq!(offset_manager.last_added, Some(7));
        assert_eq!(offset_manager.offsets, [0, 1, 1, 1, 1, 1, 6, 7]);

        offset_manager.append(8);
        assert_eq!(offset_manager.start, 0);
        assert_eq!(offset_manager.len, 5);
        assert_eq!(offset_manager.count, 5);
        assert_eq!(offset_manager.last_added, Some(8));
        assert_eq!(offset_manager.offsets, [0, 1, 6, 7, 8, 1, 6, 7]);

        offset_manager.remove(6);
        offset_manager.remove(1);
        offset_manager.remove(0);

        assert_eq!(offset_manager.start, 3);
        assert_eq!(offset_manager.len, 2);
        assert_eq!(offset_manager.count, 2);
        assert_eq!(offset_manager.last_added, Some(8));
        assert_eq!(offset_manager.offsets, [0, 0, 0, 7, 8, 1, 6, 7]);

        for i in 9..15 {
            offset_manager.append(i);
        }

        assert_eq!(offset_manager.start, 3);
        assert_eq!(offset_manager.len, 8);
        assert_eq!(offset_manager.count, 8);
        assert_eq!(offset_manager.last_added, Some(14));
        assert_eq!(offset_manager.offsets, [12, 13, 14, 7, 8, 9, 10, 11]);

        offset_manager.remove(13);
        offset_manager.remove(10);
        offset_manager.remove(9);
        offset_manager.remove(14);

        assert_eq!(offset_manager.start, 3);
        assert_eq!(offset_manager.len, 7);
        assert_eq!(offset_manager.count, 4);
        assert_eq!(offset_manager.last_added, Some(14));
        assert_eq!(offset_manager.offsets, [12, 12, 14, 7, 8, 8, 8, 11]);

        for i in 16..20 {
            offset_manager.append(i);
        }

        assert_eq!(offset_manager.start, 3);
        assert_eq!(offset_manager.len, 8);
        assert_eq!(offset_manager.count, 8);
        assert_eq!(offset_manager.last_added, Some(19));
        assert_eq!(offset_manager.offsets, [17, 18, 19, 7, 8, 11, 12, 16]);
    }

    #[test]
    fn test_auto_grow() {
        let mut offset_manager = PartitionOffsetManager::new(5);
        for i in 0..5 {
            offset_manager.append(i as i64);
        }
        assert_eq!(offset_manager.start, 0);
        assert_eq!(offset_manager.len, 5);
        assert_eq!(offset_manager.count, 5);
        assert_eq!(offset_manager.last_added, Some(4));
        assert_eq!(offset_manager.offsets, [0, 1, 2, 3, 4]);

        for i in 0..2 {
            offset_manager.remove(i);
        }

        assert_eq!(offset_manager.start, 2);
        assert_eq!(offset_manager.len, 3);
        assert_eq!(offset_manager.count, 3);
        assert_eq!(offset_manager.last_added, Some(4));
        assert_eq!(offset_manager.offsets, [0, 1, 2, 3, 4]);

        offset_manager.append(5);
        offset_manager.append(6);

        assert_eq!(offset_manager.start, 2);
        assert_eq!(offset_manager.len, 5);
        assert_eq!(offset_manager.count, 5);
        assert_eq!(offset_manager.last_added, Some(6));
        assert_eq!(offset_manager.offsets, [5, 6, 2, 3, 4]);

        offset_manager.append(7);

        assert_eq!(offset_manager.start, 2);
        assert_eq!(offset_manager.len, 6);
        assert_eq!(offset_manager.count, 6);
        assert_eq!(offset_manager.last_added, Some(7));
        assert_eq!(offset_manager.offsets, [5, 6, 2, 3, 4, 5, 6, 7, 0, 0]);
    }

    fn fill_20(offset_manager: &mut PartitionOffsetManager) {
        assert_eq!(offset_manager.last_added, None);
        for i in 0..20 {
            offset_manager.append(i);
            assert_eq!(offset_manager.start, 0);
            assert_eq!(offset_manager.last_added, Some(i));
            assert_eq!(offset_manager.count, i as usize + 1);
            assert_eq!(offset_manager.len, i as usize + 1);
        }

        assert_eq!(offset_manager.offsets, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 0]);
    }
}
