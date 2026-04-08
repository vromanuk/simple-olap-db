use arrow::array::Int64Array;

/// An integer array stored as runs of (value, count) pairs.
///
/// Instead of storing [1, 1, 1, 2, 2, 3, 3, 3, 3] (9 values),
/// stores [(1, 3), (2, 2), (3, 4)] (3 runs).
///
/// Best for sorted or clustered data where the same value repeats consecutively.
/// Worst case (no repeats) uses 2x more memory than uncompressed.
pub struct RleArray {
    runs: Vec<Run>,
    total_len: usize,
}

struct Run {
    value: i64,
    count: usize,
}

impl RleArray {
    pub fn from_values(data: &[i64]) -> Self {
        let mut runs = Vec::new();
        let mut i = 0;

        while i < data.len() {
            let value = data[i];
            let mut count = 1;
            while i + count < data.len() && data[i + count] == value {
                count += 1;
            }
            runs.push(Run { value, count });
            i += count;
        }

        Self {
            total_len: data.len(),
            runs,
        }
    }

    pub fn len(&self) -> usize {
        self.total_len
    }

    pub fn is_empty(&self) -> bool {
        self.total_len == 0
    }

    pub fn num_runs(&self) -> usize {
        self.runs.len()
    }

    pub fn value(&self, i: usize) -> i64 {
        let mut offset = 0;
        for run in &self.runs {
            if i < offset + run.count {
                return run.value;
            }
            offset += run.count;
        }
        panic!("index {i} out of bounds (len={})", self.total_len);
    }

    /// Sum all values — operates on runs, not individual values.
    /// For N values in R runs, this does R multiplications instead of N additions.
    pub fn sum(&self) -> i64 {
        self.runs.iter().map(|r| r.value * r.count as i64).sum()
    }

    /// Count values equal to target — operates on runs.
    pub fn count_eq(&self, target: i64) -> usize {
        self.runs
            .iter()
            .filter(|r| r.value == target)
            .map(|r| r.count)
            .sum()
    }

    /// Min value — checks each run's value (R comparisons, not N).
    pub fn min(&self) -> Option<i64> {
        self.runs.iter().map(|r| r.value).min()
    }

    /// Max value — checks each run's value (R comparisons, not N).
    pub fn max(&self) -> Option<i64> {
        self.runs.iter().map(|r| r.value).max()
    }

    pub fn to_arrow(&self) -> Int64Array {
        let mut values = Vec::with_capacity(self.total_len);
        for run in &self.runs {
            values.extend(std::iter::repeat_n(run.value, run.count));
        }
        Int64Array::from(values)
    }

    pub fn memory_size_bytes(&self) -> usize {
        self.runs.len() * std::mem::size_of::<Run>()
    }

    pub fn uncompressed_size_bytes(&self) -> usize {
        self.total_len * std::mem::size_of::<i64>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sorted_data() -> RleArray {
        RleArray::from_values(&[1, 1, 1, 2, 2, 3, 3, 3, 3])
    }

    #[test]
    fn basic_properties() {
        let arr = sorted_data();
        assert_eq!(arr.len(), 9);
        assert_eq!(arr.num_runs(), 3);
        assert_eq!(arr.value(0), 1);
        assert_eq!(arr.value(2), 1);
        assert_eq!(arr.value(3), 2);
        assert_eq!(arr.value(5), 3);
    }

    #[test]
    fn sum_operates_on_runs() {
        let arr = sorted_data();
        // 1*3 + 2*2 + 3*4 = 3 + 4 + 12 = 19
        assert_eq!(arr.sum(), 19);
    }

    #[test]
    fn count_eq_operates_on_runs() {
        let arr = sorted_data();
        assert_eq!(arr.count_eq(1), 3);
        assert_eq!(arr.count_eq(2), 2);
        assert_eq!(arr.count_eq(3), 4);
        assert_eq!(arr.count_eq(99), 0);
    }

    #[test]
    fn min_max() {
        let arr = sorted_data();
        assert_eq!(arr.min(), Some(1));
        assert_eq!(arr.max(), Some(3));
    }

    #[test]
    fn to_arrow_round_trips() {
        let arr = sorted_data();
        let arrow_arr = arr.to_arrow();
        assert_eq!(arrow_arr.len(), 9);
        assert_eq!(arrow_arr.value(0), 1);
        assert_eq!(arrow_arr.value(8), 3);
    }

    #[test]
    fn compressed_is_smaller_for_clustered_data() {
        let arr = sorted_data();
        // 3 runs × 16 bytes = 48 bytes vs 9 values × 8 bytes = 72 bytes
        assert!(arr.memory_size_bytes() < arr.uncompressed_size_bytes());
    }

    #[test]
    fn no_compression_for_unique_data() {
        let arr = RleArray::from_values(&[1, 2, 3, 4, 5]);
        // 5 runs × 16 bytes = 80 bytes vs 5 values × 8 bytes = 40 bytes
        // RLE is WORSE for unique data
        assert!(arr.memory_size_bytes() > arr.uncompressed_size_bytes());
        assert_eq!(arr.num_runs(), 5);
    }
}
