use std::collections::HashMap;

use arrow::array::{BooleanArray, StringArray};

/// A string array stored as a dictionary: unique values + integer indices.
///
/// Instead of storing ["US", "DE", "US", "JP", "US"] (5 strings),
/// stores dictionary: ["US", "DE", "JP"] + indices: [0, 1, 0, 2, 0].
///
/// Operations like count_eq and filter_eq work on the integer indices,
/// never touching the actual strings — this is "computing on compressed data."
pub struct DictArray {
    values: Vec<String>,
    indices: Vec<u32>,
}

impl DictArray {
    pub fn from_strings(data: &[&str]) -> Self {
        let mut value_to_index: HashMap<&str, u32> = HashMap::new();
        let mut values = Vec::new();
        let mut indices = Vec::with_capacity(data.len());

        for &s in data {
            let idx = match value_to_index.get(s) {
                Some(&idx) => idx,
                None => {
                    let idx = values.len() as u32;
                    values.push(s.to_string());
                    value_to_index.insert(s, idx);
                    idx
                }
            };
            indices.push(idx);
        }

        Self { values, indices }
    }

    pub fn len(&self) -> usize {
        self.indices.len()
    }

    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    pub fn num_unique(&self) -> usize {
        self.values.len()
    }

    pub fn value(&self, i: usize) -> &str {
        &self.values[self.indices[i] as usize]
    }

    /// Count values equal to target — operates on integer indices, never touches strings.
    pub fn count_eq(&self, target: &str) -> usize {
        let target_idx = match self.find_index(target) {
            Some(idx) => idx,
            None => return 0,
        };
        self.indices
            .iter()
            .filter(|&&idx| idx == target_idx)
            .count()
    }

    /// Boolean mask for values equal to target — operates on integer indices.
    pub fn filter_eq(&self, target: &str) -> BooleanArray {
        let target_idx = self.find_index(target);
        let bools: Vec<bool> = match target_idx {
            Some(idx) => self.indices.iter().map(|&i| i == idx).collect(),
            None => vec![false; self.indices.len()],
        };
        BooleanArray::from(bools)
    }

    /// Decompress to Arrow StringArray for interop with the rest of the system.
    pub fn to_arrow(&self) -> StringArray {
        let strings: Vec<&str> = self
            .indices
            .iter()
            .map(|&i| self.values[i as usize].as_str())
            .collect();
        StringArray::from(strings)
    }

    pub fn memory_size_bytes(&self) -> usize {
        let dict_size: usize = self.values.iter().map(|s| s.len()).sum();
        let indices_size = self.indices.len() * std::mem::size_of::<u32>();
        dict_size + indices_size
    }

    pub fn uncompressed_size_bytes(&self) -> usize {
        self.indices
            .iter()
            .map(|&i| self.values[i as usize].len())
            .sum()
    }

    fn find_index(&self, target: &str) -> Option<u32> {
        self.values
            .iter()
            .position(|v| v == target)
            .map(|i| i as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    fn sample() -> DictArray {
        DictArray::from_strings(&["US", "DE", "US", "JP", "US", "DE", "US", "JP"])
    }

    #[test]
    fn basic_properties() {
        let arr = sample();
        assert_eq!(arr.len(), 8);
        assert_eq!(arr.num_unique(), 3);
        assert_eq!(arr.value(0), "US");
        assert_eq!(arr.value(1), "DE");
        assert_eq!(arr.value(3), "JP");
    }

    #[test]
    fn count_eq_works_on_indices() {
        let arr = sample();
        assert_eq!(arr.count_eq("US"), 4);
        assert_eq!(arr.count_eq("DE"), 2);
        assert_eq!(arr.count_eq("JP"), 2);
        assert_eq!(arr.count_eq("FR"), 0);
    }

    #[test]
    fn filter_eq_returns_mask() {
        let arr = sample();
        let mask = arr.filter_eq("US");
        assert_eq!(mask.len(), 8);
        assert!(mask.value(0));
        assert!(!mask.value(1));
        assert!(mask.value(2));
    }

    #[test]
    fn to_arrow_round_trips() {
        let arr = sample();
        let arrow_arr = arr.to_arrow();
        assert_eq!(arrow_arr.len(), 8);
        assert_eq!(arrow_arr.value(0), "US");
        assert_eq!(arrow_arr.value(1), "DE");
    }

    #[test]
    fn compressed_is_smaller_for_long_strings() {
        let arr = DictArray::from_strings(&[
            "United States",
            "Germany",
            "United States",
            "Japan",
            "United States",
            "Germany",
            "United States",
            "Japan",
        ]);
        // dict: 3 strings (~27 bytes) + 8 indices × 4 bytes = 59 bytes
        // uncompressed: 8 strings averaging ~10 bytes = ~80 bytes
        assert!(arr.memory_size_bytes() < arr.uncompressed_size_bytes());
    }

    #[test]
    fn short_strings_show_index_overhead() {
        let arr = sample(); // "US", "DE", "JP" — 2 bytes each
                            // With u32 indices (4 bytes > 2 byte strings), dictionary is larger
                            // This is why real systems use bit-packed indices (2 bits for 3 values)
        assert!(arr.memory_size_bytes() >= arr.uncompressed_size_bytes());
    }
}
