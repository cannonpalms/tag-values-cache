//! Shared tag generation utilities for benchmarks and data generation
//!
//! This module provides functions to generate tag values consistent with
//! the schema used in the cardinality test data files.

use std::collections::BTreeSet;

/// Factorize a total cardinality into factors for each tag column
///
/// Given a desired total cardinality and number of columns, this returns
/// the cardinality for each column such that their product equals the total.
pub fn factorize_cardinality(total_cardinality: usize, num_columns: usize) -> Vec<usize> {
    if total_cardinality == 1 {
        return vec![1; num_columns];
    }

    let mut cardinalities = Vec::new();
    let mut remaining = total_cardinality;

    for _ in 0..num_columns - 1 {
        if remaining <= 1 {
            cardinalities.push(1);
            continue;
        }

        let target = (remaining as f64)
            .powf(1.0 / (num_columns - cardinalities.len()) as f64)
            .round() as usize;
        let target = target.max(2);

        let mut best = target;
        if !remaining.is_multiple_of(target) {
            for candidate in (2..=target + 5).rev() {
                if candidate <= remaining && remaining.is_multiple_of(candidate) {
                    best = candidate;
                    break;
                }
            }
        }
        cardinalities.push(best);
        remaining /= best;
    }

    cardinalities.push(remaining);
    cardinalities
}

/// Get the tag value for a given index
///
/// Generates values in the pattern: "a", "b", ..., "z", "aa", "ab", ...
pub fn get_tag_value(index: usize) -> String {
    if index == 0 {
        return "a".to_string();
    }

    if index < 26 {
        ((b'a' + index as u8) as char).to_string()
    } else if index < 26 + 26 * 26 {
        let idx = index - 26;
        let first = (b'a' + (idx / 26) as u8) as char;
        let second = (b'a' + (idx % 26) as u8) as char;
        format!("{}{}", first, second)
    } else {
        format!("t{}", index)
    }
}

/// Decode a combination index into tag values
///
/// Given a combination index and the cardinalities for each tag,
/// returns the tag values for that combination.
pub fn decode_tagset(combo_idx: usize, tag_cardinalities: &[usize]) -> Vec<String> {
    let mut remaining = combo_idx;
    let mut tag_indices = Vec::with_capacity(tag_cardinalities.len());

    for &card in tag_cardinalities.iter().rev() {
        tag_indices.push(remaining % card);
        remaining /= card;
    }
    tag_indices.reverse();

    tag_indices.into_iter().map(get_tag_value).collect()
}

/// Generate a TagSet (BTreeSet of key-value pairs) for a given combination index
///
/// Creates a tagset with 8 tags (t0 through t7) using the standard
/// cardinality factorization and tag value generation.
pub fn generate_tagset_for_index(
    combo_idx: usize,
    tag_cardinalities: &[usize],
) -> BTreeSet<(String, String)> {
    let tag_values = decode_tagset(combo_idx, tag_cardinalities);
    let mut tagset = BTreeSet::new();

    for (i, value) in tag_values.into_iter().enumerate() {
        tagset.insert((format!("t{}", i), value));
    }

    tagset
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factorize_cardinality() {
        // Test cardinality 1
        assert_eq!(factorize_cardinality(1, 8), vec![1, 1, 1, 1, 1, 1, 1, 1]);

        // Test cardinality 256 (2^8)
        let factors = factorize_cardinality(256, 8);
        let product: usize = factors.iter().product();
        assert_eq!(product, 256);

        // Test cardinality 1000
        let factors = factorize_cardinality(1000, 8);
        let product: usize = factors.iter().product();
        assert_eq!(product, 1000);
    }

    #[test]
    fn test_get_tag_value() {
        assert_eq!(get_tag_value(0), "a");
        assert_eq!(get_tag_value(1), "b");
        assert_eq!(get_tag_value(25), "z");
        assert_eq!(get_tag_value(26), "aa");
        assert_eq!(get_tag_value(27), "ab");
        assert_eq!(get_tag_value(51), "az");
        assert_eq!(get_tag_value(52), "ba");
    }

    #[test]
    fn test_decode_tagset() {
        let cardinalities = vec![2, 2, 2, 1, 1, 1, 1, 1];

        // First combination (0)
        let tags = decode_tagset(0, &cardinalities);
        assert_eq!(tags, vec!["a", "a", "a", "a", "a", "a", "a", "a"]);

        // Second combination (1)
        let tags = decode_tagset(1, &cardinalities);
        assert_eq!(tags, vec!["a", "a", "b", "a", "a", "a", "a", "a"]);

        // Third combination (2)
        let tags = decode_tagset(2, &cardinalities);
        assert_eq!(tags, vec!["a", "b", "a", "a", "a", "a", "a", "a"]);
    }

    #[test]
    fn test_generate_tagset_for_index() {
        let cardinalities = vec![2, 2, 1, 1, 1, 1, 1, 1];
        let tagset = generate_tagset_for_index(0, &cardinalities);

        assert_eq!(tagset.len(), 8);
        assert!(tagset.contains(&("t0".to_string(), "a".to_string())));
        assert!(tagset.contains(&("t1".to_string(), "a".to_string())));
    }
}
