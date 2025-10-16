use std::fmt::Debug;

fn main() {
    let mut tree = interavl::IntervalTree::default();

    tree.insert(1..5, 0);
    print_tree(&tree);

    tree.insert(10..15, 1);
    print_tree(&tree);

    tree.insert(20..25, 2);
    print_tree(&tree);

    // Overlapping interval
    tree.insert(12..18, 3);
    print_tree(&tree);

    // Duplicate interval & value -- no node added
    tree.insert(12..18, 3);
    print_tree(&tree);

    // Duplicate interval, different value -- old node gets updated to new value
    tree.insert(12..18, 4);
    print_tree(&tree);

    // Note: Cannot implement DOT format printing because:
    // - interavl::node::Node is not part of the public API
    // - IntervalTree doesn't expose a method to access the root or internal structure
    // - The test_utils::print_dot function only works within the interavl crate's tests
}

fn print_tree<R, V>(tree: &interavl::IntervalTree<R, V>)
where
    R: Ord + PartialEq + Debug + Clone,
    V: Debug,
{
    println!("===================");
    println!("Intervals:");
    for interval in tree.iter() {
        println!("  {interval:?}");
    }
    println!("\nTree: {tree:#?}");
    println!("===================\n");
}
