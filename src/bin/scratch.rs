use std::fmt::{Debug, Display};

fn main() {
    let mut tree = interavl::IntervalTree::default();

    tree.insert(1..5, "A");
    print_tree(&tree);

    tree.insert(10..15, "B");
    print_tree(&tree);

    tree.insert(20..25, "C");
    print_tree(&tree);

    // Overlapping interval
    tree.insert(12..18, "D");
    print_tree(&tree);

    // Duplicate interval & value -- no node added
    tree.insert(12..18, "D");
    print_tree(&tree);

    // Duplicate interval, different value -- old node gets updated to new value
    tree.insert(12..18, "E");
    print_tree(&tree);

    // Print DOT format
    println!("\n=== DOT FORMAT ===");
    println!("{}", print_dot(&tree));
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

fn print_dot<R, V>(tree: &interavl::IntervalTree<R, V>) -> String
where
    V: Display,
    R: Display + Ord,
{
    match tree.root() {
        Some(root) => interavl::util::dot::print_dot(root),
        None => "digraph {\n}\n".to_string(),
    }
}
