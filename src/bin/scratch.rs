use std::fmt::{Debug, Display};

fn main() {
    println!("=== Testing intervaltree::IntervalTree ===");
    let nodes = vec![
        (1..5, "A"),
        (10..15, "B"),
        (20..25, "C"),
        (12..18, "D"), // Overlapping interval
        (12..18, "D"), // Duplicate interval & value -- no node added
        (12..18, "E"), // Duplicate interval, different value -- old node gets updated to new value
        (5..10, "F"),
    ];
    let tree = intervaltree::IntervalTree::from_iter(nodes);
    print_tree(&tree);

    // println!("\n=== Testing interavl::IntervalTree ===");
    // play_with_interavl_trees();
}

#[expect(dead_code)]
fn play_with_interavl_trees() {
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

    tree.insert(5..10, "F");
    print_tree(&tree);

    // Print DOT format
    println!("\n=== DOT FORMAT ===");
    println!("{}", print_dot(&tree));
}

// Trait for types that can be printed as interval trees
trait PrintableTree {
    fn print(&self);
}

// Implementation for interavl::IntervalTree
impl<R, V> PrintableTree for interavl::IntervalTree<R, V>
where
    R: Ord + Clone + Debug,
    V: Debug,
{
    fn print(&self) {
        let items: Vec<_> = self.iter().map(|(r, v)| (r.clone(), v)).collect();
        println!("===================");
        println!("Intervals:");
        for (range, value) in &items {
            println!("  {range:?} -> {value:?}");
        }
        println!("\nTree: {self:#?}");
        println!("===================\n");
    }
}

// Implementation for intervaltree::IntervalTree
impl<K, V> PrintableTree for intervaltree::IntervalTree<K, V>
where
    K: Ord + Clone + Debug,
    V: Debug,
{
    fn print(&self) {
        println!("===================");
        println!("Intervals:");
        for element in self.iter() {
            println!("  {:?} -> {:?}", element.range, element.value);
        }
        println!("\nTree: {self:#?}");
        println!("===================\n");
    }
}

// Single entry point function
fn print_tree<T: PrintableTree>(tree: &T) {
    tree.print();
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
