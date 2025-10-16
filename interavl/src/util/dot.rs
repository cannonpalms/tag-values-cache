use std::fmt::{Display, Write};

use crate::node::Node;

/// Print a tree in Graphviz DOT format.
///
/// The output can be rendered with tools like `dot` or online at
/// https://dreampuf.github.io/GraphvizOnline/
pub fn print_dot<R, V>(n: &Node<R, V>) -> String
where
    V: Display,
    R: Display + Ord,
{
    let mut buf = String::new();

    writeln!(buf, "digraph {{").unwrap();
    writeln!(buf, r#"bgcolor = "transparent";"#).unwrap();
    writeln!(
        buf,
        r#"node [shape = record; style = filled; fontcolor = orange4; fillcolor = white;];"#
    )
    .unwrap();
    recurse(n, &mut buf);
    writeln!(buf, "}}").unwrap();

    buf
}

fn recurse<V, R, W>(n: &Node<R, V>, buf: &mut W)
where
    W: std::fmt::Write,
    V: Display,
    R: Display + Ord,
{
    writeln!(
        buf,
        r#""{}" [label="{} | {} | {{ max={} | h={} }}"];"#,
        n.interval(),
        n.interval(),
        n.value(),
        n.subtree_max(),
        n.height(),
    )
    .unwrap();

    for v in [n.left(), n.right()] {
        match v {
            Some(v) => {
                writeln!(
                    buf,
                    "\"{}\" -> \"{}\" [color = \"orange1\";];",
                    n.interval(),
                    v.interval()
                )
                .unwrap();
                recurse(v, buf);
            }
            None => {
                writeln!(buf, "\"null_{}\" [shape=point,style=invis];", n.interval()).unwrap();
                writeln!(
                    buf,
                    "\"{}\" -> \"null_{}\" [style=invis];",
                    n.interval(),
                    n.interval()
                )
                .unwrap();
            }
        };
    }
}
