use std::io::Write;
use std::process::Command;

use crate::flow::Flow;

pub(crate) fn write_flow_to_graphviz(
    flow: &Flow, filename: &str, open_jpg: bool,
) -> std::io::Result<()> {
    let mut file = std::fs::File::create(filename)?;
    file.write_all("digraph example1 {\n".as_bytes())?;
    file.write_all("    node [shape=record];\n".as_bytes())?;
    file.write_all("    rankdir=LR;\n".as_bytes())?; // direction of DAG
    file.write_all("    splines=polyline;\n".as_bytes())?;
    file.write_all("    nodesep=0.5;\n".as_bytes())?;

    for node in flow.nodes.iter() {
        let nodestr =
            format!("    Node{}[label=\"{}\"];\n", node.id(), node.name());
        file.write_all(nodestr.as_bytes())?;

        for child in node.children().iter() {
            let edge = format!("    Node{} -> Node{};\n", child, node.id());
            file.write_all(edge.as_bytes())?;
        }
    }
    file.write_all("}\n".as_bytes())?;
    drop(file);

    let ofilename = format!("{}.jpg", filename);
    let oflag = format!("-o{}.jpg", filename);

    // dot -Tjpg -oex.jpg exampl1.dot
    let _cmd = Command::new("dot")
        .arg("-Tjpg")
        .arg(oflag)
        .arg(filename)
        .status()
        .expect("failed to execute process");

    if open_jpg {
        let _cmd = Command::new("open")
            .arg(ofilename)
            .status()
            .expect("failed to execute process");
    }

    Ok(())
}

