use std::{fs, time::SystemTime};

use queqlite_quepaxa::{Command, CommandKind, Consensus, ThreeNodeConsensus};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let suffix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_nanos();
    let base = std::env::temp_dir().join(format!("queqlite-quepaxa-{suffix}"));
    let roots = [base.join("n1"), base.join("n2"), base.join("n3")];

    let consensus = ThreeNodeConsensus::new("example", "n1", 1, 1, roots)?;
    let entry = consensus.propose(Command::new(
        CommandKind::Deterministic,
        b"create user 42".to_vec(),
    ))?;

    println!("decided slot {} with hash {:?}", entry.index, entry.hash);
    drop(consensus);
    fs::remove_dir_all(base)?;
    Ok(())
}
