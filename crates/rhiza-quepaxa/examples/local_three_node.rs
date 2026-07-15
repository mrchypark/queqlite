use std::{
    fs,
    time::{Duration, SystemTime},
};

use rhiza_quepaxa::{Command, CommandKind, Consensus, ThreeNodeConsensus};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let suffix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_nanos();
    let base = std::env::temp_dir().join(format!("rhiza-quepaxa-{suffix}"));
    let roots = [base.join("n1"), base.join("n2"), base.join("n3")];

    let consensus = ThreeNodeConsensus::new("example", "n1", 1, 1, roots)?;
    let entry = consensus.propose(Command::new(
        CommandKind::Deterministic,
        b"create user 42".to_vec(),
    ))?;

    println!("decided slot {} with hash {:?}", entry.index, entry.hash);
    if !consensus.finish_pending_rpcs(Duration::from_secs(1)) {
        return Err("timed out waiting for pending QuePaxa RPCs".into());
    }
    drop(consensus);
    fs::remove_dir_all(base)?;
    Ok(())
}
