use std::fs;

use rhiza_core::{EntryType, LogHash, StoredCommand};
use rhiza_quepaxa::{
    AcceptedValue, ConfigChange, Error, Membership, Proposal, ProposalPriority, RecordRequest,
    RecorderFileStore, RejectReason, SealFaultPoint,
};

const CLUSTER_ID: &str = "cluster-a";
const EPOCH: u64 = 2;
const CONFIG_ID: u64 = 4;

#[test]
fn lock_only_partial_initialization_retries_as_fresh() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join(".recorder.lock"), b"").unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();

    let recorder = store(dir.path(), membership.clone()).unwrap();

    assert_eq!(
        recorder.configuration_state().unwrap().membership(),
        Some(&membership)
    );
    assert!(dir.path().join("configuration.rec").exists());
    assert!(dir.path().join("recorded-head.rec").exists());
}

#[test]
fn normal_record_reopens_with_command_and_max_without_rewriting_configuration() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let recorder = store(dir.path(), membership.clone()).unwrap();
    let configuration_path = dir.path().join("configuration.rec");
    let configuration_before = fs::read(&configuration_path).unwrap();
    let command = StoredCommand::new(EntryType::Command, b"normal".to_vec());
    let value =
        AcceptedValue::from_command(CLUSTER_ID, 8, EPOCH, CONFIG_ID, LogHash::ZERO, &command);

    recorder
        .record_proposal(record(
            8,
            proposal("writer", 1, 1, value),
            Some(command.clone()),
        ))
        .unwrap();

    assert_eq!(fs::read(&configuration_path).unwrap(), configuration_before);
    assert_eq!(
        recorder.fetch_command(command.hash()).unwrap(),
        Some(command.clone())
    );
    drop(recorder);

    let reopened = store(dir.path(), membership).unwrap();
    assert_eq!(
        reopened
            .configuration_state()
            .unwrap()
            .max_accepted_or_decided_slot(),
        Some(8)
    );
    assert_eq!(
        reopened.fetch_command(command.hash()).unwrap(),
        Some(command)
    );

    let stop = ConfigChange::stop(
        CONFIG_ID,
        reopened.configuration_state().unwrap().config_digest(),
    )
    .to_stored_command();
    let stop_value =
        AcceptedValue::from_command(CLUSTER_ID, 7, EPOCH, CONFIG_ID, LogHash::ZERO, &stop);
    assert_eq!(
        reopened.record_proposal(record(7, proposal("stopper", 1, 1, stop_value), Some(stop),)),
        Err(Error::Rejected(RejectReason::InvalidTransition))
    );
}

#[test]
fn reopen_reads_only_the_durable_head_and_ignores_non_head_history() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let recorder = store(dir.path(), membership.clone()).unwrap();
    let command = StoredCommand::new(EntryType::Command, b"normal".to_vec());
    let value =
        AcceptedValue::from_command(CLUSTER_ID, 8, EPOCH, CONFIG_ID, LogHash::ZERO, &command);
    recorder
        .record_proposal(record(8, proposal("writer", 1, 1, value), Some(command)))
        .unwrap();
    drop(recorder);
    for slot in 0..1_000 {
        if slot != 8 {
            fs::write(
                dir.path().join(format!("slot-{slot:020}.rec")),
                b"corrupt non-head history",
            )
            .unwrap();
        }
    }

    let reopened = store(dir.path(), membership).unwrap();
    assert_eq!(
        reopened
            .configuration_state()
            .unwrap()
            .max_accepted_or_decided_slot(),
        Some(8)
    );
}

#[test]
fn reopen_repairs_a_corrupt_head_slot_cache_from_the_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let recorder = store(dir.path(), membership.clone()).unwrap();
    let command = StoredCommand::new(EntryType::Command, b"normal".to_vec());
    let value =
        AcceptedValue::from_command(CLUSTER_ID, 8, EPOCH, CONFIG_ID, LogHash::ZERO, &command);
    recorder
        .record_proposal(record(8, proposal("writer", 1, 1, value), Some(command)))
        .unwrap();
    drop(recorder);
    fs::write(dir.path().join("slot-00000000000000000008.rec"), b"corrupt").unwrap();

    let reopened = store(dir.path(), membership).unwrap();
    assert_eq!(
        reopened
            .configuration_state()
            .unwrap()
            .max_accepted_or_decided_slot(),
        Some(8)
    );
    assert!(reopened.load(8).is_ok());
}

#[test]
fn reopen_fails_closed_when_the_authoritative_manifest_is_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    drop(store(dir.path(), membership.clone()).unwrap());
    let manifest_path = dir.path().join("recorded-head.rec");
    let mut manifest = fs::read(&manifest_path).unwrap();
    manifest[10] ^= 1;
    fs::write(manifest_path, manifest).unwrap();

    assert!(matches!(
        store(dir.path(), membership),
        Err(Error::Decode(_))
    ));
}

#[test]
fn old_slot_format_without_a_durable_head_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("slot-00000000000000000008.rec"), b"legacy").unwrap();

    assert!(matches!(
        store(dir.path(), Membership::new(["r1", "r2", "r3"]).unwrap()),
        Err(Error::MigrationRequired {
            format: "recorder durable head",
            version: 2
        })
    ));
}

#[test]
fn configuration_without_initialization_intent_remains_a_legacy_format() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    drop(store(dir.path(), membership.clone()).unwrap());
    fs::remove_file(dir.path().join("recorded-head.rec")).unwrap();

    assert!(matches!(
        store(dir.path(), membership),
        Err(Error::MigrationRequired {
            format: "recorder durable head",
            version: 2
        })
    ));
}

#[test]
fn version_one_head_requires_the_breaking_manifest_upgrade() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    drop(store(dir.path(), membership.clone()).unwrap());
    fs::write(
        dir.path().join("recorded-head.rec"),
        [b'Q', b'R', b'H', b'D', 0, 1],
    )
    .unwrap();

    assert!(matches!(
        store(dir.path(), membership),
        Err(Error::MigrationRequired {
            format: "recorder durable head",
            version: 2
        })
    ));
}

#[test]
fn reopen_fails_closed_when_an_acknowledged_slot_loses_its_command() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let recorder = store(dir.path(), membership.clone()).unwrap();
    let command = StoredCommand::new(EntryType::Command, b"normal".to_vec());
    let value =
        AcceptedValue::from_command(CLUSTER_ID, 8, EPOCH, CONFIG_ID, LogHash::ZERO, &command);
    recorder
        .record_proposal(record(
            8,
            proposal("writer", 1, 1, value),
            Some(command.clone()),
        ))
        .unwrap();
    drop(recorder);
    fs::remove_file(
        dir.path()
            .join(format!("command-{}.cmd", command.hash().to_hex())),
    )
    .unwrap();

    assert!(matches!(
        store(dir.path(), membership),
        Err(Error::CommandUnavailable)
    ));
}

#[test]
fn interrupted_normal_record_rolls_slot_and_head_forward_together() {
    for fault in [
        SealFaultPoint::AfterRecordManifest,
        SealFaultPoint::AfterRecordCache,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
        let recorder = store(dir.path(), membership.clone()).unwrap();
        let command = StoredCommand::new(EntryType::Command, b"normal".to_vec());
        let value =
            AcceptedValue::from_command(CLUSTER_ID, 8, EPOCH, CONFIG_ID, LogHash::ZERO, &command);
        let proposal = proposal("writer", 1, 1, value);
        recorder.set_seal_fault(Some(fault)).unwrap();

        assert!(matches!(
            recorder.record_proposal(record(8, proposal.clone(), Some(command.clone()))),
            Err(Error::Io(message)) if message.contains(&format!("{fault:?}"))
        ));
        drop(recorder);

        let reopened = store(dir.path(), membership).unwrap();
        assert_eq!(
            reopened
                .configuration_state()
                .unwrap()
                .max_accepted_or_decided_slot(),
            Some(8)
        );
        assert_eq!(
            reopened.load(8).unwrap().isr().first_current(),
            Some(&proposal)
        );
        assert_eq!(
            reopened.fetch_command(command.hash()).unwrap(),
            Some(command)
        );
        assert!(!dir.path().join("slot-head.intent").exists());
    }
}

#[test]
fn interrupted_record_before_manifest_does_not_publish_a_promise() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let recorder = store(dir.path(), membership.clone()).unwrap();
    let command = StoredCommand::new(EntryType::Command, b"normal".to_vec());
    let value =
        AcceptedValue::from_command(CLUSTER_ID, 8, EPOCH, CONFIG_ID, LogHash::ZERO, &command);
    recorder
        .set_seal_fault(Some(SealFaultPoint::BeforeRecordManifest))
        .unwrap();

    assert!(matches!(
        recorder.record_proposal(record(
            8,
            proposal("writer", 1, 1, value),
            Some(command),
        )),
        Err(Error::Io(message)) if message.contains("BeforeRecordManifest")
    ));
    drop(recorder);

    let reopened = store(dir.path(), membership).unwrap();
    assert_eq!(
        reopened
            .configuration_state()
            .unwrap()
            .max_accepted_or_decided_slot(),
        None
    );
    assert_eq!(reopened.load(8).unwrap().isr().first_current(), None);
}

#[test]
fn manifest_recovers_the_two_most_recent_slot_caches() {
    let dir = tempfile::tempdir().unwrap();
    let membership = Membership::new(["r1", "r2", "r3"]).unwrap();
    let recorder = store(dir.path(), membership.clone()).unwrap();
    let mut proposals = Vec::new();

    for slot in [8, 9] {
        let command = StoredCommand::new(EntryType::Command, format!("slot-{slot}").into_bytes());
        let value = AcceptedValue::from_command(
            CLUSTER_ID,
            slot,
            EPOCH,
            CONFIG_ID,
            LogHash::ZERO,
            &command,
        );
        let proposal = proposal("writer", slot, 1, value);
        recorder
            .record_proposal(record(slot, proposal.clone(), Some(command)))
            .unwrap();
        proposals.push((slot, proposal));
    }
    drop(recorder);

    for (slot, _) in &proposals {
        fs::remove_file(dir.path().join(format!("slot-{slot:020}.rec"))).unwrap();
    }

    let reopened = store(dir.path(), membership).unwrap();
    for (slot, proposal) in proposals {
        assert_eq!(
            reopened.load(slot).unwrap().isr().first_current(),
            Some(&proposal)
        );
    }
    assert_eq!(
        reopened
            .configuration_state()
            .unwrap()
            .max_accepted_or_decided_slot(),
        Some(9)
    );
}

fn store(root: &std::path::Path, membership: Membership) -> Result<RecorderFileStore, Error> {
    RecorderFileStore::new_with_membership(root, "r1", CLUSTER_ID, EPOCH, CONFIG_ID, membership)
}

fn record(slot: u64, proposal: Proposal, command: Option<StoredCommand>) -> RecordRequest {
    RecordRequest {
        cluster_id: CLUSTER_ID.into(),
        epoch: EPOCH,
        config_id: CONFIG_ID,
        config_digest: Membership::new(["r1", "r2", "r3"]).unwrap().digest(),
        slot,
        step: 4,
        proposal,
        command,
    }
}

fn proposal(proposer_id: &str, proposal_id: u64, priority: u64, value: AcceptedValue) -> Proposal {
    Proposal::new(
        ProposalPriority::from_u64(priority),
        proposer_id,
        proposal_id,
        value,
    )
}
