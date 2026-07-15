use proptest::prelude::*;
use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    thread,
    time::Duration,
};

use rhiza_core::{Command, CommandKind, EntryType, LogHash, StoredCommand};
use rhiza_quepaxa::{
    AcceptedValue, CertifiedDecisionInspection, ConfigChange, DecisionProof, DriveOutcome, Error,
    IsrState, Membership, Proposal, ProposalPriority, ProposerProgress, RecordRequest,
    RecordSummary, RecorderFileStore, RecorderReply, RecorderRequest, RecorderRpc, RecorderSummary,
    RejectReason, ThreeNodeConsensus,
};

fn value(byte: u8) -> AcceptedValue {
    let command = StoredCommand::new(EntryType::Command, vec![byte]);
    AcceptedValue::from_command("cluster", 7, 2, 3, LogHash::ZERO, &command)
}

fn proposal(priority: u64, proposer: &str, id: u64, byte: u8) -> Proposal {
    Proposal::new(
        ProposalPriority::from_u64(priority),
        proposer,
        id,
        value(byte),
    )
}

#[test]
fn proposal_order_binds_value_across_proposer_restart_id_collision() {
    let left = proposal(9, "p", 4, 1);
    let right = proposal(9, "p", 4, 2);
    assert_ne!(left, right);
    assert_ne!(left.cmp(&right), std::cmp::Ordering::Equal);
    assert_eq!(left.cmp(&right), right.cmp(&left).reverse());
    assert_eq!(Proposal::nil().cmp(&left), std::cmp::Ordering::Less);
}

#[test]
fn constant_space_isr_matches_algorithm_three() {
    let p1 = proposal(1, "a", 1, 1);
    let p2 = proposal(2, "b", 1, 2);
    let p3 = proposal(3, "c", 1, 3);

    let (s1, r1) = IsrState::default().record(4, p1.clone());
    assert_eq!(
        (r1.step, r1.first_current, r1.aggregate_prior),
        (4, Some(p1.clone()), None)
    );

    let (s2, r2) = s1.record(4, p2.clone());
    assert_eq!(r2.first_current, Some(p1));
    assert_eq!(s2.aggregate_current(), Some(&p2));

    let (s3, r3) = s2.record(5, p3.clone());
    assert_eq!(r3.aggregate_prior, Some(p2));
    assert_eq!(r3.first_current, Some(p3.clone()));

    let (skipped, reply) = s3.record(8, p3);
    assert_eq!(reply.step, 8);
    assert_eq!(reply.aggregate_prior, None);
    assert_eq!(skipped.step(), 8);
}

proptest! {
    #[test]
    fn isr_stale_duplicate_reorder_and_skip_invariants(
        start in 1_u64..100,
        priorities in prop::collection::vec(1_u64..10_000, 1..40),
        deltas in prop::collection::vec(0_u64..5, 1..40),
    ) {
        let mut state = IsrState::default();
        for (index, (&priority, &delta)) in priorities.iter().zip(&deltas).enumerate() {
            let requested = start.saturating_add(delta);
            let before = state.clone();
            let p = proposal(priority, "p", index as u64 + 1, index as u8);
            let (next, reply) = state.record(requested, p);
            prop_assert!(next.step() >= before.step());
            prop_assert_eq!(reply.step, next.step());
            if requested < before.step() {
                prop_assert_eq!(&next, &before);
            }
            if requested > before.step().saturating_add(1) {
                prop_assert_eq!(next.aggregate_prior(), None);
            }
            state = next;
        }
    }
}

fn consensus(root: &Path, proposer: &str) -> ThreeNodeConsensus {
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let recorders = membership
        .members()
        .iter()
        .map(|id| {
            let store = RecorderFileStore::new_with_membership(
                root.join(id),
                id.clone(),
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            (id.clone(), Box::new(store) as Box<dyn RecorderRpc>)
        })
        .collect();
    ThreeNodeConsensus::from_recorders_with_ids("cluster", proposer, 1, 1, recorders).unwrap()
}

#[test]
fn preferred_proposer_decides_in_step_four_with_fast_proof() {
    let root = tempfile::tempdir().unwrap();
    let consensus = consensus(root.path(), "n1");
    let command = StoredCommand::new(EntryType::Command, b"fast".to_vec());
    consensus.register_command(command.hash(), command.payload.clone());
    let outcome = consensus
        .drive(ProposerProgress::new(
            1,
            Proposal::new(
                ProposalPriority::MAX,
                "n1",
                1,
                AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command),
            ),
        ))
        .unwrap();
    assert!(matches!(
        outcome,
        DriveOutcome::Decision(DecisionProof::FastPath { .. })
    ));
    assert!(matches!(
        consensus
            .inspect_certified_decision_at(1, LogHash::ZERO)
            .unwrap(),
        CertifiedDecisionInspection::Committed(_)
    ));
    for id in ["n1", "n2", "n3"] {
        let store = RecorderFileStore::new_with_membership(
            root.path().join(id),
            id,
            "cluster",
            1,
            1,
            Membership::new(["n1", "n2", "n3"]).unwrap(),
        );
        assert!(store.is_err(), "open recorder ownership remains exclusive");
    }
}

#[test]
fn root_constructor_installs_membership_before_proof_installation() {
    let root = tempfile::tempdir().unwrap();
    let recorder_roots = ["n1", "n2", "n3"].map(|id| root.path().join(id));
    let consensus = ThreeNodeConsensus::new("cluster", "n1", 1, 1, recorder_roots).unwrap();

    let entry = consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, b"configured-roots".to_vec()),
        )
        .unwrap();

    assert_eq!(entry.payload, b"configured-roots");
}

#[derive(Debug)]
struct FixedPriority;

impl rhiza_quepaxa::PrioritySource for FixedPriority {
    fn sample(
        &self,
        _slot: u64,
        round: u64,
        proposer: &str,
        recorder: &str,
    ) -> Result<ProposalPriority, Error> {
        let seed = round + proposer.as_bytes()[0] as u64 + recorder.as_bytes()[1] as u64;
        Ok(ProposalPriority::from_u64(seed.max(1)))
    }
}

#[derive(Debug)]
struct FailingPriority;

impl rhiza_quepaxa::PrioritySource for FailingPriority {
    fn sample(
        &self,
        _slot: u64,
        _round: u64,
        _proposer: &str,
        _recorder: &str,
    ) -> Result<ProposalPriority, Error> {
        Err(Error::RandomnessUnavailable(
            "injected entropy failure".into(),
        ))
    }
}

#[test]
fn priority_randomness_failure_is_typed_and_fail_stop() {
    let root = tempfile::tempdir().unwrap();
    let consensus = consensus(root.path(), "n2").with_priority_source(Arc::new(FailingPriority));
    let command = StoredCommand::new(EntryType::Command, b"rng-failure".to_vec());
    consensus.register_command(command.hash(), command.payload.clone());
    let progress = ProposerProgress::new(
        1,
        Proposal::new(
            ProposalPriority::MAX,
            "n2",
            1,
            AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command),
        ),
    );

    assert!(matches!(
        consensus.drive(progress),
        Err(Error::RandomnessUnavailable(message)) if message.contains("entropy")
    ));
}

#[test]
fn non_preferred_proposer_uses_leaderless_four_phase_path() {
    let root = tempfile::tempdir().unwrap();
    let consensus = consensus(root.path(), "n2").with_priority_source(Arc::new(FixedPriority));
    let command = StoredCommand::new(EntryType::Command, b"slow".to_vec());
    consensus.register_command(command.hash(), command.payload.clone());
    let mut progress = ProposerProgress::new(
        1,
        Proposal::new(
            ProposalPriority::MAX,
            "n2",
            1,
            AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command),
        ),
    );
    let proof = loop {
        match consensus.drive(progress).unwrap() {
            DriveOutcome::Progress(next) | DriveOutcome::Pending(next) => progress = next,
            DriveOutcome::Decision(proof) => break proof,
        }
    };
    assert!(matches!(proof, DecisionProof::Phase2 { step, .. } if step % 4 == 2));
    assert!(matches!(
        consensus
            .inspect_certified_decision_at(1, LogHash::ZERO)
            .unwrap(),
        CertifiedDecisionInspection::Pending
    ));
    assert!(matches!(
        consensus.recover_decision_at(1, LogHash::ZERO).unwrap(),
        rhiza_quepaxa::DecisionInspection::Committed(entry) if entry.payload == b"slow"
    ));
}

#[test]
fn proof_validation_rejects_tampering_quorum_config_and_step() {
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let p = proposal(u64::MAX, "n1", 1, 1);
    let summaries = ["n1", "n2"]
        .into_iter()
        .map(|id| RecorderSummary {
            recorder_id: id.to_string(),
            slot: 7,
            step: 4,
            first_current: Some(Proposal::new(ProposalPriority::MAX, "n1", 1, value(1))),
            aggregate_prior: None,
        })
        .collect();
    let proof = DecisionProof::FastPath {
        cluster_id: "cluster".into(),
        slot: 7,
        epoch: 2,
        config_id: 3,
        config_digest: membership.digest(),
        proposal: Proposal::new(ProposalPriority::MAX, "n1", 1, value(1)),
        summaries,
    };
    proof.validate_for(7, 2, 3, &membership).unwrap();
    assert_eq!(
        proof.validate_for_cluster("other-cluster", 7, 2, 3, &membership),
        Err(RejectReason::WrongCluster)
    );

    let mut tampered = proof.clone();
    let DecisionProof::FastPath { summaries, .. } = &mut tampered else {
        unreachable!()
    };
    summaries[1].recorder_id = "n1".into();
    assert_eq!(
        tampered.validate_for(7, 2, 3, &membership),
        Err(RejectReason::InvalidCertificate)
    );
    assert_eq!(
        proof.validate_for(8, 2, 3, &membership),
        Err(RejectReason::MalformedDecision)
    );
    assert_eq!(
        proof.validate_for(7, 2, 4, &membership),
        Err(RejectReason::WrongConfig)
    );

    let DecisionProof::FastPath { summaries, .. } = proof.clone() else {
        unreachable!()
    };
    let phase2 = DecisionProof::Phase2 {
        cluster_id: "cluster".into(),
        slot: 7,
        epoch: 2,
        config_id: 3,
        config_digest: membership.digest(),
        step: 5,
        proposal: p,
        summaries,
    };
    assert_eq!(
        phase2.validate_for(7, 2, 3, &membership),
        Err(RejectReason::InvalidCertificate)
    );
}

#[derive(Clone)]
struct FailingRecord {
    store: RecorderFileStore,
    failures: Arc<AtomicUsize>,
}

impl RecorderRpc for FailingRecord {
    fn call(&self, request: RecorderRequest) -> Result<RecorderReply, Error> {
        self.store.call(request)
    }

    fn record(&self, request: RecordRequest) -> Result<RecordSummary, Error> {
        if self
            .failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok()
        {
            return Err(Error::Io("scripted deadline".into()));
        }
        self.store.record(request)
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> Result<(), Error> {
        self.store.install_decision_proof(proof, membership)
    }
}

#[test]
fn drive_has_no_fixed_retry_cap_and_eventually_decides() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let recorders = membership
        .members()
        .iter()
        .map(|id| {
            let store = RecorderFileStore::new_with_membership(
                root.path().join(id),
                id.clone(),
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            (
                id.clone(),
                Box::new(FailingRecord {
                    store,
                    failures: Arc::new(AtomicUsize::new(12)),
                }) as Box<dyn RecorderRpc>,
            )
        })
        .collect();
    let consensus =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
    let command = StoredCommand::new(EntryType::Command, b"eventual".to_vec());
    consensus.register_command(command.hash(), command.payload.clone());
    let value = AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command);
    let mut progress =
        ProposerProgress::new(1, Proposal::new(ProposalPriority::MAX, "n1", 1, value));
    let mut pending = 0;
    loop {
        match consensus.drive(progress).unwrap() {
            DriveOutcome::Pending(next) => {
                pending += 1;
                progress = next;
            }
            DriveOutcome::Progress(next) => progress = next,
            DriveOutcome::Decision(_) => break,
        }
        assert!(pending < 30);
    }
    assert!(pending > 8, "the run exceeded the removed legacy retry cap");
}

#[test]
fn three_interleaved_proposers_cooperate_on_one_value() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let stores: Vec<_> = membership
        .members()
        .iter()
        .map(|id| {
            RecorderFileStore::new_with_membership(
                root.path().join(id),
                id.clone(),
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap()
        })
        .collect();
    let engines: Vec<_> = ["n1", "n2", "n3"]
        .into_iter()
        .map(|proposer| {
            let recorders = membership
                .members()
                .iter()
                .zip(&stores)
                .map(|(id, store)| (id.clone(), Box::new(store.clone()) as Box<dyn RecorderRpc>))
                .collect();
            Arc::new(
                ThreeNodeConsensus::from_recorders_with_ids("cluster", proposer, 1, 1, recorders)
                    .unwrap(),
            )
        })
        .collect();
    let mut proposers: Vec<_> = engines
        .into_iter()
        .enumerate()
        .map(|(index, engine)| {
            let command =
                StoredCommand::new(EntryType::Command, format!("value-{index}").into_bytes());
            engine.register_command(command.hash(), command.payload.clone());
            let accepted = AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command);
            let proposer = format!("n{}", index + 1);
            (
                engine,
                Some(ProposerProgress::new(
                    1,
                    Proposal::new(ProposalPriority::MAX, proposer, 1, accepted),
                )),
                None,
            )
        })
        .collect();
    for _ in 0..1_000 {
        for (engine, progress, decision) in &mut proposers {
            let Some(current) = progress.take() else {
                continue;
            };
            match engine.drive(current).unwrap() {
                DriveOutcome::Pending(next) | DriveOutcome::Progress(next) => {
                    *progress = Some(next);
                }
                DriveOutcome::Decision(proof) => {
                    *decision = proof.proposal().value.clone();
                }
            }
        }
        if proposers.iter().all(|(_, _, decision)| decision.is_some()) {
            break;
        }
    }
    let values: Vec<_> = proposers
        .into_iter()
        .filter_map(|(_, _, decision)| decision)
        .collect();
    assert_eq!(values.len(), 3);
    assert!(values.windows(2).all(|pair| pair[0] == pair[1]));
}

#[test]
fn recorder_crash_reopen_reconstructs_decision_from_phase_state() {
    let root = tempfile::tempdir().unwrap();
    let engine = consensus(root.path(), "n1");
    let before = engine
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, b"reopen".to_vec()),
        )
        .unwrap();
    assert!(engine.inspect_decision_proof_at(1).unwrap().is_none());
    assert!(engine.finish_pending_rpcs(Duration::from_secs(1)));
    drop(engine);

    let reopened = consensus(root.path(), "n3").with_priority_source(Arc::new(FixedPriority));
    assert!(matches!(
        reopened
            .inspect_certified_decision_at(1, LogHash::ZERO)
            .unwrap(),
        CertifiedDecisionInspection::Committed(_)
    ));
    let after = reopened
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, b"different".to_vec()),
        )
        .unwrap();
    assert_eq!(before, after);
}

#[derive(Debug)]
struct LegacyOnlyRecorder {
    id: String,
    accepted: AcceptedValue,
}

impl RecorderRpc for LegacyOnlyRecorder {
    fn call(&self, request: RecorderRequest) -> Result<RecorderReply, Error> {
        let RecorderRequest::Inspect {
            slot,
            config_id,
            config_digest,
            ..
        } = request
        else {
            return Err(Error::Rejected(RejectReason::InvalidRequest));
        };
        Ok(RecorderReply {
            recorder_id: self.id.clone(),
            slot,
            config_id,
            config_digest,
            step: 1,
            highest_promised: Some(rhiza_quepaxa::Ballot::new(1, 1, "legacy")),
            accepted: Some(rhiza_quepaxa::AcceptedSummary {
                ballot: rhiza_quepaxa::Ballot::new(1, 1, "legacy"),
                value: self.accepted.clone(),
            }),
            decided: None,
            command: None,
        })
    }
}

#[test]
fn phase1_quorum_is_never_synthesized_into_a_committed_decision() {
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let command = StoredCommand::new(EntryType::Command, b"phase1-a".to_vec());
    let accepted = AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command);
    let recorders = membership
        .members()
        .iter()
        .map(|id| {
            (
                id.clone(),
                Box::new(LegacyOnlyRecorder {
                    id: id.clone(),
                    accepted: accepted.clone(),
                }) as Box<dyn RecorderRpc>,
            )
        })
        .collect();
    let consensus =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n2", 1, 1, recorders).unwrap();

    assert_eq!(
        consensus
            .inspect_certified_decision_at(1, LogHash::ZERO)
            .unwrap(),
        CertifiedDecisionInspection::Pending
    );
}

#[derive(Debug)]
struct CallOnlyRecorder;

impl RecorderRpc for CallOnlyRecorder {
    fn call(&self, _request: RecorderRequest) -> Result<RecorderReply, Error> {
        Err(Error::Rejected(RejectReason::InvalidRequest))
    }
}

struct StaleSummaryRecorder {
    id: String,
    summary: RecordSummary,
}

impl RecorderRpc for StaleSummaryRecorder {
    fn call(&self, _request: RecorderRequest) -> Result<RecorderReply, Error> {
        Err(Error::Rejected(RejectReason::InvalidRequest))
    }

    fn recorder_id(&self) -> Result<String, Error> {
        Ok(self.id.clone())
    }

    fn inspect_decision_proof(&self, _slot: u64) -> Result<Option<DecisionProof>, Error> {
        Ok(None)
    }

    fn inspect_record_summary(&self, _slot: u64) -> Result<Option<RecordSummary>, Error> {
        Ok(Some(self.summary.clone()))
    }

    fn uses_typed_protocol(&self) -> bool {
        true
    }
}

#[test]
fn recorder_without_typed_record_fails_closed() {
    let request = RecordRequest {
        cluster_id: "cluster".into(),
        epoch: 1,
        config_id: 1,
        config_digest: LogHash::ZERO,
        slot: 1,
        step: 4,
        proposal: proposal(1, "n1", 1, 1),
        command: None,
    };
    assert_eq!(
        CallOnlyRecorder.record(request),
        Err(Error::TypedRecordRequired)
    );
}

#[test]
fn typed_summary_inspection_rejects_stale_configuration_evidence() {
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let command = StoredCommand::new(EntryType::Command, b"stale".to_vec());
    let proposal = Proposal::new(
        ProposalPriority::MAX,
        "n1",
        1,
        AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command),
    );
    let recorders = membership
        .members()
        .iter()
        .map(|id| {
            (
                id.clone(),
                Box::new(StaleSummaryRecorder {
                    id: id.clone(),
                    summary: RecordSummary {
                        recorder_id: id.clone(),
                        slot: 1,
                        config_id: 2,
                        config_digest: LogHash::ZERO,
                        step: 4,
                        first_current: Some(proposal.clone()),
                        aggregate_prior: None,
                        decided: None,
                    },
                }) as Box<dyn RecorderRpc>,
            )
        })
        .collect();
    let consensus =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

    assert!(matches!(
        consensus
            .inspect_certified_decision_at(1, LogHash::ZERO)
            .unwrap(),
        CertifiedDecisionInspection::Unavailable
    ));
}

#[test]
fn hedged_proposer_finishes_another_proposers_exact_h_quorum() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let stores: Vec<_> = membership
        .members()
        .iter()
        .map(|id| {
            RecorderFileStore::new_with_membership(
                root.path().join(id),
                id.clone(),
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap()
        })
        .collect();
    let n1_command = StoredCommand::new(EntryType::Command, b"n1".to_vec());
    let n2_command = StoredCommand::new(EntryType::Command, b"n2".to_vec());
    let n1_value = AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &n1_command);
    let n2_value = AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &n2_command);
    for store in &stores {
        store
            .store_command(n1_command.hash(), n1_command.clone())
            .unwrap();
        store
            .store_command(n2_command.hash(), n2_command.clone())
            .unwrap();
        store
            .record(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 1,
                step: 4,
                proposal: Proposal::new(ProposalPriority::MAX, "n1", 1, n1_value.clone()),
                command: None,
            })
            .unwrap();
    }
    let recorders = membership
        .members()
        .iter()
        .zip(&stores)
        .map(|(id, store)| (id.clone(), Box::new(store.clone()) as Box<dyn RecorderRpc>))
        .collect();
    let n2 = ThreeNodeConsensus::from_recorders_with_ids("cluster", "n2", 1, 1, recorders).unwrap();

    let outcome = n2
        .drive(ProposerProgress::new(
            1,
            Proposal::new(ProposalPriority::MAX, "n2", 1, n2_value),
        ))
        .unwrap();
    let DriveOutcome::Decision(proof) = outcome else {
        panic!("hedged proposer did not finish the observed H proof");
    };
    assert_eq!(proof.proposal().proposer_id, "n1");
    assert_eq!(proof.proposal().value.as_ref(), Some(&n1_value));
}

#[test]
fn hedged_proposer_installs_an_adopted_config_change_on_a_quorum() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let config_command = ConfigChange::bound_stop(
        "cluster",
        1,
        membership.digest(),
        2,
        membership.members().to_vec(),
    )
    .unwrap()
    .to_stored_command();
    let config_value =
        AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &config_command);
    let stores: Vec<_> = membership
        .members()
        .iter()
        .map(|id| {
            RecorderFileStore::new_with_membership(
                root.path().join(id),
                id.clone(),
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap()
        })
        .collect();
    for store in &stores {
        store
            .record(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 1,
                step: 4,
                proposal: Proposal::new(ProposalPriority::MAX, "n1", 1, config_value.clone()),
                command: Some(config_command.clone()),
            })
            .unwrap();
    }
    let recorders = membership
        .members()
        .iter()
        .zip(&stores)
        .map(|(id, store)| (id.clone(), Box::new(store.clone()) as Box<dyn RecorderRpc>))
        .collect();
    let n2 = ThreeNodeConsensus::from_recorders_with_ids("cluster", "n2", 1, 1, recorders).unwrap();
    let local = StoredCommand::new(EntryType::Command, b"ordinary".to_vec());
    n2.register_command(local.hash(), local.payload.clone());

    let outcome = n2
        .drive(ProposerProgress::new(
            1,
            Proposal::new(
                ProposalPriority::MAX,
                "n2",
                1,
                AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &local),
            ),
        ))
        .unwrap();

    assert!(matches!(outcome, DriveOutcome::Decision(_)));
    assert!(
        stores
            .iter()
            .filter(|store| store.configuration_state().unwrap().seal().is_some())
            .count()
            >= membership.quorum_size()
    );
}

#[test]
fn production_store_rejects_arbitrary_legacy_decide() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let store = RecorderFileStore::new_with_membership(
        root.path().join("n1"),
        "n1",
        "cluster",
        1,
        1,
        membership.clone(),
    )
    .unwrap();
    let command = StoredCommand::new(EntryType::Command, b"arbitrary".to_vec());
    let value = AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command);
    store.store_command(command.hash(), command).unwrap();
    let decision = rhiza_quepaxa::DecisionCertificate {
        slot: 1,
        epoch: 1,
        config_id: 1,
        config_digest: membership.digest(),
        ballot: rhiza_quepaxa::Ballot::new(1, 1, "attacker"),
        value,
        recorder_ids: vec!["n1".into(), "n2".into()],
    };
    assert_eq!(
        store.apply(RecorderRequest::Decide {
            cluster_id: "cluster".into(),
            epoch: 1,
            config_id: 1,
            config_digest: membership.digest(),
            slot: 1,
            decision,
        }),
        Err(Error::Rejected(RejectReason::InvalidRequest))
    );
}

#[test]
fn old_qrec_state_fails_closed() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("slot-00000000000000000001.rec");
    let mut bytes = b"QREC".to_vec();
    bytes.extend_from_slice(&3_u16.to_be_bytes());
    let digest = LogHash::digest(&[&bytes]);
    bytes.extend_from_slice(digest.as_bytes());
    std::fs::write(path, bytes).unwrap();
    let store = RecorderFileStore::new_with_id(root.path(), "n1", "cluster", 1, 1).unwrap();
    assert_eq!(
        store.load(1),
        Err(Error::MigrationRequired {
            format: "QREC",
            version: 3,
        })
    );
}

#[derive(Default)]
struct ProtocolCounts {
    legacy_stores: AtomicUsize,
    fetches: AtomicUsize,
    piggybacks: AtomicUsize,
    proof_installs: AtomicUsize,
}

#[derive(Clone)]
struct ObservedRecorder {
    store: RecorderFileStore,
    counts: Arc<ProtocolCounts>,
}

impl RecorderRpc for ObservedRecorder {
    fn call(&self, request: RecorderRequest) -> Result<RecorderReply, Error> {
        match request {
            RecorderRequest::StoreCommand { .. } => {
                self.counts.legacy_stores.fetch_add(1, Ordering::SeqCst);
            }
            RecorderRequest::FetchCommand { .. } => {
                self.counts.fetches.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
        self.store.call(request)
    }

    fn record(&self, request: RecordRequest) -> Result<RecordSummary, Error> {
        if request.command.is_some() {
            self.counts.piggybacks.fetch_add(1, Ordering::SeqCst);
        }
        self.store.record(request)
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> Result<(), Error> {
        self.store.install_decision_proof(proof, membership)?;
        self.counts.proof_installs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn inspect_decision_proof(&self, slot: u64) -> Result<Option<DecisionProof>, Error> {
        self.store.inspect_decision_proof(slot)
    }

    fn uses_typed_protocol(&self) -> bool {
        true
    }
}

struct DeadRecorder;

impl RecorderRpc for DeadRecorder {
    fn call(&self, _request: RecorderRequest) -> Result<RecorderReply, Error> {
        Err(Error::Io("recorder unavailable".into()))
    }

    fn record(&self, _request: RecordRequest) -> Result<RecordSummary, Error> {
        Err(Error::Io("recorder unavailable".into()))
    }

    fn install_decision_proof(
        &self,
        _proof: DecisionProof,
        _membership: &Membership,
    ) -> Result<(), Error> {
        Err(Error::Io("recorder unavailable".into()))
    }
}

#[derive(Clone)]
struct RejectingRecordRecorder {
    rejected: Option<Arc<(Mutex<bool>, Condvar)>>,
    reason: RejectReason,
}

impl RecorderRpc for RejectingRecordRecorder {
    fn call(&self, _request: RecorderRequest) -> Result<RecorderReply, Error> {
        Err(Error::Io("recorder unavailable".into()))
    }

    fn record(&self, _request: RecordRequest) -> Result<RecordSummary, Error> {
        if let Some(rejected) = &self.rejected {
            let (state, condition) = &**rejected;
            *state.lock().unwrap() = true;
            condition.notify_all();
        }
        Err(Error::Rejected(self.reason.clone()))
    }

    fn install_decision_proof(
        &self,
        _proof: DecisionProof,
        _membership: &Membership,
    ) -> Result<(), Error> {
        Err(Error::Io("recorder unavailable".into()))
    }

    fn uses_typed_protocol(&self) -> bool {
        true
    }
}

#[derive(Clone)]
struct WaitForRejectionRecorder {
    store: RecorderFileStore,
    rejected: Arc<(Mutex<bool>, Condvar)>,
}

impl RecorderRpc for WaitForRejectionRecorder {
    fn call(&self, request: RecorderRequest) -> Result<RecorderReply, Error> {
        self.store.call(request)
    }

    fn record(&self, request: RecordRequest) -> Result<RecordSummary, Error> {
        let (state, condition) = &*self.rejected;
        let rejected = condition
            .wait_while(state.lock().unwrap(), |rejected| !*rejected)
            .unwrap();
        drop(rejected);
        self.store.record(request)
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> Result<(), Error> {
        self.store.install_decision_proof(proof, membership)
    }

    fn uses_typed_protocol(&self) -> bool {
        true
    }
}

#[derive(Clone)]
struct RecordThroughStepThenDead {
    store: RecorderFileStore,
    last_live_step: u64,
    crashed: Arc<AtomicBool>,
}

impl RecorderRpc for RecordThroughStepThenDead {
    fn call(&self, request: RecorderRequest) -> Result<RecorderReply, Error> {
        if self.crashed.load(Ordering::Acquire) {
            Err(Error::Io("recorder crashed".into()))
        } else {
            self.store.call(request)
        }
    }

    fn record(&self, request: RecordRequest) -> Result<RecordSummary, Error> {
        if request.step > self.last_live_step {
            self.crashed.store(true, Ordering::Release);
            Err(Error::Io("recorder crashed".into()))
        } else {
            self.store.record(request)
        }
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> Result<(), Error> {
        if self.crashed.load(Ordering::Acquire) {
            Err(Error::Io("recorder crashed".into()))
        } else {
            self.store.install_decision_proof(proof, membership)
        }
    }

    fn uses_typed_protocol(&self) -> bool {
        true
    }
}

#[derive(Clone)]
struct UnavailableBeforeStep {
    store: RecorderFileStore,
    first_live_step: u64,
}

impl RecorderRpc for UnavailableBeforeStep {
    fn call(&self, request: RecorderRequest) -> Result<RecorderReply, Error> {
        self.store.call(request)
    }

    fn record(&self, request: RecordRequest) -> Result<RecordSummary, Error> {
        if request.step < self.first_live_step {
            return Err(Error::Io("recorder unavailable".into()));
        }
        if request.command.is_none() {
            return Err(Error::Rejected(RejectReason::InvalidRequest));
        }
        self.store.record(request)
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> Result<(), Error> {
        self.store.install_decision_proof(proof, membership)
    }

    fn uses_typed_protocol(&self) -> bool {
        true
    }
}

struct BlockingRecordRecorder {
    started: Arc<(Mutex<bool>, Condvar)>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

impl RecorderRpc for BlockingRecordRecorder {
    fn call(&self, _request: RecorderRequest) -> Result<RecorderReply, Error> {
        Err(Error::Io("recorder unavailable".into()))
    }

    fn record(&self, _request: RecordRequest) -> Result<RecordSummary, Error> {
        let (started, started_condition) = &*self.started;
        *started.lock().unwrap() = true;
        started_condition.notify_all();

        let (release, release_condition) = &*self.release;
        let mut released = release.lock().unwrap();
        while !*released {
            released = release_condition.wait(released).unwrap();
        }
        Err(Error::Io("recorder unavailable".into()))
    }

    fn install_decision_proof(
        &self,
        _proof: DecisionProof,
        _membership: &Membership,
    ) -> Result<(), Error> {
        Err(Error::Io("recorder unavailable".into()))
    }

    fn uses_typed_protocol(&self) -> bool {
        true
    }
}

struct CountingProofRecorder {
    proof_installs: Arc<AtomicUsize>,
}

impl RecorderRpc for CountingProofRecorder {
    fn call(&self, _request: RecorderRequest) -> Result<RecorderReply, Error> {
        Err(Error::Io("recorder unavailable".into()))
    }

    fn record(&self, _request: RecordRequest) -> Result<RecordSummary, Error> {
        Err(Error::Io("recorder unavailable".into()))
    }

    fn install_decision_proof(
        &self,
        _proof: DecisionProof,
        _membership: &Membership,
    ) -> Result<(), Error> {
        self.proof_installs.fetch_add(1, Ordering::SeqCst);
        Err(Error::Io("recorder unavailable".into()))
    }
}

#[test]
fn preferred_fast_path_piggybacks_command_without_store_or_fetch_rpcs() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let counts = Arc::new(ProtocolCounts::default());
    let stores: Vec<_> = ["n1", "n2"]
        .into_iter()
        .map(|id| {
            RecorderFileStore::new_with_membership(
                root.path().join(id),
                id,
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap()
        })
        .collect();
    let recorders = vec![
        (
            "n1".into(),
            Box::new(ObservedRecorder {
                store: stores[0].clone(),
                counts: Arc::clone(&counts),
            }) as Box<dyn RecorderRpc>,
        ),
        (
            "n2".into(),
            Box::new(ObservedRecorder {
                store: stores[1].clone(),
                counts: Arc::clone(&counts),
            }) as Box<dyn RecorderRpc>,
        ),
        ("n3".into(), Box::new(DeadRecorder) as Box<dyn RecorderRpc>),
    ];
    let consensus =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
    let command = StoredCommand::new(EntryType::Command, b"one-round-trip".to_vec());

    let entry = consensus
        .propose_stored_at(1, LogHash::ZERO, command.clone())
        .unwrap();
    drop(consensus);

    assert_eq!(entry.payload, command.payload);
    assert_eq!(counts.legacy_stores.load(Ordering::SeqCst), 0);
    assert_eq!(counts.fetches.load(Ordering::SeqCst), 0);
    assert!(counts.piggybacks.load(Ordering::SeqCst) >= membership.quorum_size());
    assert_eq!(counts.proof_installs.load(Ordering::SeqCst), 0);
    for store in stores {
        assert_eq!(
            store.fetch_command(command.hash()).unwrap(),
            Some(command.clone())
        );
    }
}

#[test]
fn non_preferred_path_piggybacks_command_without_a_separate_store_round() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let counts = Arc::new(ProtocolCounts::default());
    let recorders = membership
        .members()
        .iter()
        .map(|id| {
            let store = RecorderFileStore::new_with_membership(
                root.path().join(id),
                id.clone(),
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            (
                id.clone(),
                Box::new(ObservedRecorder {
                    store,
                    counts: Arc::clone(&counts),
                }) as Box<dyn RecorderRpc>,
            )
        })
        .collect();
    let consensus =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n2", 1, 1, recorders).unwrap();

    consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, b"slow-path".to_vec()),
        )
        .unwrap();
    drop(consensus);

    assert_eq!(counts.legacy_stores.load(Ordering::SeqCst), 0);
    assert_eq!(counts.fetches.load(Ordering::SeqCst), 0);
    assert!(counts.piggybacks.load(Ordering::SeqCst) >= membership.quorum_size());
    assert!(counts.piggybacks.load(Ordering::SeqCst) <= 6);
}

#[test]
fn adopted_command_is_redistributed_after_a_holder_crashes_in_a_later_phase() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let adopted = StoredCommand::new(EntryType::Command, b"adopted".to_vec());
    let adopted_proposal = Proposal::new(
        ProposalPriority::from_u64(10_000),
        "other",
        1,
        AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &adopted),
    );
    let stores: Vec<_> = membership
        .members()
        .iter()
        .map(|id| {
            RecorderFileStore::new_with_membership(
                root.path().join(id),
                id.clone(),
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap()
        })
        .collect();
    for store in &stores[..2] {
        store
            .record(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 1,
                step: 4,
                proposal: adopted_proposal.clone(),
                command: Some(adopted.clone()),
            })
            .unwrap();
    }
    let crashed = Arc::new(AtomicBool::new(false));
    let recorders = vec![
        (
            "n1".into(),
            Box::new(RecordThroughStepThenDead {
                store: stores[0].clone(),
                last_live_step: 5,
                crashed: Arc::clone(&crashed),
            }) as Box<dyn RecorderRpc>,
        ),
        (
            "n2".into(),
            Box::new(stores[1].clone()) as Box<dyn RecorderRpc>,
        ),
        (
            "n3".into(),
            Box::new(UnavailableBeforeStep {
                store: stores[2].clone(),
                first_live_step: 6,
            }) as Box<dyn RecorderRpc>,
        ),
    ];
    let consensus = ThreeNodeConsensus::from_recorders_with_ids("cluster", "n2", 1, 1, recorders)
        .unwrap()
        .with_priority_source(Arc::new(FixedPriority));

    let entry = consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, b"offered".to_vec()),
        )
        .unwrap();

    assert_eq!(entry.payload, adopted.payload);
    assert!(crashed.load(Ordering::Acquire));
    assert_eq!(
        stores[2].fetch_command(adopted.hash()).unwrap(),
        Some(adopted)
    );
}

#[test]
fn record_broadcast_ignores_a_minority_typed_rejection() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let rejected = Arc::new((Mutex::new(false), Condvar::new()));
    let mut recorders = Vec::new();
    for id in ["n1", "n2"] {
        let store = RecorderFileStore::new_with_membership(
            root.path().join(id),
            id,
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        recorders.push((
            id.to_string(),
            Box::new(WaitForRejectionRecorder {
                store,
                rejected: Arc::clone(&rejected),
            }) as Box<dyn RecorderRpc>,
        ));
    }
    recorders.push((
        "n3".into(),
        Box::new(RejectingRecordRecorder {
            rejected: Some(rejected),
            reason: RejectReason::WrongSlot,
        }) as Box<dyn RecorderRpc>,
    ));
    let consensus =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

    let entry = consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, b"minority-rejection".to_vec()),
        )
        .unwrap();

    assert_eq!(entry.payload, b"minority-rejection");
}

#[test]
fn record_broadcast_returns_typed_rejection_when_quorum_is_impossible() {
    let recorders = vec![
        (
            "n1".into(),
            Box::new(RejectingRecordRecorder {
                rejected: None,
                reason: RejectReason::WrongSlot,
            }) as Box<dyn RecorderRpc>,
        ),
        ("n2".into(), Box::new(DeadRecorder) as Box<dyn RecorderRpc>),
        ("n3".into(), Box::new(DeadRecorder) as Box<dyn RecorderRpc>),
    ];
    let consensus =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

    assert_eq!(
        consensus.propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, b"rejected".to_vec()),
        ),
        Err(Error::Rejected(RejectReason::WrongSlot))
    );
}

#[test]
fn consensus_drop_does_not_wait_for_a_blocked_minority_rpc() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let started = Arc::new((Mutex::new(false), Condvar::new()));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let mut recorders = Vec::new();
    for id in ["n1", "n2"] {
        let store = RecorderFileStore::new_with_membership(
            root.path().join(id),
            id,
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        recorders.push((id.to_string(), Box::new(store) as Box<dyn RecorderRpc>));
    }
    recorders.push((
        "n3".into(),
        Box::new(BlockingRecordRecorder {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        }) as Box<dyn RecorderRpc>,
    ));
    let consensus =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
    consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, b"background-record".to_vec()),
        )
        .unwrap();

    let (started_lock, started_condition) = &*started;
    let (record_started, _) = started_condition
        .wait_timeout_while(
            started_lock.lock().unwrap(),
            Duration::from_secs(1),
            |started| !*started,
        )
        .unwrap();
    assert!(*record_started);

    let (drop_started_sender, drop_started_receiver) = std::sync::mpsc::channel();
    let (dropped_sender, dropped_receiver) = std::sync::mpsc::channel();
    let drop_thread = thread::spawn(move || {
        drop_started_sender.send(()).unwrap();
        drop(consensus);
        dropped_sender.send(()).unwrap();
    });
    drop_started_receiver.recv().unwrap();
    dropped_receiver
        .recv_timeout(Duration::from_secs(1))
        .unwrap();

    let (release_lock, release_condition) = &*release;
    *release_lock.lock().unwrap() = true;
    release_condition.notify_all();

    drop_thread.join().unwrap();
}

#[test]
fn proof_install_rejects_membership_other_than_the_recorder_configuration() {
    let root = tempfile::tempdir().unwrap();
    let current = Membership::new(["n1", "n2", "n3"]).unwrap();
    let supplied = Membership::new(["n1", "n2", "n4"]).unwrap();
    let store = RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, current)
        .unwrap();
    let command = StoredCommand::new(EntryType::Command, b"membership-proof".to_vec());
    store
        .store_command(command.hash(), command.clone())
        .unwrap();
    let proposal = Proposal::new(
        ProposalPriority::MAX,
        "n1",
        1,
        AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command),
    );
    let proof = DecisionProof::FastPath {
        cluster_id: "cluster".into(),
        slot: 1,
        epoch: 1,
        config_id: 1,
        config_digest: supplied.digest(),
        proposal: proposal.clone(),
        summaries: supplied.members()[..supplied.quorum_size()]
            .iter()
            .map(|id| RecorderSummary {
                recorder_id: id.clone(),
                slot: 1,
                step: 4,
                first_current: Some(proposal.clone()),
                aggregate_prior: None,
            })
            .collect(),
    };

    assert_eq!(
        store.install_decision_proof_record(proof, &supplied),
        Err(Error::Rejected(RejectReason::WrongConfig))
    );
    assert!(store.load(1).unwrap().decision_proof().is_none());
}

#[test]
fn proof_cache_accepts_different_metadata_for_the_same_decided_value() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let store = RecorderFileStore::new_with_membership(
        root.path(),
        "n1",
        "cluster",
        1,
        1,
        membership.clone(),
    )
    .unwrap();
    let command = StoredCommand::new(EntryType::Command, b"same-value".to_vec());
    store
        .store_command(command.hash(), command.clone())
        .unwrap();
    let value = AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command);
    let proof = |proposer: &str, proposal_id: u64| {
        let proposal = Proposal::new(ProposalPriority::MAX, proposer, proposal_id, value.clone());
        DecisionProof::FastPath {
            cluster_id: "cluster".into(),
            slot: 1,
            epoch: 1,
            config_id: 1,
            config_digest: membership.digest(),
            proposal: proposal.clone(),
            summaries: ["n1", "n2"]
                .into_iter()
                .map(|id| RecorderSummary {
                    recorder_id: id.into(),
                    slot: 1,
                    step: 4,
                    first_current: Some(proposal.clone()),
                    aggregate_prior: None,
                })
                .collect(),
        }
    };

    store
        .install_decision_proof_record(proof("n1", 1), &membership)
        .unwrap();
    store
        .install_decision_proof_record(proof("n2", 2), &membership)
        .unwrap();
}

#[test]
fn ordinary_fast_path_does_not_install_proof_cache_on_recorders() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let counts = Arc::new(ProtocolCounts::default());
    let minority_proof_installs = Arc::new(AtomicUsize::new(0));
    let recorders = ["n1", "n2"]
        .into_iter()
        .map(|id| {
            let store = RecorderFileStore::new_with_membership(
                root.path().join(id),
                id,
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            (
                id.to_string(),
                Box::new(ObservedRecorder {
                    store,
                    counts: Arc::clone(&counts),
                }) as Box<dyn RecorderRpc>,
            )
        })
        .chain(std::iter::once((
            "n3".into(),
            Box::new(CountingProofRecorder {
                proof_installs: Arc::clone(&minority_proof_installs),
            }) as Box<dyn RecorderRpc>,
        )))
        .collect();
    let consensus =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

    consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, b"fast".to_vec()),
        )
        .unwrap();
    drop(consensus);

    assert_eq!(counts.proof_installs.load(Ordering::SeqCst), 0);
    assert_eq!(minority_proof_installs.load(Ordering::SeqCst), 0);
}

thread_local! {
    static PIGGYBACK_PROPERTY_ROOT: tempfile::TempDir = tempfile::tempdir().unwrap();
}

static PIGGYBACK_PROPERTY_CASE: AtomicUsize = AtomicUsize::new(0);

proptest! {
    #[test]
    fn mismatched_piggyback_never_persists_or_advances(
        offered in prop::collection::vec(any::<u8>(), 1..64),
        other in prop::collection::vec(any::<u8>(), 1..64),
    ) {
        prop_assume!(offered != other);
        let case = PIGGYBACK_PROPERTY_CASE.fetch_add(1, Ordering::Relaxed);
        let root = PIGGYBACK_PROPERTY_ROOT.with(|root| root.path().join(case.to_string()));
        std::fs::create_dir(&root).unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let store = RecorderFileStore::new_with_membership(
            root, "n1", "cluster", 1, 1, membership.clone(),
        ).unwrap();
        let expected = StoredCommand::new(EntryType::Command, offered);
        let mismatched = StoredCommand::new(EntryType::Command, other);
        let request = RecordRequest {
            cluster_id: "cluster".into(),
            epoch: 1,
            config_id: 1,
            config_digest: membership.digest(),
            slot: 1,
            step: 4,
            proposal: Proposal::new(
                ProposalPriority::MAX,
                "n1",
                1,
                AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &expected),
            ),
            command: Some(mismatched.clone()),
        };

        prop_assert_eq!(
            store.record(request),
            Err(Error::Rejected(RejectReason::InvalidValue)),
        );
        prop_assert_eq!(store.fetch_command(mismatched.hash()).unwrap(), None);
        let state = store.load(1).unwrap();
        prop_assert_eq!(state.isr(), &IsrState::default());
    }
}

#[test]
fn proposer_failure_after_piggyback_recovers_with_restarted_quorum_and_one_dead_recorder() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let command = StoredCommand::new(EntryType::Command, b"recover-after-proposer-crash".to_vec());
    let value = AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command);
    let proposal = Proposal::new(ProposalPriority::MAX, "n1", 1, value.clone());

    for id in ["n1", "n2"] {
        let store = RecorderFileStore::new_with_membership(
            root.path().join(id),
            id,
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        store
            .record(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 1,
                step: 4,
                proposal: proposal.clone(),
                command: Some(command.clone()),
            })
            .unwrap();
    }

    let recorders = ["n1", "n2"]
        .into_iter()
        .map(|id| {
            let store = RecorderFileStore::new_with_membership(
                root.path().join(id),
                id,
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            (id.to_string(), Box::new(store) as Box<dyn RecorderRpc>)
        })
        .chain(std::iter::once((
            "n3".into(),
            Box::new(DeadRecorder) as Box<dyn RecorderRpc>,
        )))
        .collect();
    let replacement =
        ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

    let outcome = replacement
        .drive(ProposerProgress::new(1, proposal))
        .unwrap();
    assert!(matches!(
        outcome,
        DriveOutcome::Decision(DecisionProof::FastPath { .. })
    ));
}
