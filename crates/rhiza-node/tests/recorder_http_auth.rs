use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use rhiza_core::{EntryType, LogHash, StoredCommand};
use rhiza_node::{recorder_router, HttpRecorderClient, PeerConfig};
use rhiza_quepaxa::{
    AcceptedValue, DecisionProof, Error, Membership, Proposal, ProposalPriority, RecordRequest,
    RecordSummary, RecorderRpc, RejectReason,
};

fn peers() -> Vec<PeerConfig> {
    (1..=3)
        .map(|index| {
            PeerConfig::new(
                format!("node-{index}"),
                format!("http://node-{index}:8081"),
                format!("peer-token-{index}"),
            )
            .unwrap()
        })
        .collect()
}

#[derive(Clone, Default)]
struct CountingRecorder {
    records: Arc<AtomicUsize>,
    proofs: Arc<AtomicUsize>,
}

impl RecorderRpc for CountingRecorder {
    fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
        Ok("node-1".into())
    }

    fn record(&self, request: RecordRequest) -> rhiza_quepaxa::Result<RecordSummary> {
        self.records.fetch_add(1, Ordering::Relaxed);
        Ok(RecordSummary {
            recorder_id: "node-1".into(),
            slot: request.slot,
            config_id: request.config_id,
            config_digest: request.config_digest,
            step: request.step,
            first_current: Some(request.proposal),
            aggregate_prior: None,
            decided: None,
        })
    }

    fn install_decision_proof(
        &self,
        _proof: DecisionProof,
        _membership: &Membership,
    ) -> rhiza_quepaxa::Result<()> {
        self.proofs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn record_request(proposer_id: &str, slot: u64) -> RecordRequest {
    let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let command = StoredCommand::new(EntryType::Command, format!("command-{slot}").into_bytes());
    RecordRequest {
        cluster_id: "rhiza:sql:cluster-a".into(),
        epoch: 1,
        config_id: 1,
        config_digest: membership.digest(),
        slot,
        step: 4,
        proposal: Proposal::new(
            ProposalPriority::MAX,
            proposer_id,
            slot,
            AcceptedValue::from_command("rhiza:sql:cluster-a", slot, 1, 1, LogHash::ZERO, &command),
        ),
        command: Some(command),
    }
}

fn decision_proof(proposer_id: &str, slot: u64) -> DecisionProof {
    let request = record_request(proposer_id, slot);
    DecisionProof::FastPath {
        cluster_id: request.cluster_id,
        slot: request.slot,
        epoch: request.epoch,
        config_id: request.config_id,
        config_digest: request.config_digest,
        proposal: request.proposal,
        summaries: Vec::new(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn http_recorder_accepts_member_relay_and_rejects_non_member_without_backend_call() {
    let recorder = CountingRecorder::default();
    let records = Arc::clone(&recorder.records);
    let proofs = Arc::clone(&recorder.proofs);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, recorder_router(recorder, peers()))
            .await
            .unwrap();
    });

    tokio::task::spawn_blocking(move || {
        let client =
            HttpRecorderClient::new(format!("http://{address}"), "node-2", "peer-token-2").unwrap();
        assert_eq!(client.record(record_request("node-1", 1)).unwrap().slot, 1);
        assert!(matches!(
            client.record(record_request("node-9", 2)),
            Err(Error::Rejected(RejectReason::InvalidRequest))
        ));
        let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
        client
            .install_decision_proof(decision_proof("node-1", 3), &membership)
            .unwrap();
        assert!(matches!(
            client.install_decision_proof(decision_proof("node-9", 4), &membership),
            Err(Error::Rejected(RejectReason::InvalidRequest))
        ));
        assert_eq!(client.recorder_id().unwrap(), "node-1");
    })
    .await
    .unwrap();

    assert_eq!(records.load(Ordering::Relaxed), 1);
    assert_eq!(proofs.load(Ordering::Relaxed), 1);
    server.abort();
}
