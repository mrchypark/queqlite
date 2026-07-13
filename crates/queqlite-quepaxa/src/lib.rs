use std::{
    cmp::Ordering as CmpOrdering,
    collections::{BTreeMap, BTreeSet},
    fmt, fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
};

use queqlite_core::{
    canonical_membership_digest, ClusterId, Command, CommandKind, ConfigId, EntryType, Epoch,
    LogEntry, LogHash, LogIndex, NodeId, StoredCommand,
};

pub use queqlite_core::ConfigChange;

pub type Result<T> = std::result::Result<T, Error>;
pub type Slot = u64;
pub type Round = u64;
pub type Phase = u8;
pub type Step = u64;
pub type Priority = u128;

const RECORDER_STATE_VERSION: u16 = 4;
const CONFIGURATION_STATE_VERSION: u16 = 3;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    ChainConflict {
        slot: Slot,
        expected_prev_hash: LogHash,
        actual_prev_hash: LogHash,
    },
    CommandHashMismatch,
    CommandUnavailable,
    Cancelled,
    ConflictingCertificates,
    /// Legacy source-compatibility error; the active engine returns Pending.
    ContentionExhausted {
        attempts: usize,
        highest_promised: Option<Ballot>,
    },
    Decode(String),
    DuplicateRecorderIdentity,
    EmptyRecorderIdentity,
    EmptyFixedMembership,
    InvalidFixedMembershipSize,
    InvalidRecoveredTip,
    Io(String),
    MigrationRequired {
        format: &'static str,
        version: u16,
    },
    NoQuorum,
    ProposeFailed,
    RandomnessUnavailable(String),
    RecorderRootLocked(PathBuf),
    Rejected(RejectReason),
    TypedProofInstallRequired,
    TypedRecordRequired,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ChainConflict { slot, .. } => {
                write!(f, "QuePaxa predecessor conflicts at slot {slot}")
            }
            Self::CommandHashMismatch => write!(f, "QuePaxa command hash mismatch"),
            Self::CommandUnavailable => write!(f, "QuePaxa command bytes unavailable"),
            Self::Cancelled => write!(f, "QuePaxa proposal cancelled"),
            Self::ConflictingCertificates => {
                write!(f, "QuePaxa recovered conflicting decision certificates")
            }
            Self::ContentionExhausted { attempts, .. } => {
                write!(
                    f,
                    "QuePaxa contention retries exhausted after {attempts} attempts"
                )
            }
            Self::Decode(message) => write!(f, "QuePaxa decode failed: {message}"),
            Self::DuplicateRecorderIdentity => write!(f, "recorder identities must be unique"),
            Self::EmptyRecorderIdentity => write!(f, "recorder identity must not be empty"),
            Self::EmptyFixedMembership => {
                write!(f, "fixed membership must include at least one node")
            }
            Self::InvalidFixedMembershipSize => {
                write!(f, "membership requires between three and seven recorders")
            }
            Self::InvalidRecoveredTip => write!(f, "recovered qlog next index must be positive"),
            Self::Io(message) => write!(f, "QuePaxa io failed: {message}"),
            Self::MigrationRequired { format, version } => {
                write!(f, "QuePaxa {format} version {version} requires migration")
            }
            Self::NoQuorum => write!(f, "QuePaxa quorum was not reached"),
            Self::ProposeFailed => write!(f, "QuePaxa propose failed"),
            Self::RandomnessUnavailable(message) => {
                write!(f, "QuePaxa OS randomness unavailable: {message}")
            }
            Self::RecorderRootLocked(root) => {
                write!(f, "recorder root is already owned: {}", root.display())
            }
            Self::Rejected(reason) => write!(f, "QuePaxa recorder rejected request: {reason:?}"),
            Self::TypedProofInstallRequired => {
                write!(
                    f,
                    "recorder does not implement typed decision-proof installation"
                )
            }
            Self::TypedRecordRequired => {
                write!(f, "recorder does not implement the typed Record operation")
            }
        }
    }
}

impl std::error::Error for Error {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Membership {
    members: Vec<NodeId>,
    digest: LogHash,
}

impl Membership {
    pub fn new<const N: usize>(members: [&str; N]) -> Result<Self> {
        Self::from_voters(members.into_iter().map(String::from))
    }

    pub fn from_voters(voters: impl IntoIterator<Item = NodeId>) -> Result<Self> {
        Self::from_members(voters.into_iter().collect())
    }

    pub fn members(&self) -> &[NodeId] {
        &self.members
    }

    pub fn contains(&self, recorder_id: &str) -> bool {
        self.members
            .binary_search_by(|member| member.as_str().cmp(recorder_id))
            .is_ok()
    }

    pub const fn digest(&self) -> LogHash {
        self.digest
    }

    pub fn quorum_size(&self) -> usize {
        quorum_size(self.members.len())
    }

    fn from_members(members: Vec<NodeId>) -> Result<Self> {
        if members.is_empty() {
            return Err(Error::EmptyFixedMembership);
        }
        if !(3..=7).contains(&members.len()) {
            return Err(Error::InvalidFixedMembershipSize);
        }
        if members.iter().any(String::is_empty) {
            return Err(Error::EmptyRecorderIdentity);
        }
        let member_count = members.len();
        let members: Vec<_> = members
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        if members.len() != member_count {
            return Err(Error::DuplicateRecorderIdentity);
        }
        Ok(Self {
            digest: canonical_membership_digest(&members)
                .map_err(|_| Error::InvalidFixedMembershipSize)?,
            members,
        })
    }
}

pub type FixedMembership = Membership;

pub trait Consensus {
    fn propose(&self, command: Command) -> Result<LogEntry>;
}

pub fn quorum_size(n: usize) -> usize {
    n / 2 + 1
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
pub struct Ballot {
    pub round: Round,
    pub priority: Priority,
    pub proposer_id: NodeId,
}

impl Ballot {
    pub fn new(round: Round, priority: Priority, proposer_id: impl Into<NodeId>) -> Self {
        Self {
            round,
            priority,
            proposer_id: proposer_id.into(),
        }
    }
}

#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct LogicalStep {
    pub round: Round,
    pub phase: Phase,
}

impl LogicalStep {
    pub const fn as_u64(&self) -> Step {
        self.round * 4 + self.phase as u64
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    serde::Deserialize,
    serde::Serialize,
)]
pub struct ProposalPriority(pub [u8; 32]);

impl ProposalPriority {
    pub const ZERO: Self = Self([0; 32]);
    pub const MAX: Self = Self([u8::MAX; 32]);

    pub const fn from_u64(value: u64) -> Self {
        let mut bytes = [0; 32];
        let encoded = value.to_be_bytes();
        let mut index = 0;
        while index < encoded.len() {
            bytes[24 + index] = encoded[index];
            index += 1;
        }
        Self(bytes)
    }

    fn legacy_u128(self) -> u128 {
        u128::from_be_bytes(self.0[16..].try_into().expect("fixed priority suffix"))
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Proposal {
    pub priority: ProposalPriority,
    pub proposer_id: NodeId,
    pub proposal_id: u64,
    pub value: Option<AcceptedValue>,
}

impl Proposal {
    pub fn new(
        priority: ProposalPriority,
        proposer_id: impl Into<NodeId>,
        proposal_id: u64,
        value: AcceptedValue,
    ) -> Self {
        Self {
            priority,
            proposer_id: proposer_id.into(),
            proposal_id,
            value: Some(value),
        }
    }

    pub fn nil() -> Self {
        Self {
            priority: ProposalPriority::ZERO,
            proposer_id: String::new(),
            proposal_id: 0,
            value: None,
        }
    }

    fn identity(&self) -> (ProposalPriority, &str, u64, Option<&AcceptedValue>) {
        (
            self.priority,
            &self.proposer_id,
            self.proposal_id,
            self.value.as_ref(),
        )
    }

    fn is_nil(&self) -> bool {
        self.value.is_none()
    }
}

impl PartialEq for Proposal {
    fn eq(&self, other: &Self) -> bool {
        self.identity() == other.identity()
    }
}

impl Eq for Proposal {}

impl PartialOrd for Proposal {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for Proposal {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        match (self.is_nil(), other.is_nil()) {
            (true, true) => CmpOrdering::Equal,
            (true, false) => CmpOrdering::Less,
            (false, true) => CmpOrdering::Greater,
            (false, false) => self.identity().cmp(&other.identity()),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct IsrState {
    step: Step,
    first_current: Option<Proposal>,
    aggregate_current: Option<Proposal>,
    aggregate_prior: Option<Proposal>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct IsrReply {
    pub step: Step,
    pub first_current: Option<Proposal>,
    pub aggregate_prior: Option<Proposal>,
}

impl IsrState {
    pub const fn step(&self) -> Step {
        self.step
    }

    pub const fn first_current(&self) -> Option<&Proposal> {
        self.first_current.as_ref()
    }

    pub const fn aggregate_current(&self) -> Option<&Proposal> {
        self.aggregate_current.as_ref()
    }

    pub const fn aggregate_prior(&self) -> Option<&Proposal> {
        self.aggregate_prior.as_ref()
    }

    /// Pure Algorithm 3 transition. Stale inputs return an unchanged state.
    pub fn record(&self, step: Step, proposal: Proposal) -> (Self, IsrReply) {
        let mut next = self.clone();
        if step == next.step {
            if next.first_current.is_none() {
                next.first_current = Some(proposal.clone());
            }
            if next.aggregate_current.as_ref() < Some(&proposal) {
                next.aggregate_current = Some(proposal);
            }
        } else if step > next.step {
            next.aggregate_prior = if step == next.step.saturating_add(1) {
                next.aggregate_current.take()
            } else {
                None
            };
            next.step = step;
            next.first_current = Some(proposal.clone());
            next.aggregate_current = Some(proposal);
        }
        let reply = IsrReply {
            step: next.step,
            first_current: next.first_current.clone(),
            aggregate_prior: next.aggregate_prior.clone(),
        };
        (next, reply)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct AcceptedValue {
    pub command_hash: LogHash,
    pub prev_hash: LogHash,
    pub entry_hash: LogHash,
}

impl PartialOrd for AcceptedValue {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for AcceptedValue {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        (
            self.entry_hash.as_bytes(),
            self.command_hash.as_bytes(),
            self.prev_hash.as_bytes(),
        )
            .cmp(&(
                other.entry_hash.as_bytes(),
                other.command_hash.as_bytes(),
                other.prev_hash.as_bytes(),
            ))
    }
}

impl AcceptedValue {
    pub fn from_command(
        cluster_id: &str,
        slot: Slot,
        epoch: Epoch,
        config_id: ConfigId,
        prev_hash: LogHash,
        command: &StoredCommand,
    ) -> Self {
        Self {
            command_hash: command.hash(),
            prev_hash,
            entry_hash: LogEntry::calculate_hash(
                cluster_id,
                slot,
                epoch,
                config_id,
                command.entry_type,
                prev_hash,
                &command.payload,
            ),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct AcceptedSummary {
    pub ballot: Ballot,
    pub value: AcceptedValue,
}

pub type ProposalSummary = AcceptedSummary;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct DecisionCertificate {
    pub slot: Slot,
    pub epoch: Epoch,
    pub config_id: ConfigId,
    pub config_digest: LogHash,
    pub ballot: Ballot,
    pub value: AcceptedValue,
    pub recorder_ids: Vec<NodeId>,
}

impl DecisionCertificate {
    pub fn cluster_id(&self) -> Option<&str> {
        decode_certificate_proposer(&self.ballot.proposer_id).map(|(cluster_id, _)| cluster_id)
    }

    pub fn validate_for_cluster(
        &self,
        cluster_id: &str,
        config_id: ConfigId,
        membership: &Membership,
    ) -> std::result::Result<(), RejectReason> {
        if self.cluster_id() != Some(cluster_id) {
            return Err(RejectReason::WrongCluster);
        }
        self.validate_for(config_id, membership)
    }

    pub fn validate(&self, membership: &FixedMembership) -> std::result::Result<(), RejectReason> {
        if self.config_digest != membership.digest() {
            return Err(RejectReason::WrongConfig);
        }
        if self.recorder_ids.len() != membership.quorum_size()
            || !self.recorder_ids.windows(2).all(|pair| pair[0] < pair[1])
            || self
                .recorder_ids
                .iter()
                .any(|recorder_id| !membership.contains(recorder_id))
        {
            return Err(RejectReason::InvalidCertificate);
        }
        Ok(())
    }

    pub fn validate_for(
        &self,
        config_id: ConfigId,
        membership: &Membership,
    ) -> std::result::Result<(), RejectReason> {
        if self.config_id != config_id {
            return Err(RejectReason::WrongConfig);
        }
        self.validate(membership)
    }

    fn validate_context(
        &self,
        slot: Slot,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
    ) -> std::result::Result<(), RejectReason> {
        if self.slot != slot || self.epoch != epoch {
            return Err(RejectReason::MalformedDecision);
        }
        if self.config_id != config_id || self.config_digest != config_digest {
            return Err(RejectReason::WrongConfig);
        }
        Ok(())
    }
}

pub type DecisionRecord = DecisionCertificate;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RecorderSummary {
    pub recorder_id: NodeId,
    pub slot: Slot,
    pub step: Step,
    pub first_current: Option<Proposal>,
    pub aggregate_prior: Option<Proposal>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum DecisionProof {
    FastPath {
        cluster_id: ClusterId,
        slot: Slot,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        proposal: Proposal,
        summaries: Vec<RecorderSummary>,
    },
    Phase2 {
        cluster_id: ClusterId,
        slot: Slot,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        step: Step,
        proposal: Proposal,
        summaries: Vec<RecorderSummary>,
    },
}

impl DecisionProof {
    pub fn proposal(&self) -> &Proposal {
        match self {
            Self::FastPath { proposal, .. } | Self::Phase2 { proposal, .. } => proposal,
        }
    }

    pub fn validate_for(
        &self,
        slot: Slot,
        epoch: Epoch,
        config_id: ConfigId,
        membership: &Membership,
    ) -> std::result::Result<(), RejectReason> {
        let (proof_slot, proof_epoch, proof_config, digest, step, proposal, summaries, fast) =
            match self {
                Self::FastPath {
                    slot,
                    epoch,
                    config_id,
                    config_digest,
                    proposal,
                    summaries,
                    ..
                } => (
                    *slot,
                    *epoch,
                    *config_id,
                    *config_digest,
                    4,
                    proposal,
                    summaries,
                    true,
                ),
                Self::Phase2 {
                    slot,
                    epoch,
                    config_id,
                    config_digest,
                    step,
                    proposal,
                    summaries,
                    ..
                } => (
                    *slot,
                    *epoch,
                    *config_id,
                    *config_digest,
                    *step,
                    proposal,
                    summaries,
                    false,
                ),
            };
        if proof_slot != slot || proof_epoch != epoch {
            return Err(RejectReason::MalformedDecision);
        }
        if proof_config != config_id || digest != membership.digest() {
            return Err(RejectReason::WrongConfig);
        }
        if proposal.is_nil() || proposal.value.is_none() {
            return Err(RejectReason::InvalidCertificate);
        }
        if summaries.len() != membership.quorum_size()
            || !summaries
                .windows(2)
                .all(|pair| pair[0].recorder_id < pair[1].recorder_id)
            || summaries.iter().any(|summary| {
                !membership.contains(&summary.recorder_id)
                    || summary.slot != slot
                    || summary.step != step
            })
        {
            return Err(RejectReason::InvalidCertificate);
        }
        if fast {
            if step != 4
                || proposal.priority != ProposalPriority::MAX
                || summaries.iter().any(|summary| {
                    !summary
                        .first_current
                        .as_ref()
                        .is_some_and(|candidate| proposal_exact(candidate, proposal))
                })
            {
                return Err(RejectReason::InvalidCertificate);
            }
        } else {
            if step % 4 != 2 {
                return Err(RejectReason::InvalidCertificate);
            }
            let maximum = summaries
                .iter()
                .filter_map(|summary| summary.aggregate_prior.as_ref())
                .max();
            if maximum != Some(proposal)
                || !maximum.is_some_and(|candidate| proposal_exact(candidate, proposal))
            {
                return Err(RejectReason::InvalidCertificate);
            }
        }
        Ok(())
    }

    pub fn validate_for_cluster(
        &self,
        cluster_id: &str,
        slot: Slot,
        epoch: Epoch,
        config_id: ConfigId,
        membership: &Membership,
    ) -> std::result::Result<(), RejectReason> {
        if proof_cluster_id(self) != cluster_id {
            return Err(RejectReason::WrongCluster);
        }
        self.validate_for(slot, epoch, config_id, membership)
    }
}

fn proposal_exact(left: &Proposal, right: &Proposal) -> bool {
    left == right && left.value == right.value
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RecordRequest {
    pub cluster_id: ClusterId,
    pub epoch: Epoch,
    pub config_id: ConfigId,
    pub config_digest: LogHash,
    pub slot: Slot,
    pub step: Step,
    pub proposal: Proposal,
    #[serde(default)]
    pub command: Option<StoredCommand>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RecordSummary {
    pub recorder_id: NodeId,
    pub slot: Slot,
    pub config_id: ConfigId,
    pub config_digest: LogHash,
    pub step: Step,
    pub first_current: Option<Proposal>,
    pub aggregate_prior: Option<Proposal>,
    pub decided: Option<DecisionProof>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RecordResponse {
    pub from: NodeId,
    pub slot: Slot,
    pub step: Step,
    pub highest_promised: Option<Ballot>,
    pub accepted: Option<AcceptedSummary>,
    pub recorder_epoch: Epoch,
    pub config_id: ConfigId,
    pub config_digest: LogHash,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RecorderReply {
    pub recorder_id: NodeId,
    pub slot: Slot,
    pub config_id: ConfigId,
    pub config_digest: LogHash,
    pub step: Step,
    pub highest_promised: Option<Ballot>,
    pub accepted: Option<AcceptedSummary>,
    pub decided: Option<DecisionCertificate>,
    pub command: Option<StoredCommand>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum RecorderRequest {
    Identity,
    StoreCommand {
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
    },
    FetchCommand {
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        command_hash: LogHash,
    },
    Inspect {
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        slot: Slot,
    },
    /// Legacy transport envelope; active protocol code uses `RecorderRpc::record`.
    Observe {
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        slot: Slot,
        ballot: Ballot,
    },
    /// Legacy transport envelope; active protocol code uses `RecorderRpc::record`.
    Converge {
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        slot: Slot,
        ballot: Ballot,
        value: AcceptedValue,
    },
    Decide {
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        slot: Slot,
        decision: DecisionCertificate,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum RejectReason {
    StaleEpoch,
    FutureEpoch,
    WrongCluster,
    WrongConfig,
    WrongSlot,
    AlreadyDecided,
    MalformedDecision,
    BallotPromised { promised: Ballot },
    ConflictingValue,
    InvalidValue,
    InvalidCertificate,
    ConfigurationSealed { stop_slot: Slot },
    ConfigurationNotInstalled,
    ActivationRequired,
    TransitionInProgress,
    InvalidTransition,
    LocalVoterRequired,
    StepRegression,
    InvalidRequest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigurationSeal {
    pub stop_slot: Slot,
    pub command_hash: LogHash,
    pub prefix_hash: LogHash,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigurationState {
    config_id: ConfigId,
    config_digest: LogHash,
    membership: Option<Membership>,
    predecessor: Option<ConfigurationSeal>,
    seal: Option<ConfigurationSeal>,
    max_accepted_or_decided_slot: Option<Slot>,
    activated: bool,
}

impl ConfigurationState {
    pub const fn config_id(&self) -> ConfigId {
        self.config_id
    }

    pub const fn config_digest(&self) -> LogHash {
        self.config_digest
    }

    pub const fn membership(&self) -> Option<&Membership> {
        self.membership.as_ref()
    }

    pub const fn predecessor(&self) -> Option<&ConfigurationSeal> {
        self.predecessor.as_ref()
    }

    pub const fn seal(&self) -> Option<&ConfigurationSeal> {
        self.seal.as_ref()
    }

    pub const fn is_activated(&self) -> bool {
        self.activated
    }

    pub const fn max_accepted_or_decided_slot(&self) -> Option<Slot> {
        self.max_accepted_or_decided_slot
    }

    fn initial(
        config_id: ConfigId,
        config_digest: LogHash,
        membership: Option<Membership>,
    ) -> Self {
        Self {
            config_id,
            config_digest,
            membership,
            predecessor: None,
            seal: None,
            max_accepted_or_decided_slot: None,
            activated: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SealFaultPoint {
    AfterIntent,
    AfterSlot,
    AfterConfiguration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecorderSlotState {
    slot: Slot,
    cluster_id: ClusterId,
    epoch: Epoch,
    config_id: ConfigId,
    config_digest: LogHash,
    highest_promised: Option<Ballot>,
    accepted: Option<AcceptedSummary>,
    decided: Option<DecisionCertificate>,
    isr: IsrState,
    decided_proof: Option<DecisionProof>,
}

impl RecorderSlotState {
    pub fn new(
        slot: Slot,
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        config_id: ConfigId,
    ) -> Self {
        Self::new_with_digest(slot, cluster_id, epoch, config_id, LogHash::ZERO)
    }

    pub fn new_with_digest(
        slot: Slot,
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
    ) -> Self {
        Self {
            slot,
            cluster_id: cluster_id.into(),
            epoch,
            config_id,
            config_digest,
            highest_promised: None,
            accepted: None,
            decided: None,
            isr: IsrState::default(),
            decided_proof: None,
        }
    }

    pub fn apply(
        &mut self,
        request: RecorderRequest,
    ) -> std::result::Result<RecorderReply, RejectReason> {
        match request {
            RecorderRequest::Inspect {
                cluster_id,
                epoch,
                config_id,
                config_digest,
                slot,
            } => self.inspect(cluster_id, epoch, config_id, config_digest, slot),
            RecorderRequest::Observe { .. }
            | RecorderRequest::Converge { .. }
            | RecorderRequest::Decide { .. } => Err(RejectReason::InvalidRequest),
            RecorderRequest::Identity
            | RecorderRequest::StoreCommand { .. }
            | RecorderRequest::FetchCommand { .. } => Err(RejectReason::InvalidRequest),
        }
    }

    pub fn decided(&self) -> Option<&DecisionCertificate> {
        self.decided.as_ref()
    }

    pub fn decision_proof(&self) -> Option<&DecisionProof> {
        self.decided_proof.as_ref()
    }

    pub fn isr(&self) -> &IsrState {
        &self.isr
    }

    pub fn record(
        &self,
        request: &RecordRequest,
    ) -> std::result::Result<(Self, IsrReply), RejectReason> {
        self.validate(
            request.cluster_id.clone(),
            request.epoch,
            request.config_id,
            request.config_digest,
            request.slot,
        )?;
        let mut next = self.clone();
        let (isr, reply) = self.isr.record(request.step, request.proposal.clone());
        next.isr = isr;
        Ok((next, reply))
    }

    fn install_proof(&mut self, proof: DecisionProof) -> std::result::Result<(), RejectReason> {
        if let Some(existing) = &self.decided_proof {
            if existing.proposal().value != proof.proposal().value {
                return Err(RejectReason::AlreadyDecided);
            }
            return Ok(());
        }
        self.decided_proof = Some(proof);
        Ok(())
    }

    pub const fn slot(&self) -> Slot {
        self.slot
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub const fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub const fn config_id(&self) -> ConfigId {
        self.config_id
    }

    pub const fn config_digest(&self) -> LogHash {
        self.config_digest
    }

    pub fn highest_promised(&self) -> Option<&Ballot> {
        self.highest_promised.as_ref()
    }

    pub fn accepted(&self) -> Option<&AcceptedSummary> {
        self.accepted.as_ref()
    }

    pub fn max_step_seen(&self) -> Step {
        self.highest_promised
            .as_ref()
            .map_or(0, |ballot| ballot.round)
    }

    fn inspect(
        &self,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        slot: Slot,
    ) -> std::result::Result<RecorderReply, RejectReason> {
        self.validate(cluster_id, epoch, config_id, config_digest, slot)?;
        Ok(self.reply())
    }

    fn validate(
        &self,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        slot: Slot,
    ) -> std::result::Result<(), RejectReason> {
        if cluster_id != self.cluster_id {
            return Err(RejectReason::WrongCluster);
        }
        if slot != self.slot {
            return Err(RejectReason::WrongSlot);
        }
        if epoch < self.epoch {
            return Err(RejectReason::StaleEpoch);
        }
        if epoch > self.epoch {
            return Err(RejectReason::FutureEpoch);
        }
        if config_id != self.config_id {
            return Err(RejectReason::WrongConfig);
        }
        if config_digest != self.config_digest {
            return Err(RejectReason::WrongConfig);
        }
        Ok(())
    }

    fn reply(&self) -> RecorderReply {
        RecorderReply {
            recorder_id: String::new(),
            slot: self.slot,
            config_id: self.config_id,
            config_digest: self.config_digest,
            step: self.max_step_seen(),
            highest_promised: self.highest_promised.clone(),
            accepted: self.accepted.clone(),
            decided: self.decided.clone(),
            command: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RecorderFileStore {
    root: PathBuf,
    recorder_id: NodeId,
    cluster_id: ClusterId,
    epoch: Epoch,
    config_id: ConfigId,
    config_digest: LogHash,
    configuration: Arc<Mutex<ConfigurationState>>,
    seal_fault: Arc<Mutex<Option<SealFaultPoint>>>,
    _root_lock: Arc<fs::File>,
    sync: Arc<Mutex<()>>,
}

pub trait RecorderRpc: Send + Sync {
    /// Performs one recorder operation with a finite deadline.
    ///
    /// Implementations must return an error when that deadline expires. Quorum
    /// operations call recorders concurrently, but wait for every call to
    /// finish before evaluating replies; an unbounded implementation can stall
    /// the entire quorum operation.
    fn call(&self, request: RecorderRequest) -> Result<RecorderReply>;

    /// Genuine QuePaxa Record operation. Implementations must opt in.
    fn record(&self, _request: RecordRequest) -> Result<RecordSummary> {
        Err(Error::TypedRecordRequired)
    }

    fn install_decision_proof(
        &self,
        _proof: DecisionProof,
        _membership: &Membership,
    ) -> Result<()> {
        Err(Error::TypedProofInstallRequired)
    }

    fn inspect_decision_proof(&self, _slot: Slot) -> Result<Option<DecisionProof>> {
        Ok(None)
    }

    fn inspect_record_summary(&self, _slot: Slot) -> Result<Option<RecordSummary>> {
        Err(Error::TypedRecordRequired)
    }

    fn uses_typed_protocol(&self) -> bool {
        false
    }

    fn recorder_id(&self) -> Result<NodeId> {
        Ok(self.call(RecorderRequest::Identity)?.recorder_id)
    }

    fn store_command(&self, command_hash: LogHash, command: StoredCommand) -> Result<()> {
        self.store_command_for(String::new(), 0, 0, LogHash::ZERO, command_hash, command)
    }

    fn store_command_for(
        &self,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
    ) -> Result<()> {
        self.call(RecorderRequest::StoreCommand {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            command_hash,
            command,
        })?;
        Ok(())
    }

    fn fetch_command(&self, command_hash: LogHash) -> Result<Option<StoredCommand>> {
        self.fetch_command_for(String::new(), 0, 0, LogHash::ZERO, command_hash)
    }

    fn fetch_command_for(
        &self,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        command_hash: LogHash,
    ) -> Result<Option<StoredCommand>> {
        Ok(self
            .call(RecorderRequest::FetchCommand {
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
            })?
            .command)
    }
}

impl RecorderFileStore {
    pub fn new(
        root: impl Into<PathBuf>,
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        config_id: ConfigId,
    ) -> Result<Self> {
        let root = root.into();
        let recorder_id = root
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .unwrap_or("recorder")
            .to_string();
        Self::new_with_id(root, recorder_id, cluster_id, epoch, config_id)
    }

    pub fn new_with_id(
        root: impl Into<PathBuf>,
        recorder_id: impl Into<NodeId>,
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        config_id: ConfigId,
    ) -> Result<Self> {
        let root = root.into();
        let recorder_id = recorder_id.into();
        if recorder_id.is_empty() {
            return Err(Error::EmptyRecorderIdentity);
        }
        fs::create_dir_all(&root).map_err(|err| Error::Io(err.to_string()))?;
        let root_lock = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join(".recorder.lock"))
            .map_err(|err| Error::Io(err.to_string()))?;
        match root_lock.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => {
                return Err(Error::RecorderRootLocked(root));
            }
            Err(fs::TryLockError::Error(err)) => return Err(Error::Io(err.to_string())),
        }
        Ok(Self {
            root,
            recorder_id,
            cluster_id: cluster_id.into(),
            epoch,
            config_id,
            config_digest: LogHash::ZERO,
            configuration: Arc::new(Mutex::new(ConfigurationState::initial(
                config_id,
                LogHash::ZERO,
                None,
            ))),
            seal_fault: Arc::new(Mutex::new(None)),
            _root_lock: Arc::new(root_lock),
            sync: Arc::new(Mutex::new(())),
        })
    }

    pub fn new_with_membership(
        root: impl Into<PathBuf>,
        recorder_id: impl Into<NodeId>,
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        config_id: ConfigId,
        membership: Membership,
    ) -> Result<Self> {
        let recorder_id = recorder_id.into();
        let mut store = Self::new_with_id(root, recorder_id, cluster_id, epoch, config_id)?;
        let configured = if store.configuration_path().exists() {
            decode_configuration_state(
                &fs::read(store.configuration_path()).map_err(|err| Error::Io(err.to_string()))?,
            )?
        } else {
            let configured =
                ConfigurationState::initial(config_id, membership.digest(), Some(membership));
            atomic_write(
                &store.configuration_path(),
                &encode_configuration_state(&configured)?,
            )?;
            configured
        };
        if configured
            .membership
            .as_ref()
            .is_some_and(|current| current.digest() != configured.config_digest)
        {
            return Err(Error::Decode("installed membership digest mismatch".into()));
        }
        store.config_id = configured.config_id;
        store.config_digest = configured.config_digest;
        store.configuration = Arc::new(Mutex::new(configured));
        store.recover_intent()?;
        Ok(store)
    }

    pub fn configuration_state(&self) -> Result<ConfigurationState> {
        self.configuration
            .lock()
            .map(|state| state.clone())
            .map_err(|_| Error::Io("configuration lock poisoned".into()))
    }

    #[doc(hidden)]
    pub fn set_seal_fault(&self, fault: Option<SealFaultPoint>) -> Result<()> {
        *self
            .seal_fault
            .lock()
            .map_err(|_| Error::Io("seal fault lock poisoned".into()))? = fault;
        Ok(())
    }

    pub fn install_successor(
        &self,
        _next_config_id: ConfigId,
        _membership: Membership,
        _stop_certificate: &DecisionCertificate,
        _stop_slot: Slot,
        _prefix_hash: LogHash,
    ) -> Result<ConfigurationState> {
        Err(Error::Rejected(RejectReason::InvalidTransition))
    }

    pub fn install_successor_from_proof(
        &self,
        membership: Membership,
        stop_proof: &DecisionProof,
    ) -> Result<ConfigurationState> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        let current = self.configuration_state()?;
        let Some(old_membership) = current.membership.as_ref() else {
            return Err(Error::Rejected(RejectReason::ConfigurationNotInstalled));
        };
        let next_config_id = current
            .config_id
            .checked_add(1)
            .ok_or(Error::Rejected(RejectReason::InvalidTransition))?;
        if current.predecessor.is_some() && !current.activated {
            return Err(Error::Rejected(RejectReason::TransitionInProgress));
        }
        if !membership.contains(&self.recorder_id) {
            return Err(Error::Rejected(RejectReason::LocalVoterRequired));
        }
        if proof_cluster_id(stop_proof) != self.cluster_id {
            return Err(Error::Rejected(RejectReason::WrongCluster));
        }
        let (stop_slot, epoch, config_id, config_digest) = proof_context(stop_proof);
        if epoch != self.epoch
            || config_id != current.config_id
            || config_digest != current.config_digest
        {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        stop_proof
            .validate_for_cluster(
                &self.cluster_id,
                stop_slot,
                self.epoch,
                current.config_id,
                old_membership,
            )
            .map_err(Error::Rejected)?;
        let stop_command = ConfigChange::bound_stop(
            self.cluster_id.clone(),
            current.config_id,
            current.config_digest,
            next_config_id,
            membership.members().to_vec(),
        )
        .map_err(|_| Error::Rejected(RejectReason::InvalidTransition))?
        .to_stored_command();
        let stop_value = stop_proof
            .proposal()
            .value
            .as_ref()
            .ok_or(Error::Rejected(RejectReason::InvalidCertificate))?;
        let expected_stop = AcceptedValue::from_command(
            &self.cluster_id,
            stop_slot,
            self.epoch,
            current.config_id,
            stop_value.prev_hash,
            &stop_command,
        );
        if *stop_value != expected_stop {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        let prefix_hash = expected_stop.entry_hash;
        let expected_seal = ConfigurationSeal {
            stop_slot,
            command_hash: expected_stop.command_hash,
            prefix_hash,
        };
        if current
            .seal
            .as_ref()
            .is_some_and(|seal| seal != &expected_seal)
        {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        self.store_command_unlocked(expected_stop.command_hash, &stop_command)?;
        let installed = ConfigurationState {
            config_id: next_config_id,
            config_digest: membership.digest(),
            membership: Some(membership),
            predecessor: Some(expected_seal),
            seal: None,
            max_accepted_or_decided_slot: None,
            activated: false,
        };
        atomic_write(
            &self.configuration_path(),
            &encode_configuration_state(&installed)?,
        )?;
        *self
            .configuration
            .lock()
            .map_err(|_| Error::Io("configuration lock poisoned".into()))? = installed.clone();
        Ok(installed)
    }

    pub fn recover_successor_activation_from_checkpoint(
        &self,
        stop_slot: Slot,
        prefix_hash: LogHash,
        recovered_tip: Slot,
    ) -> Result<ConfigurationState> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        let current = self.configuration_state()?;
        let predecessor = current
            .predecessor
            .as_ref()
            .ok_or(Error::Rejected(RejectReason::InvalidTransition))?;
        if predecessor.stop_slot != stop_slot
            || predecessor.prefix_hash != prefix_hash
            || recovered_tip <= stop_slot
        {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        if current.activated {
            if current
                .max_accepted_or_decided_slot
                .is_some_and(|slot| slot > recovered_tip)
            {
                return Err(Error::Rejected(RejectReason::InvalidTransition));
            }
            return Ok(current);
        }
        let mut recovered = current;
        recovered.activated = true;
        recovered.max_accepted_or_decided_slot = Some(recovered_tip);
        atomic_write(
            &self.configuration_path(),
            &encode_configuration_state(&recovered)?,
        )?;
        *self
            .configuration
            .lock()
            .map_err(|_| Error::Io("configuration lock poisoned".into()))? = recovered.clone();
        Ok(recovered)
    }

    pub fn apply(&self, request: RecorderRequest) -> Result<RecorderReply> {
        if matches!(
            request,
            RecorderRequest::Observe { .. }
                | RecorderRequest::Converge { .. }
                | RecorderRequest::Decide { .. }
        ) {
            return Err(Error::Rejected(RejectReason::InvalidRequest));
        }
        if !matches!(request, RecorderRequest::Identity) {
            self.validate_request_context(&request)?;
        }
        match request {
            RecorderRequest::Identity => Ok(self.reply(0, None)),
            RecorderRequest::StoreCommand {
                config_id,
                config_digest,
                command_hash,
                command,
                ..
            } => {
                let _guard = self
                    .sync
                    .lock()
                    .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
                self.recover_intent()?;
                let context = RecorderRequest::StoreCommand {
                    cluster_id: self.cluster_id.clone(),
                    epoch: self.epoch,
                    config_id,
                    config_digest,
                    command_hash,
                    command: command.clone(),
                };
                self.validate_request_context(&context)?;
                self.store_command_unlocked(command_hash, &command)?;
                let mut reply = self.reply(0, None);
                reply.config_id = config_id;
                reply.config_digest = config_digest;
                Ok(reply)
            }
            RecorderRequest::FetchCommand {
                config_id,
                config_digest,
                command_hash,
                ..
            } => {
                let _guard = self
                    .sync
                    .lock()
                    .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
                self.recover_intent()?;
                let command = self.fetch_command_unlocked(command_hash)?;
                let mut reply = self.reply(0, command);
                reply.config_id = config_id;
                reply.config_digest = config_digest;
                Ok(reply)
            }
            request => {
                let slot =
                    request_slot(&request).ok_or(Error::Rejected(RejectReason::InvalidRequest))?;
                let request_digest = request_context(&request)
                    .ok_or(Error::Rejected(RejectReason::InvalidRequest))?
                    .3;
                let should_save = !matches!(request, RecorderRequest::Inspect { .. });
                let _guard = self
                    .sync
                    .lock()
                    .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
                self.recover_intent()?;
                self.validate_request_context(&request)?;
                let configuration = self.configuration_state()?;
                let change = match &request {
                    RecorderRequest::Converge { value, .. } => {
                        self.change_for_value_unlocked(value)?
                    }
                    RecorderRequest::Decide { decision, .. } => {
                        if let Some(membership) = configuration.membership.as_ref() {
                            decision
                                .validate_for(configuration.config_id, membership)
                                .map_err(Error::Rejected)?;
                        }
                        self.change_for_value_unlocked(&decision.value)?
                    }
                    _ => None,
                };
                if !configuration.activated
                    && change.is_none()
                    && matches!(
                        &request,
                        RecorderRequest::Converge { .. } | RecorderRequest::Decide { .. }
                    )
                {
                    return Err(Error::Rejected(RejectReason::ActivationRequired));
                }
                self.validate_slot_gate(&configuration, slot, change.as_ref())?;
                match &request {
                    RecorderRequest::Converge { value, .. } => {
                        self.validate_value_unlocked(slot, value)?;
                    }
                    RecorderRequest::Decide { decision, .. } => {
                        self.validate_value_unlocked(slot, &decision.value)?;
                    }
                    _ => {}
                }
                let mut state = self.load_unlocked(slot, request_digest)?;
                let mut reply = state.apply(request).map_err(Error::Rejected)?;
                let next_configuration =
                    self.transition_after_apply(&configuration, &state, change.as_ref(), None)?;
                if next_configuration != configuration {
                    self.commit_transition_unlocked(&state, &next_configuration)?;
                } else if should_save {
                    self.save_unlocked(&state)?;
                }
                reply.recorder_id = self.recorder_id.clone();
                Ok(reply)
            }
        }
    }

    pub fn record_proposal(&self, request: RecordRequest) -> Result<RecordSummary> {
        self.validate_record_context(&request)?;
        let value = request
            .proposal
            .value
            .as_ref()
            .ok_or(Error::Rejected(RejectReason::InvalidRequest))?;
        if request.command.as_ref().is_some_and(|command| {
            AcceptedValue::from_command(
                &self.cluster_id,
                request.slot,
                request.epoch,
                request.config_id,
                value.prev_hash,
                command,
            ) != *value
        }) {
            return Err(Error::Rejected(RejectReason::InvalidValue));
        }
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        if let Some(command) = &request.command {
            self.store_command_unlocked(value.command_hash, command)?;
        }
        let configuration = self.configuration_state()?;
        let change = self.change_for_value_unlocked(value)?;
        if !configuration.activated && change.is_none() {
            return Err(Error::Rejected(RejectReason::ActivationRequired));
        }
        self.validate_slot_gate(&configuration, request.slot, change.as_ref())?;
        self.validate_value_unlocked(request.slot, value)?;
        let state = self.load_unlocked(request.slot, request.config_digest)?;
        if let Some(proof) = state.decision_proof() {
            return Ok(record_summary(
                &self.recorder_id,
                &state,
                Some(proof.clone()),
            ));
        }
        let (mut next, _) = state.record(&request).map_err(Error::Rejected)?;

        // Legacy fields are intentionally not synthesized from ISR state.
        next.highest_promised = next.isr.first_current().and_then(proposal_ballot);
        next.accepted = None;
        let next_configuration =
            self.transition_after_apply(&configuration, &next, change.as_ref(), Some(value))?;
        if next_configuration != configuration {
            self.commit_transition_unlocked(&next, &next_configuration)?;
        } else {
            self.save_unlocked(&next)?;
        }
        Ok(record_summary(&self.recorder_id, &next, None))
    }

    pub fn install_decision_proof_record(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> Result<()> {
        let (slot, epoch, config_id, digest) = proof_context(&proof);
        if proof_cluster_id(&proof) != self.cluster_id {
            return Err(Error::Rejected(RejectReason::WrongCluster));
        }
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        let configuration = self.configuration_state()?;
        if epoch != self.epoch
            || config_id != configuration.config_id
            || digest != configuration.config_digest
            || configuration.membership.as_ref() != Some(membership)
        {
            return Err(Error::Rejected(RejectReason::WrongConfig));
        }
        proof
            .validate_for_cluster(&self.cluster_id, slot, epoch, config_id, membership)
            .map_err(Error::Rejected)?;
        let value = proof
            .proposal()
            .value
            .as_ref()
            .ok_or(Error::Rejected(RejectReason::InvalidCertificate))?;
        self.validate_value_unlocked(slot, value)?;
        let mut state = self.load_unlocked(slot, digest)?;
        if state.decision_proof().is_some() {
            state.install_proof(proof).map_err(Error::Rejected)?;
            return Ok(());
        }
        let change = self.change_for_value_unlocked(value)?;
        self.validate_slot_gate(&configuration, slot, change.as_ref())?;
        state
            .install_proof(proof.clone())
            .map_err(Error::Rejected)?;
        let certificate = certificate_from_proof(&proof)?;
        if let Some(existing) = &state.decided {
            if existing.value != certificate.value {
                return Err(Error::Rejected(RejectReason::AlreadyDecided));
            }
        } else {
            state.decided = Some(certificate);
        }
        let next =
            self.transition_after_apply(&configuration, &state, change.as_ref(), Some(value))?;
        if next != configuration {
            self.commit_transition_unlocked(&state, &next)
        } else {
            self.save_unlocked(&state)
        }
    }

    fn validate_record_context(&self, request: &RecordRequest) -> Result<()> {
        if request.cluster_id != self.cluster_id {
            return Err(Error::Rejected(RejectReason::WrongCluster));
        }
        if request.epoch != self.epoch {
            return Err(Error::Rejected(if request.epoch < self.epoch {
                RejectReason::StaleEpoch
            } else {
                RejectReason::FutureEpoch
            }));
        }
        let configuration = self.configuration_state()?;
        if request.config_id != configuration.config_id
            || (configuration.config_digest != LogHash::ZERO
                && request.config_digest != configuration.config_digest)
        {
            return Err(Error::Rejected(RejectReason::WrongConfig));
        }
        Ok(())
    }

    pub fn load(&self, slot: Slot) -> Result<RecorderSlotState> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.load_unlocked(slot, self.config_digest())
    }

    pub fn save(&self, state: &RecorderSlotState) -> Result<()> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        let configuration = self.configuration_state()?;
        if state.cluster_id != self.cluster_id
            || state.epoch != self.epoch
            || state.config_id != configuration.config_id
            || (configuration.config_digest != LogHash::ZERO
                && state.config_digest != configuration.config_digest)
        {
            return Err(Error::Rejected(RejectReason::WrongConfig));
        }
        let change = state
            .decided()
            .map(|decision| &decision.value)
            .or_else(|| state.accepted().map(|accepted| &accepted.value))
            .map(|value| self.change_for_value_unlocked(value))
            .transpose()?
            .flatten();
        self.validate_slot_gate(&configuration, state.slot(), change.as_ref())?;
        let applied_value = state
            .decided()
            .map(|decision| &decision.value)
            .or_else(|| state.accepted().map(|accepted| &accepted.value));
        let next =
            self.transition_after_apply(&configuration, state, change.as_ref(), applied_value)?;
        if next != configuration {
            self.commit_transition_unlocked(state, &next)
        } else {
            self.save_unlocked(state)
        }
    }

    pub fn store_command(&self, command_hash: LogHash, command: StoredCommand) -> Result<()> {
        if command.hash() != command_hash {
            return Err(Error::CommandHashMismatch);
        }
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.store_command_unlocked(command_hash, &command)
    }

    fn store_command_unlocked(&self, command_hash: LogHash, command: &StoredCommand) -> Result<()> {
        let path = self.command_path(command_hash);
        if path.exists() {
            return match self.fetch_command_unlocked(command_hash)? {
                Some(existing) if existing == *command => Ok(()),
                _ => Err(Error::CommandHashMismatch),
            };
        }
        atomic_write(&path, &encode_stored_command(command))
    }

    pub fn fetch_command(&self, command_hash: LogHash) -> Result<Option<StoredCommand>> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.fetch_command_unlocked(command_hash)
    }

    fn load_unlocked(&self, slot: Slot, config_digest: LogHash) -> Result<RecorderSlotState> {
        let path = self.path(slot);
        if !path.exists() {
            return Ok(RecorderSlotState::new_with_digest(
                slot,
                self.cluster_id.clone(),
                self.epoch,
                self.current_config_id(),
                config_digest,
            ));
        }
        let state =
            decode_recorder_state(&fs::read(path).map_err(|err| Error::Io(err.to_string()))?)?;
        if state.slot != slot
            || state.cluster_id != self.cluster_id
            || state.epoch != self.epoch
            || state.config_id != self.current_config_id()
            || (config_digest != LogHash::ZERO && state.config_digest != config_digest)
        {
            return Err(Error::Decode("recorder state identity mismatch".into()));
        }
        Ok(state)
    }

    fn save_unlocked(&self, state: &RecorderSlotState) -> Result<()> {
        atomic_write(&self.path(state.slot()), &encode_recorder_state(state)?)
    }

    fn fetch_command_unlocked(&self, command_hash: LogHash) -> Result<Option<StoredCommand>> {
        let path = self.command_path(command_hash);
        if !path.exists() {
            return Ok(None);
        }
        let command =
            decode_stored_command(&fs::read(path).map_err(|err| Error::Io(err.to_string()))?)?;
        if command.hash() != command_hash {
            return Err(Error::CommandHashMismatch);
        }
        Ok(Some(command))
    }

    fn validate_value_unlocked(&self, slot: Slot, value: &AcceptedValue) -> Result<()> {
        let command = self
            .fetch_command_unlocked(value.command_hash)?
            .ok_or(Error::CommandUnavailable)?;
        let expected = AcceptedValue::from_command(
            &self.cluster_id,
            slot,
            self.epoch,
            self.current_config_id(),
            value.prev_hash,
            &command,
        );
        if expected != *value {
            return Err(Error::Rejected(RejectReason::InvalidValue));
        }
        Ok(())
    }

    fn change_for_value_unlocked(&self, value: &AcceptedValue) -> Result<Option<ConfigChange>> {
        let command = self
            .fetch_command_unlocked(value.command_hash)?
            .ok_or(Error::CommandUnavailable)?;
        if command.entry_type != EntryType::ConfigChange {
            return Ok(None);
        }
        ConfigChange::recognize(&command)
            .map_err(|_| Error::Rejected(RejectReason::InvalidRequest))
            .map(Some)
    }

    fn validate_slot_gate(
        &self,
        configuration: &ConfigurationState,
        slot: Slot,
        change: Option<&ConfigChange>,
    ) -> Result<()> {
        if let Some(predecessor) = &configuration.predecessor {
            if slot <= predecessor.stop_slot {
                return Err(Error::Rejected(RejectReason::ConfigurationNotInstalled));
            }
        }
        if let Some(seal) = &configuration.seal {
            if slot > seal.stop_slot {
                return Err(Error::Rejected(RejectReason::ConfigurationSealed {
                    stop_slot: seal.stop_slot,
                }));
            }
            if matches!(
                change,
                Some(ConfigChange::Stop { .. } | ConfigChange::BoundStop { .. })
            ) && (slot != seal.stop_slot || seal.command_hash == LogHash::ZERO)
            {
                return Err(Error::Rejected(RejectReason::TransitionInProgress));
            }
        }
        if matches!(
            change,
            Some(ConfigChange::Stop { .. } | ConfigChange::BoundStop { .. })
        ) && configuration
            .max_accepted_or_decided_slot
            .is_some_and(|accepted_slot| accepted_slot > slot)
        {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        if !configuration.activated {
            let Some(predecessor) = &configuration.predecessor else {
                return Err(Error::Rejected(RejectReason::InvalidTransition));
            };
            match change {
                Some(ConfigChange::BoundActivationBarrier {
                    successor,
                    stop_slot,
                    prefix_hash,
                    stop_command_hash,
                }) if successor.cluster_id() == self.cluster_id
                    && successor.config_id() == configuration.config_id
                    && successor.digest() == configuration.config_digest
                    && successor.predecessor_config_id().checked_add(1)
                        == Some(configuration.config_id)
                    && *stop_slot == predecessor.stop_slot
                    && *prefix_hash == predecessor.prefix_hash
                    && *stop_command_hash == predecessor.command_hash
                    && slot == predecessor.stop_slot + 1 => {}
                None if slot == predecessor.stop_slot + 1 => {}
                _ => return Err(Error::Rejected(RejectReason::ActivationRequired)),
            }
        } else if matches!(
            change,
            Some(
                ConfigChange::ActivationBarrier { .. }
                    | ConfigChange::BoundActivationBarrier { .. }
            )
        ) {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        if let Some(change) = change {
            let (config_id, config_digest) = change.binding();
            if config_id != configuration.config_id || config_digest != configuration.config_digest
            {
                return Err(Error::Rejected(RejectReason::WrongConfig));
            }
        }
        Ok(())
    }

    fn transition_after_apply(
        &self,
        configuration: &ConfigurationState,
        state: &RecorderSlotState,
        change: Option<&ConfigChange>,
        applied_value: Option<&AcceptedValue>,
    ) -> Result<ConfigurationState> {
        let mut next = configuration.clone();
        if applied_value.is_some() {
            next.max_accepted_or_decided_slot = Some(
                next.max_accepted_or_decided_slot
                    .map_or(state.slot(), |current| current.max(state.slot())),
            );
        }
        if state.decision_proof().is_some()
            && next.seal.as_ref().is_some_and(|seal| {
                seal.stop_slot == state.slot()
                    && applied_value.is_some_and(|value| value.command_hash != seal.command_hash)
            })
        {
            next.seal = None;
        }
        match change {
            Some(ConfigChange::Stop { .. } | ConfigChange::BoundStop { .. })
                if applied_value.is_some() =>
            {
                let value = applied_value.expect("checked applied value");
                let proposed = ConfigurationSeal {
                    stop_slot: state.slot(),
                    command_hash: value.command_hash,
                    prefix_hash: value.entry_hash,
                };
                if let Some(existing) = &next.seal {
                    if existing != &proposed {
                        return Err(Error::Rejected(RejectReason::TransitionInProgress));
                    }
                } else {
                    next.seal = Some(proposed);
                }
            }
            Some(
                ConfigChange::ActivationBarrier { .. }
                | ConfigChange::BoundActivationBarrier { .. },
            ) if state.decision_proof().is_some() => {
                next.activated = true;
            }
            _ => {}
        }
        Ok(next)
    }

    fn validate_request_context(&self, request: &RecorderRequest) -> Result<()> {
        let (cluster_id, epoch, config_id, config_digest) =
            request_context(request).ok_or(Error::Rejected(RejectReason::InvalidRequest))?;
        if cluster_id != &self.cluster_id {
            return Err(Error::Rejected(RejectReason::WrongCluster));
        }
        if epoch < self.epoch {
            return Err(Error::Rejected(RejectReason::StaleEpoch));
        }
        if epoch > self.epoch {
            return Err(Error::Rejected(RejectReason::FutureEpoch));
        }
        let configuration = self.configuration_state()?;
        if config_id != configuration.config_id {
            return Err(Error::Rejected(RejectReason::WrongConfig));
        }
        if configuration.config_digest != LogHash::ZERO
            && config_digest != configuration.config_digest
        {
            return Err(Error::Rejected(RejectReason::WrongConfig));
        }
        Ok(())
    }

    fn reply(&self, slot: Slot, command: Option<StoredCommand>) -> RecorderReply {
        RecorderReply {
            recorder_id: self.recorder_id.clone(),
            slot,
            config_id: self.current_config_id(),
            config_digest: self.config_digest(),
            step: 0,
            highest_promised: None,
            accepted: None,
            decided: None,
            command,
        }
    }

    fn path(&self, slot: Slot) -> PathBuf {
        self.root.join(format!("slot-{slot:020}.rec"))
    }

    fn command_path(&self, command_hash: LogHash) -> PathBuf {
        self.root
            .join(format!("command-{}.cmd", command_hash.to_hex()))
    }

    fn configuration_path(&self) -> PathBuf {
        self.root.join("configuration.rec")
    }

    fn intent_path(&self) -> PathBuf {
        self.root.join("configuration.intent")
    }

    fn recover_intent(&self) -> Result<()> {
        let path = self.intent_path();
        if !path.exists() {
            return Ok(());
        }
        let (slot, slot_bytes, configuration_bytes) =
            decode_transition_intent(&fs::read(&path).map_err(|err| Error::Io(err.to_string()))?)?;
        let configuration = decode_configuration_state(&configuration_bytes)?;
        atomic_write(&self.path(slot), &slot_bytes)?;
        atomic_write(&self.configuration_path(), &configuration_bytes)?;
        fs::remove_file(path).map_err(|err| Error::Io(err.to_string()))?;
        fs::File::open(&self.root)
            .and_then(|directory| directory.sync_all())
            .map_err(|err| Error::Io(err.to_string()))?;
        *self
            .configuration
            .lock()
            .map_err(|_| Error::Io("configuration lock poisoned".into()))? = configuration;
        Ok(())
    }

    fn commit_transition_unlocked(
        &self,
        slot_state: &RecorderSlotState,
        configuration: &ConfigurationState,
    ) -> Result<()> {
        let slot_bytes = encode_recorder_state(slot_state)?;
        let configuration_bytes = encode_configuration_state(configuration)?;
        atomic_write(
            &self.intent_path(),
            &encode_transition_intent(slot_state.slot(), &slot_bytes, &configuration_bytes)?,
        )?;
        self.fail_seal_at(SealFaultPoint::AfterIntent)?;
        atomic_write(&self.path(slot_state.slot()), &slot_bytes)?;
        self.fail_seal_at(SealFaultPoint::AfterSlot)?;
        atomic_write(&self.configuration_path(), &configuration_bytes)?;
        self.fail_seal_at(SealFaultPoint::AfterConfiguration)?;
        fs::remove_file(self.intent_path()).map_err(|err| Error::Io(err.to_string()))?;
        fs::File::open(&self.root)
            .and_then(|directory| directory.sync_all())
            .map_err(|err| Error::Io(err.to_string()))?;
        *self
            .configuration
            .lock()
            .map_err(|_| Error::Io("configuration lock poisoned".into()))? = configuration.clone();
        Ok(())
    }

    fn fail_seal_at(&self, point: SealFaultPoint) -> Result<()> {
        let mut fault = self
            .seal_fault
            .lock()
            .map_err(|_| Error::Io("seal fault lock poisoned".into()))?;
        if *fault == Some(point) {
            *fault = None;
            return Err(Error::Io(format!("injected seal fault at {point:?}")));
        }
        Ok(())
    }

    fn config_digest(&self) -> LogHash {
        self.configuration
            .lock()
            .map(|state| state.config_digest)
            .unwrap_or(self.config_digest)
    }

    fn current_config_id(&self) -> ConfigId {
        self.configuration
            .lock()
            .map(|state| state.config_id)
            .unwrap_or(self.config_id)
    }
}

pub struct ThreeNodeConsensus {
    cluster_id: ClusterId,
    proposer_id: NodeId,
    epoch: Epoch,
    config_id: ConfigId,
    config_digest: LogHash,
    membership: FixedMembership,
    recorders: Vec<Arc<dyn RecorderRpc>>,
    priority_source: Arc<dyn PrioritySource>,
    proposal_sequence: AtomicU64,
    legacy_tip: Mutex<SingleNodeState>,
    background_threads: Mutex<Vec<thread::JoinHandle<()>>>,
}

pub type QuePaxaConsensus = ThreeNodeConsensus;
pub type StoppableQuePaxa = ThreeNodeConsensus;

pub trait PrioritySource: Send + Sync {
    fn sample(
        &self,
        slot: Slot,
        round: Round,
        proposer_id: &str,
        recorder_id: &str,
    ) -> Result<ProposalPriority>;
}

#[derive(Debug, Default)]
pub struct OsPrioritySource;

impl PrioritySource for OsPrioritySource {
    fn sample(
        &self,
        slot: Slot,
        round: Round,
        proposer_id: &str,
        recorder_id: &str,
    ) -> Result<ProposalPriority> {
        let mut bytes = [0; 32];
        let _ = (slot, round, proposer_id, recorder_id);
        fs::File::open("/dev/urandom")
            .and_then(|mut file| file.read_exact(&mut bytes))
            .map_err(|error| Error::RandomnessUnavailable(error.to_string()))?;
        if bytes == ProposalPriority::ZERO.0 || bytes == ProposalPriority::MAX.0 {
            bytes[31] = 1;
        }
        Ok(ProposalPriority(bytes))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProposerProgress {
    pub slot: Slot,
    pub step: Step,
    pub proposal: Proposal,
    phase_zero_priorities: BTreeMap<(Round, NodeId), ProposalPriority>,
    command: Option<StoredCommand>,
    command_holders: BTreeSet<NodeId>,
    transition_involved: bool,
}

impl ProposerProgress {
    pub fn new(slot: Slot, proposal: Proposal) -> Self {
        Self {
            slot,
            step: 4,
            proposal,
            phase_zero_priorities: BTreeMap::new(),
            command: None,
            command_holders: BTreeSet::new(),
            transition_involved: false,
        }
    }

    fn with_command(mut self, command: StoredCommand) -> Self {
        self.transition_involved = command.entry_type == EntryType::ConfigChange;
        self.command = Some(command);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DriveOutcome {
    Progress(ProposerProgress),
    Pending(ProposerProgress),
    Decision(DecisionProof),
}

impl fmt::Debug for ThreeNodeConsensus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ThreeNodeConsensus")
            .field("cluster_id", &self.cluster_id)
            .field("proposer_id", &self.proposer_id)
            .field("epoch", &self.epoch)
            .field("config_id", &self.config_id)
            .field("recorders", &self.membership.members())
            .finish_non_exhaustive()
    }
}

impl Drop for ThreeNodeConsensus {
    fn drop(&mut self) {
        let handles = match self.background_threads.get_mut() {
            Ok(handles) => std::mem::take(handles),
            Err(poisoned) => std::mem::take(poisoned.into_inner()),
        };
        for handle in handles {
            let _ = handle.join();
        }
    }
}

impl ThreeNodeConsensus {
    pub const fn config_id(&self) -> ConfigId {
        self.config_id
    }

    pub const fn membership(&self) -> &FixedMembership {
        &self.membership
    }

    pub fn new(
        cluster_id: impl Into<ClusterId>,
        proposer_id: impl Into<NodeId>,
        epoch: Epoch,
        config_id: ConfigId,
        recorder_roots: [PathBuf; 3],
    ) -> Result<Self> {
        Self::from_recovered_tip(
            cluster_id,
            proposer_id,
            epoch,
            config_id,
            recorder_roots,
            1,
            LogHash::ZERO,
        )
    }

    pub fn from_recovered_tip(
        cluster_id: impl Into<ClusterId>,
        proposer_id: impl Into<NodeId>,
        epoch: Epoch,
        config_id: ConfigId,
        recorder_roots: [PathBuf; 3],
        next_index: LogIndex,
        last_hash: LogHash,
    ) -> Result<Self> {
        let cluster_id = cluster_id.into();
        let recorder_roots: Vec<_> = recorder_roots.into_iter().collect();
        let recorder_ids: Vec<_> = recorder_roots
            .iter()
            .map(|root| {
                root.file_name()
                    .and_then(|name| name.to_str())
                    .filter(|name| !name.is_empty())
                    .unwrap_or("recorder")
                    .to_owned()
            })
            .collect();
        let membership = Membership::from_voters(recorder_ids.iter().cloned())?;
        let recorders = recorder_roots
            .into_iter()
            .zip(recorder_ids)
            .map(|(root, recorder_id)| -> Result<Box<dyn RecorderRpc>> {
                Ok(Box::new(RecorderFileStore::new_with_membership(
                    root,
                    recorder_id,
                    cluster_id.clone(),
                    epoch,
                    config_id,
                    membership.clone(),
                )?) as Box<dyn RecorderRpc>)
            })
            .collect::<Result<Vec<_>>>()?;
        Self::from_recorders_with_recovered_tip(
            cluster_id,
            proposer_id,
            epoch,
            config_id,
            recorders,
            next_index,
            last_hash,
        )
    }

    pub fn from_recorders(
        cluster_id: impl Into<ClusterId>,
        proposer_id: impl Into<NodeId>,
        epoch: Epoch,
        config_id: ConfigId,
        recorders: Vec<Box<dyn RecorderRpc>>,
    ) -> Result<Self> {
        Self::from_recorders_with_recovered_tip(
            cluster_id,
            proposer_id,
            epoch,
            config_id,
            recorders,
            1,
            LogHash::ZERO,
        )
    }

    /// Constructs a proposer from expected recorder identities paired with RPC clients.
    ///
    /// This path does not issue `Identity` RPCs. Reply identities are still
    /// checked against the corresponding expected identity on every call.
    pub fn from_recorders_with_ids(
        cluster_id: impl Into<ClusterId>,
        proposer_id: impl Into<NodeId>,
        epoch: Epoch,
        config_id: ConfigId,
        recorders: Vec<(NodeId, Box<dyn RecorderRpc>)>,
    ) -> Result<Self> {
        Self::from_recorders_with_ids_and_recovered_tip(
            cluster_id,
            proposer_id,
            epoch,
            config_id,
            recorders,
            1,
            LogHash::ZERO,
        )
    }

    pub fn from_recorders_with_recovered_tip(
        cluster_id: impl Into<ClusterId>,
        proposer_id: impl Into<NodeId>,
        epoch: Epoch,
        config_id: ConfigId,
        recorders: Vec<Box<dyn RecorderRpc>>,
        next_index: LogIndex,
        last_hash: LogHash,
    ) -> Result<Self> {
        let recorder_ids = recorders
            .iter()
            .map(|recorder| recorder.recorder_id())
            .collect::<Result<Vec<_>>>()?;
        Self::from_recorders_with_ids_and_recovered_tip(
            cluster_id,
            proposer_id,
            epoch,
            config_id,
            recorder_ids.into_iter().zip(recorders).collect(),
            next_index,
            last_hash,
        )
    }

    /// Recovered-tip variant of [`Self::from_recorders_with_ids`].
    pub fn from_recorders_with_ids_and_recovered_tip(
        cluster_id: impl Into<ClusterId>,
        proposer_id: impl Into<NodeId>,
        epoch: Epoch,
        config_id: ConfigId,
        recorders: Vec<(NodeId, Box<dyn RecorderRpc>)>,
        next_index: LogIndex,
        last_hash: LogHash,
    ) -> Result<Self> {
        if next_index == 0 {
            return Err(Error::InvalidRecoveredTip);
        }
        let (recorder_ids, recorders): (Vec<_>, Vec<_>) = recorders.into_iter().unzip();
        let recorders = recorders.into_iter().map(Arc::from).collect();
        let membership = FixedMembership::from_members(recorder_ids)?;
        let config_digest = membership.digest();
        Ok(Self {
            cluster_id: cluster_id.into(),
            proposer_id: proposer_id.into(),
            epoch,
            config_id,
            config_digest,
            membership,
            recorders,
            priority_source: Arc::new(OsPrioritySource),
            proposal_sequence: AtomicU64::new(1),
            legacy_tip: Mutex::new(SingleNodeState {
                next_index,
                last_hash,
            }),
            background_threads: Mutex::new(Vec::new()),
        })
    }

    pub fn with_priority_source(mut self, source: Arc<dyn PrioritySource>) -> Self {
        self.priority_source = source;
        self
    }

    pub fn register_command(&self, command_hash: LogHash, command_bytes: Vec<u8>) {
        let command = StoredCommand::new(EntryType::Command, command_bytes);
        if command.hash() == command_hash {
            let _ = self.store_command_on_quorum(command_hash, &command);
        }
    }

    pub fn propose_with_priority(&self, command: Command, priority: Priority) -> Result<LogEntry> {
        let mut tip = self.legacy_tip.lock().map_err(|_| Error::ProposeFailed)?;
        let entry =
            self.propose_at_with_priority(tip.next_index, tip.last_hash, command, priority)?;
        tip.next_index = entry.index + 1;
        tip.last_hash = entry.hash;
        Ok(entry)
    }

    pub fn propose_at(&self, slot: Slot, prev_hash: LogHash, command: Command) -> Result<LogEntry> {
        self.propose_at_with_priority(slot, prev_hash, command, Priority::MAX)
    }

    pub fn propose_at_cancellable(
        &self,
        slot: Slot,
        prev_hash: LogHash,
        command: Command,
        cancelled: &std::sync::atomic::AtomicBool,
    ) -> Result<LogEntry> {
        self.propose_stored_at_with_priority_until(
            slot,
            prev_hash,
            stored_command(command)?,
            Priority::MAX,
            || cancelled.load(Ordering::Acquire),
        )
    }

    pub fn propose_at_with_priority(
        &self,
        slot: Slot,
        prev_hash: LogHash,
        command: Command,
        priority: Priority,
    ) -> Result<LogEntry> {
        self.propose_stored_at_with_priority(slot, prev_hash, stored_command(command)?, priority)
    }

    pub fn propose_stop_at(&self, slot: Slot, prev_hash: LogHash) -> Result<LogEntry> {
        self.propose_stored_at_with_priority(
            slot,
            prev_hash,
            ConfigChange::stop(self.config_id, self.config_digest).to_stored_command(),
            Priority::MAX,
        )
    }

    pub fn propose_stop_for_successor_at(
        &self,
        slot: Slot,
        prev_hash: LogHash,
        successor: &Membership,
    ) -> Result<LogEntry> {
        let next_config_id = self
            .config_id
            .checked_add(1)
            .ok_or(Error::Rejected(RejectReason::InvalidTransition))?;
        let stop = ConfigChange::bound_stop(
            self.cluster_id.clone(),
            self.config_id,
            self.config_digest,
            next_config_id,
            successor.members().to_vec(),
        )
        .map_err(|_| Error::Rejected(RejectReason::InvalidTransition))?;
        self.propose_stored_at_with_priority(
            slot,
            prev_hash,
            stop.to_stored_command(),
            Priority::MAX,
        )
    }

    pub fn propose_activation_barrier_at(
        &self,
        stop_slot: Slot,
        prefix_hash: LogHash,
    ) -> Result<LogEntry> {
        self.propose_stored_at_with_priority(
            stop_slot.checked_add(1).ok_or(Error::InvalidRecoveredTip)?,
            prefix_hash,
            ConfigChange::activation_barrier(
                self.config_id,
                self.config_digest,
                stop_slot,
                prefix_hash,
            )
            .to_stored_command(),
            Priority::MAX,
        )
    }

    pub fn propose_activation_for_stop_entry(&self, stop: &LogEntry) -> Result<LogEntry> {
        let command = StoredCommand::new(stop.entry_type, stop.payload.clone());
        let change = ConfigChange::recognize(&command)
            .map_err(|_| Error::Rejected(RejectReason::InvalidTransition))?;
        let successor = change
            .successor()
            .filter(|successor| {
                successor.cluster_id() == self.cluster_id
                    && successor.config_id() == self.config_id
                    && successor.digest() == self.config_digest
                    && successor.members() == self.membership.members()
            })
            .ok_or(Error::Rejected(RejectReason::InvalidTransition))?
            .clone();
        self.propose_stored_at_with_priority(
            stop.index
                .checked_add(1)
                .ok_or(Error::InvalidRecoveredTip)?,
            stop.hash,
            ConfigChange::bound_activation_barrier(
                successor,
                stop.index,
                stop.hash,
                command.hash(),
            )
            .to_stored_command(),
            Priority::MAX,
        )
    }

    pub fn propose_activation_for_stop_at(&self, stop_proof: &DecisionProof) -> Result<LogEntry> {
        if proof_cluster_id(stop_proof) != self.cluster_id {
            return Err(Error::Rejected(RejectReason::WrongCluster));
        }
        let (stop_slot, epoch, predecessor_config_id, _) = proof_context(stop_proof);
        if epoch != self.epoch || predecessor_config_id.checked_add(1) != Some(self.config_id) {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        let value = stop_proof
            .proposal()
            .value
            .as_ref()
            .ok_or(Error::Rejected(RejectReason::InvalidCertificate))?;
        let bound_stop = ConfigChange::bound_stop(
            self.cluster_id.clone(),
            predecessor_config_id,
            proof_context(stop_proof).3,
            self.config_id,
            self.membership.members().to_vec(),
        )
        .map_err(|_| Error::Rejected(RejectReason::InvalidTransition))?;
        let stop_command = bound_stop.to_stored_command();
        let expected = AcceptedValue::from_command(
            &self.cluster_id,
            stop_slot,
            self.epoch,
            predecessor_config_id,
            value.prev_hash,
            &stop_command,
        );
        if &expected != value {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        let successor = bound_stop
            .successor()
            .expect("bound stop has successor")
            .clone();
        self.propose_stored_at_with_priority(
            stop_slot.checked_add(1).ok_or(Error::InvalidRecoveredTip)?,
            value.entry_hash,
            ConfigChange::bound_activation_barrier(
                successor,
                stop_slot,
                value.entry_hash,
                value.command_hash,
            )
            .to_stored_command(),
            Priority::MAX,
        )
    }

    pub fn propose_stored_at(
        &self,
        slot: Slot,
        prev_hash: LogHash,
        command: StoredCommand,
    ) -> Result<LogEntry> {
        self.propose_stored_at_with_priority(slot, prev_hash, command, Priority::MAX)
    }

    fn propose_stored_at_with_priority(
        &self,
        slot: Slot,
        prev_hash: LogHash,
        offered_command: StoredCommand,
        _priority: Priority,
    ) -> Result<LogEntry> {
        self.propose_stored_at_with_priority_until(
            slot,
            prev_hash,
            offered_command,
            _priority,
            || false,
        )
    }

    fn propose_stored_at_with_priority_until<F>(
        &self,
        slot: Slot,
        prev_hash: LogHash,
        offered_command: StoredCommand,
        _priority: Priority,
        cancelled: F,
    ) -> Result<LogEntry>
    where
        F: Fn() -> bool,
    {
        if cancelled() {
            return Err(Error::Cancelled);
        }
        let offered_value = AcceptedValue::from_command(
            &self.cluster_id,
            slot,
            self.epoch,
            self.config_id,
            prev_hash,
            &offered_command,
        );
        let proposal_id = self.proposal_sequence.fetch_add(1, Ordering::Relaxed);
        let mut progress = ProposerProgress::new(
            slot,
            Proposal::new(
                ProposalPriority::MAX,
                self.proposer_id.clone(),
                proposal_id,
                offered_value,
            ),
        )
        .with_command(offered_command.clone());
        loop {
            if cancelled() {
                return Err(Error::Cancelled);
            }
            match self.drive(progress)? {
                DriveOutcome::Progress(next) => progress = next,
                DriveOutcome::Pending(next) => {
                    progress = next;
                    thread::sleep(std::time::Duration::from_millis(10));
                }
                DriveOutcome::Decision(proof) => {
                    let value = proof
                        .proposal()
                        .value
                        .as_ref()
                        .ok_or(Error::Rejected(RejectReason::InvalidCertificate))?;
                    self.ensure_predecessor(slot, prev_hash, value.prev_hash)?;
                    let command = if self.command_matches_value(slot, value, &offered_command) {
                        offered_command.clone()
                    } else {
                        self.fetch_verified_value(slot, value)?
                            .ok_or(Error::CommandUnavailable)?
                    };
                    return self.log_entry_from_value(slot, command, value);
                }
            }
        }
    }

    pub fn drive(&self, mut progress: ProposerProgress) -> Result<DriveOutcome> {
        self.ensure_progress_command(&mut progress)?;
        let round = progress.step / 4;
        let phase = progress.step % 4;
        let command_targets: BTreeSet<_> = self
            .membership
            .members()
            .iter()
            .filter(|recorder_id| !progress.command_holders.contains(*recorder_id))
            .cloned()
            .collect();
        let requests: Vec<_> = self
            .membership
            .members()
            .iter()
            .map(|recorder_id| -> Result<RecordRequest> {
                let mut proposal = progress.proposal.clone();
                if phase == 0 {
                    proposal.priority =
                        if progress.step == 4 && self.proposer_id == self.membership.members()[0] {
                            ProposalPriority::MAX
                        } else {
                            *progress
                                .phase_zero_priorities
                                .entry((round, recorder_id.clone()))
                                .or_insert(self.priority_source.sample(
                                    progress.slot,
                                    round,
                                    &self.proposer_id,
                                    recorder_id,
                                )?)
                        };
                }
                Ok(RecordRequest {
                    cluster_id: self.cluster_id.clone(),
                    epoch: self.epoch,
                    config_id: self.config_id,
                    config_digest: self.config_digest,
                    slot: progress.slot,
                    step: progress.step,
                    proposal,
                    command: command_targets
                        .contains(recorder_id)
                        .then(|| progress.command.clone())
                        .flatten(),
                })
            })
            .collect::<Result<_>>()?;
        let mut replies = self.record_broadcast(requests)?;
        progress.command_holders.extend(
            replies
                .iter()
                .filter(|reply| command_targets.contains(&reply.recorder_id))
                .map(|reply| reply.recorder_id.clone()),
        );
        for reply in &replies {
            if let Some(proof) = &reply.decided {
                if proof_cluster_id(proof) != self.cluster_id {
                    return Err(Error::Rejected(RejectReason::WrongCluster));
                }
                proof
                    .validate_for_cluster(
                        &self.cluster_id,
                        progress.slot,
                        self.epoch,
                        self.config_id,
                        &self.membership,
                    )
                    .map_err(Error::Rejected)?;
                return self.finish_decision(
                    proof.clone(),
                    progress.command.as_ref(),
                    progress.transition_involved,
                );
            }
        }
        if let Some(highest) = replies.iter().map(|reply| reply.step).max() {
            if highest > progress.step {
                let caught_up = replies
                    .iter()
                    .filter(|reply| reply.step == highest)
                    .min_by(|left, right| left.recorder_id.cmp(&right.recorder_id))
                    .expect("highest reply exists");
                progress.step = highest;
                if let Some(proposal) = &caught_up.first_current {
                    progress.proposal = proposal.clone();
                }
                self.ensure_progress_command(&mut progress)?;
                return Ok(DriveOutcome::Progress(progress));
            }
        }
        replies.retain(|reply| reply.step == progress.step);
        replies.sort_by(|left, right| left.recorder_id.cmp(&right.recorder_id));
        replies.dedup_by(|left, right| left.recorder_id == right.recorder_id);
        if replies.len() < self.membership.quorum_size() {
            return Ok(DriveOutcome::Pending(progress));
        }
        replies.truncate(self.membership.quorum_size());
        let summaries: Vec<_> = replies
            .iter()
            .map(|reply| RecorderSummary {
                recorder_id: reply.recorder_id.clone(),
                slot: reply.slot,
                step: reply.step,
                first_current: reply.first_current.clone(),
                aggregate_prior: reply.aggregate_prior.clone(),
            })
            .collect();
        match phase {
            0 => {
                let fast_proposal = summaries
                    .first()
                    .and_then(|summary| summary.first_current.as_ref())
                    .filter(|proposal| proposal.priority == ProposalPriority::MAX)
                    .filter(|proposal| {
                        progress.step == 4
                            && summaries.iter().all(|summary| {
                                summary
                                    .first_current
                                    .as_ref()
                                    .is_some_and(|candidate| proposal_exact(candidate, proposal))
                            })
                    })
                    .cloned();
                if let Some(proposal) = fast_proposal {
                    let proof = DecisionProof::FastPath {
                        cluster_id: self.cluster_id.clone(),
                        slot: progress.slot,
                        epoch: self.epoch,
                        config_id: self.config_id,
                        config_digest: self.config_digest,
                        proposal,
                        summaries,
                    };
                    return self.finish_decision(
                        proof,
                        progress.command.as_ref(),
                        progress.transition_involved,
                    );
                }
                progress.proposal = replies
                    .iter()
                    .filter_map(|reply| reply.first_current.clone())
                    .max()
                    .ok_or(Error::Rejected(RejectReason::InvalidRequest))?;
            }
            1 => {}
            2 => {
                let maximum = replies
                    .iter()
                    .filter_map(|reply| reply.aggregate_prior.clone())
                    .max();
                if maximum.as_ref() == Some(&progress.proposal) {
                    let proof = DecisionProof::Phase2 {
                        cluster_id: self.cluster_id.clone(),
                        slot: progress.slot,
                        epoch: self.epoch,
                        config_id: self.config_id,
                        config_digest: self.config_digest,
                        step: progress.step,
                        proposal: progress.proposal.clone(),
                        summaries,
                    };
                    return self.finish_decision(
                        proof,
                        progress.command.as_ref(),
                        progress.transition_involved,
                    );
                }
            }
            3 => {
                progress.proposal = replies
                    .iter()
                    .filter_map(|reply| reply.aggregate_prior.clone())
                    .max()
                    .ok_or(Error::Rejected(RejectReason::InvalidRequest))?;
            }
            _ => unreachable!("phase is step modulo four"),
        }
        self.ensure_progress_command(&mut progress)?;
        progress.step = progress.step.checked_add(1).ok_or(Error::ProposeFailed)?;
        Ok(DriveOutcome::Progress(progress))
    }

    fn finish_decision(
        &self,
        proof: DecisionProof,
        known_command: Option<&StoredCommand>,
        transition_involved: bool,
    ) -> Result<DriveOutcome> {
        proof
            .validate_for_cluster(
                &self.cluster_id,
                proof_context(&proof).0,
                self.epoch,
                self.config_id,
                &self.membership,
            )
            .map_err(Error::Rejected)?;
        let value = proof
            .proposal()
            .value
            .as_ref()
            .ok_or(Error::Rejected(RejectReason::InvalidCertificate))?;
        let command = match known_command {
            Some(command)
                if self.command_matches_value(proof_context(&proof).0, value, command) =>
            {
                command.clone()
            }
            _ => self
                .fetch_verified_value(proof_context(&proof).0, value)?
                .ok_or(Error::CommandUnavailable)?,
        };
        if command.entry_type != EntryType::ConfigChange && !transition_involved {
            return Ok(DriveOutcome::Decision(proof));
        }
        self.install_decision_proof_quorum(proof.clone())?;
        Ok(DriveOutcome::Decision(proof))
    }

    fn install_decision_proof_quorum(&self, proof: DecisionProof) -> Result<()> {
        let membership = self.membership.clone();
        let quorum = membership.quorum_size();
        let (sender, receiver) = std::sync::mpsc::channel();
        let mut handles = Vec::with_capacity(self.recorders.len());
        for recorder in &self.recorders {
            let recorder = Arc::clone(recorder);
            let sender = sender.clone();
            let proof = proof.clone();
            let membership = membership.clone();
            handles.push(thread::spawn(move || {
                let installed = recorder.install_decision_proof(proof, &membership).is_ok();
                let _ = sender.send(installed);
            }));
        }
        drop(sender);
        let mut installed = 0;
        for success in receiver {
            installed += usize::from(success);
            if installed >= quorum {
                break;
            }
        }
        self.track_background_threads(handles);
        if installed < quorum {
            return Err(Error::NoQuorum);
        }
        Ok(())
    }

    fn record_broadcast(&self, requests: Vec<RecordRequest>) -> Result<Vec<RecordSummary>> {
        let quorum = self.membership.quorum_size();
        let config_id = self.config_id;
        let config_digest = self.config_digest;
        let (sender, receiver) = std::sync::mpsc::channel();
        let mut handles = Vec::with_capacity(self.recorders.len());
        for (index, ((expected_id, recorder), request)) in self
            .membership
            .members()
            .iter()
            .cloned()
            .zip(self.recorders.iter().cloned())
            .zip(requests)
            .enumerate()
        {
            let sender = sender.clone();
            handles.push(thread::spawn(move || {
                let expected_slot = request.slot;
                let reply = recorder.record(request).and_then(|reply| {
                    if reply.recorder_id == expected_id
                        && reply.slot == expected_slot
                        && reply.config_id == config_id
                        && reply.config_digest == config_digest
                    {
                        Ok(reply)
                    } else {
                        Err(Error::Rejected(RejectReason::InvalidRequest))
                    }
                });
                let _ = sender.send((index, reply));
            }));
        }
        drop(sender);
        let total = handles.len();
        let mut completed = 0;
        let mut typed_errors = vec![None; total];
        let mut replies = Vec::with_capacity(quorum);
        for (index, result) in receiver {
            completed += 1;
            match result {
                Ok(reply) => {
                    if !replies
                        .iter()
                        .any(|seen: &RecordSummary| seen.recorder_id == reply.recorder_id)
                    {
                        replies.push(reply);
                    }
                    if replies.len() >= quorum {
                        self.track_background_threads(handles);
                        return Ok(replies);
                    }
                }
                Err(error @ Error::Rejected(_)) | Err(error @ Error::TypedRecordRequired) => {
                    typed_errors[index] = Some(error);
                }
                Err(_) => {}
            }
            if replies.len() + total.saturating_sub(completed) < quorum {
                self.track_background_threads(handles);
                return match typed_errors.into_iter().flatten().next() {
                    Some(error) => Err(error),
                    None => Ok(replies),
                };
            }
        }
        self.track_background_threads(handles);
        match typed_errors.into_iter().flatten().next() {
            Some(error) => Err(error),
            None => Ok(replies),
        }
    }

    pub fn inspect_decision_at(
        &self,
        slot: Slot,
        prev_hash: LogHash,
    ) -> Result<DecisionInspection> {
        Ok(match self.inspect_certified_decision_at(slot, prev_hash)? {
            CertifiedDecisionInspection::Committed(certified) => {
                DecisionInspection::Committed(certified.entry)
            }
            CertifiedDecisionInspection::Empty => DecisionInspection::Empty,
            CertifiedDecisionInspection::Pending => DecisionInspection::Pending,
            CertifiedDecisionInspection::Unavailable => DecisionInspection::Unavailable,
        })
    }

    pub fn inspect_decision_proof_at(&self, slot: Slot) -> Result<Option<DecisionProof>> {
        let quorum = self.membership.quorum_size();
        let (sender, receiver) = std::sync::mpsc::channel();
        let mut handles = Vec::with_capacity(self.recorders.len());
        for (recorder_id, recorder) in self
            .membership
            .members()
            .iter()
            .cloned()
            .zip(self.recorders.iter().cloned())
        {
            let sender = sender.clone();
            handles.push(thread::spawn(move || {
                let _ = sender.send((recorder_id, recorder.inspect_decision_proof(slot)));
            }));
        }
        drop(sender);
        let mut successful = BTreeSet::new();
        let mut proofs = Vec::new();
        for (recorder_id, result) in receiver {
            if let Ok(proof) = result {
                successful.insert(recorder_id);
                proofs.extend(proof);
                if successful.len() >= quorum {
                    break;
                }
            }
        }
        self.track_background_threads(handles);
        if successful.len() < quorum {
            return Err(Error::NoQuorum);
        }
        for proof in &proofs {
            if proof_cluster_id(proof) != self.cluster_id {
                return Err(Error::Rejected(RejectReason::WrongCluster));
            }
            proof
                .validate_for_cluster(
                    &self.cluster_id,
                    slot,
                    self.epoch,
                    self.config_id,
                    &self.membership,
                )
                .map_err(Error::Rejected)?;
        }
        let Some(first) = proofs.first() else {
            return Ok(None);
        };
        if proofs
            .iter()
            .skip(1)
            .any(|proof| proof.proposal().value != first.proposal().value)
        {
            return Err(Error::ConflictingCertificates);
        }
        proofs.sort_by_key(|proof| match proof {
            DecisionProof::FastPath { .. } => 4,
            DecisionProof::Phase2 { step, .. } => *step,
        });
        Ok(proofs.pop())
    }

    pub fn inspect_certified_decision_at(
        &self,
        slot: Slot,
        prev_hash: LogHash,
    ) -> Result<CertifiedDecisionInspection> {
        if let Some(proof) = self.inspect_decision_proof_at(slot)? {
            let decision = certificate_from_proof(&proof)?;
            self.ensure_predecessor(slot, prev_hash, decision.value.prev_hash)?;
            let Some(command) = self.fetch_verified_value(slot, &decision.value)? else {
                return Ok(CertifiedDecisionInspection::Unavailable);
            };
            if command.entry_type == EntryType::ConfigChange {
                self.install_decision_proof_quorum(proof.clone())?;
            }
            let entry = self.log_entry_from_value(slot, command, &decision.value)?;
            return Ok(CertifiedDecisionInspection::Committed(Box::new(
                CertifiedDecision {
                    entry,
                    certificate: decision,
                    proof,
                },
            )));
        }
        if self
            .recorders
            .iter()
            .all(|recorder| recorder.uses_typed_protocol())
        {
            return self.inspect_typed_record_summaries(slot, prev_hash);
        }
        // Compatibility inspection may classify an undecided legacy slot, but
        // never supplies a production decision or certificate.
        let quorum = self.membership.quorum_size();
        let request = RecorderRequest::Inspect {
            cluster_id: self.cluster_id.clone(),
            epoch: self.epoch,
            config_id: self.config_id,
            config_digest: self.config_digest,
            slot,
        };
        let config_id = self.config_id;
        let config_digest = self.config_digest;
        let (sender, receiver) = std::sync::mpsc::channel();
        for (expected_id, recorder) in self
            .membership
            .members()
            .iter()
            .cloned()
            .zip(self.recorders.iter().cloned())
        {
            let sender = sender.clone();
            let request = request.clone();
            thread::spawn(move || {
                let reply = recorder.call(request).ok().filter(|reply| {
                    reply.recorder_id == expected_id
                        && reply.slot == slot
                        && reply.config_id == config_id
                        && reply.config_digest == config_digest
                });
                let _ = sender.send(reply);
            });
        }
        drop(sender);
        let mut replies = Vec::with_capacity(quorum);
        for reply in receiver.into_iter().flatten() {
            replies.push(reply);
            if replies.len() >= quorum {
                break;
            }
        }
        if replies.len() < quorum {
            return Ok(CertifiedDecisionInspection::Unavailable);
        }
        if replies
            .iter()
            .any(|reply| reply.highest_promised.is_some() || reply.accepted.is_some())
        {
            Ok(CertifiedDecisionInspection::Pending)
        } else {
            Ok(CertifiedDecisionInspection::Empty)
        }
    }

    fn inspect_typed_record_summaries(
        &self,
        slot: Slot,
        prev_hash: LogHash,
    ) -> Result<CertifiedDecisionInspection> {
        let quorum = self.membership.quorum_size();
        let config_id = self.config_id;
        let config_digest = self.config_digest;
        let (sender, receiver) = std::sync::mpsc::channel();
        let mut handles = Vec::with_capacity(self.recorders.len());
        for (expected_id, recorder) in self
            .membership
            .members()
            .iter()
            .cloned()
            .zip(self.recorders.iter().cloned())
        {
            let sender = sender.clone();
            handles.push(thread::spawn(move || {
                let summary = recorder.inspect_record_summary(slot).and_then(|summary| {
                    if summary.as_ref().is_none_or(|summary| {
                        summary.recorder_id == expected_id
                            && summary.slot == slot
                            && summary.config_id == config_id
                            && summary.config_digest == config_digest
                    }) {
                        Ok(summary)
                    } else {
                        Err(Error::Rejected(RejectReason::InvalidRequest))
                    }
                });
                let _ = sender.send(summary);
            }));
        }
        drop(sender);
        let mut successful = 0;
        let mut summaries = Vec::new();
        for summary in receiver.into_iter().flatten() {
            successful += 1;
            summaries.extend(summary);
            if successful >= quorum {
                break;
            }
        }
        self.track_background_threads(handles);
        if successful < quorum {
            return Ok(CertifiedDecisionInspection::Unavailable);
        }
        if summaries.is_empty() {
            return Ok(CertifiedDecisionInspection::Empty);
        }
        summaries.sort_by(|left, right| left.recorder_id.cmp(&right.recorder_id));
        summaries.dedup_by(|left, right| left.recorder_id == right.recorder_id);
        for step in summaries
            .iter()
            .map(|summary| summary.step)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .rev()
        {
            let mut step_summaries: Vec<_> = summaries
                .iter()
                .filter(|summary| summary.step == step)
                .cloned()
                .collect();
            if step_summaries.len() < quorum {
                continue;
            }
            step_summaries.truncate(quorum);
            let proof_summaries: Vec<_> = step_summaries
                .iter()
                .map(|summary| RecorderSummary {
                    recorder_id: summary.recorder_id.clone(),
                    slot: summary.slot,
                    step: summary.step,
                    first_current: summary.first_current.clone(),
                    aggregate_prior: summary.aggregate_prior.clone(),
                })
                .collect();
            let proof = if step == 4 {
                step_summaries
                    .first()
                    .and_then(|summary| summary.first_current.clone())
                    .filter(|proposal| proposal.priority == ProposalPriority::MAX)
                    .filter(|proposal| {
                        step_summaries.iter().all(|summary| {
                            summary
                                .first_current
                                .as_ref()
                                .is_some_and(|candidate| proposal_exact(candidate, proposal))
                        })
                    })
                    .map(|proposal| DecisionProof::FastPath {
                        cluster_id: self.cluster_id.clone(),
                        slot,
                        epoch: self.epoch,
                        config_id: self.config_id,
                        config_digest: self.config_digest,
                        proposal,
                        summaries: proof_summaries.clone(),
                    })
            } else {
                None
            };
            let Some(proof) = proof else {
                continue;
            };
            if proof
                .validate_for_cluster(
                    &self.cluster_id,
                    slot,
                    self.epoch,
                    self.config_id,
                    &self.membership,
                )
                .is_err()
            {
                continue;
            }
            let decision = certificate_from_proof(&proof)?;
            self.ensure_predecessor(slot, prev_hash, decision.value.prev_hash)?;
            let Some(command) = self.fetch_verified_value(slot, &decision.value)? else {
                return Ok(CertifiedDecisionInspection::Unavailable);
            };
            if command.entry_type == EntryType::ConfigChange {
                self.install_decision_proof_quorum(proof.clone())?;
            }
            let entry = self.log_entry_from_value(slot, command, &decision.value)?;
            return Ok(CertifiedDecisionInspection::Committed(Box::new(
                CertifiedDecision {
                    entry,
                    certificate: decision,
                    proof,
                },
            )));
        }
        Ok(CertifiedDecisionInspection::Pending)
    }

    pub fn recover_decision_at(
        &self,
        slot: Slot,
        prev_hash: LogHash,
    ) -> Result<DecisionInspection> {
        match self.inspect_decision_at(slot, prev_hash)? {
            DecisionInspection::Pending => self
                .propose_stored_at(
                    slot,
                    prev_hash,
                    StoredCommand::new(EntryType::Noop, Vec::new()),
                )
                .map(DecisionInspection::Committed),
            inspection => Ok(inspection),
        }
    }

    pub fn recover_decided_at(&self, slot: Slot, prev_hash: LogHash) -> Result<Option<LogEntry>> {
        match self.inspect_decision_at(slot, prev_hash)? {
            DecisionInspection::Committed(entry) => Ok(Some(entry)),
            DecisionInspection::Empty | DecisionInspection::Pending => Ok(None),
            DecisionInspection::Unavailable => Err(Error::CommandUnavailable),
        }
    }

    pub fn recover_decided_next(&self) -> Result<Option<LogEntry>> {
        let mut tip = self.legacy_tip.lock().map_err(|_| Error::ProposeFailed)?;
        let Some(entry) = self.recover_decided_at(tip.next_index, tip.last_hash)? else {
            return Ok(None);
        };
        tip.next_index = entry
            .index
            .checked_add(1)
            .ok_or(Error::InvalidRecoveredTip)?;
        tip.last_hash = entry.hash;
        Ok(Some(entry))
    }

    fn ensure_predecessor(
        &self,
        slot: Slot,
        actual_prev_hash: LogHash,
        expected_prev_hash: LogHash,
    ) -> Result<()> {
        if actual_prev_hash != expected_prev_hash {
            return Err(Error::ChainConflict {
                slot,
                expected_prev_hash,
                actual_prev_hash,
            });
        }
        Ok(())
    }

    fn store_command_on_quorum(
        &self,
        command_hash: LogHash,
        command: &StoredCommand,
    ) -> Result<()> {
        let quorum = quorum_size(self.recorders.len());
        let (sender, receiver) = std::sync::mpsc::channel();
        for recorder in &self.recorders {
            let recorder = Arc::clone(recorder);
            let command = command.clone();
            let cluster_id = self.cluster_id.clone();
            let epoch = self.epoch;
            let config_id = self.config_id;
            let config_digest = self.config_digest;
            let sender = sender.clone();
            thread::spawn(move || {
                let stored = recorder
                    .store_command_for(
                        cluster_id,
                        epoch,
                        config_id,
                        config_digest,
                        command_hash,
                        command,
                    )
                    .is_ok();
                let _ = sender.send(stored);
            });
        }
        drop(sender);
        let mut stored = 0;
        for success in receiver {
            stored += usize::from(success);
            if stored >= quorum {
                break;
            }
        }
        if stored < quorum {
            return Err(Error::NoQuorum);
        }
        Ok(())
    }

    fn fetch_verified_value(
        &self,
        slot: Slot,
        value: &AcceptedValue,
    ) -> Result<Option<StoredCommand>> {
        let (sender, receiver) = std::sync::mpsc::channel();
        let mut handles = Vec::with_capacity(self.recorders.len());
        for recorder in &self.recorders {
            let recorder = Arc::clone(recorder);
            let sender = sender.clone();
            let cluster_id = self.cluster_id.clone();
            let command_hash = value.command_hash;
            let epoch = self.epoch;
            let config_id = self.config_id;
            let config_digest = self.config_digest;
            handles.push(thread::spawn(move || {
                let fetched = recorder.fetch_command_for(
                    cluster_id,
                    epoch,
                    config_id,
                    config_digest,
                    command_hash,
                );
                let _ = sender.send(fetched);
            }));
        }
        drop(sender);
        let mut mismatch = false;
        for command in receiver.into_iter().filter_map(Result::ok).flatten() {
            if command.hash() != value.command_hash {
                continue;
            }
            let expected = AcceptedValue::from_command(
                &self.cluster_id,
                slot,
                self.epoch,
                self.config_id,
                value.prev_hash,
                &command,
            );
            if expected == *value {
                self.track_background_threads(handles);
                return Ok(Some(command));
            }
            mismatch = true;
        }
        self.track_background_threads(handles);
        if mismatch {
            Err(Error::Rejected(RejectReason::InvalidValue))
        } else {
            Ok(None)
        }
    }

    fn track_background_threads(&self, handles: Vec<thread::JoinHandle<()>>) {
        let Ok(mut background) = self.background_threads.lock() else {
            return;
        };
        let mut index = 0;
        while index < background.len() {
            if background[index].is_finished() {
                let _ = background.swap_remove(index).join();
            } else {
                index += 1;
            }
        }
        background.extend(handles);
    }

    fn ensure_progress_command(&self, progress: &mut ProposerProgress) -> Result<()> {
        let value = progress
            .proposal
            .value
            .as_ref()
            .ok_or(Error::Rejected(RejectReason::InvalidRequest))?;
        if progress
            .command
            .as_ref()
            .is_some_and(|command| self.command_matches_value(progress.slot, value, command))
        {
            return Ok(());
        }
        progress.command_holders.clear();
        progress.command = self.fetch_verified_value(progress.slot, value)?;
        if let Some(command) = &progress.command {
            progress.transition_involved |= command.entry_type == EntryType::ConfigChange;
            Ok(())
        } else {
            Err(Error::CommandUnavailable)
        }
    }

    fn command_matches_value(
        &self,
        slot: Slot,
        value: &AcceptedValue,
        command: &StoredCommand,
    ) -> bool {
        command.hash() == value.command_hash
            && AcceptedValue::from_command(
                &self.cluster_id,
                slot,
                self.epoch,
                self.config_id,
                value.prev_hash,
                command,
            ) == *value
    }

    fn log_entry_from_value(
        &self,
        slot: Slot,
        command: StoredCommand,
        value: &AcceptedValue,
    ) -> Result<LogEntry> {
        let entry = LogEntry {
            cluster_id: self.cluster_id.clone(),
            epoch: self.epoch,
            config_id: self.config_id,
            index: slot,
            entry_type: command.entry_type,
            payload: command.payload,
            prev_hash: value.prev_hash,
            hash: value.entry_hash,
        };
        if entry.recompute_hash() != entry.hash {
            return Err(Error::Rejected(RejectReason::InvalidValue));
        }
        Ok(entry)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecisionInspection {
    Committed(LogEntry),
    Empty,
    Pending,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CertifiedDecision {
    pub entry: LogEntry,
    pub certificate: DecisionCertificate,
    pub proof: DecisionProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CertifiedDecisionInspection {
    Empty,
    Pending,
    Committed(Box<CertifiedDecision>),
    Unavailable,
}

impl RecorderRpc for RecorderFileStore {
    fn call(&self, request: RecorderRequest) -> Result<RecorderReply> {
        self.apply(request)
    }

    fn recorder_id(&self) -> Result<NodeId> {
        Ok(self.recorder_id.clone())
    }

    fn record(&self, request: RecordRequest) -> Result<RecordSummary> {
        self.record_proposal(request)
    }

    fn install_decision_proof(&self, proof: DecisionProof, membership: &Membership) -> Result<()> {
        self.install_decision_proof_record(proof, membership)
    }

    fn inspect_decision_proof(&self, slot: Slot) -> Result<Option<DecisionProof>> {
        Ok(self.load(slot)?.decision_proof().cloned())
    }

    fn inspect_record_summary(&self, slot: Slot) -> Result<Option<RecordSummary>> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        let configuration = self.configuration_state()?;
        if !self.path(slot).exists() {
            return Ok(None);
        }
        let state = self.load_unlocked(slot, configuration.config_digest)?;
        Ok(Some(record_summary(
            &self.recorder_id,
            &state,
            state.decision_proof().cloned(),
        )))
    }

    fn uses_typed_protocol(&self) -> bool {
        true
    }

    fn store_command(&self, command_hash: LogHash, command: StoredCommand) -> Result<()> {
        RecorderFileStore::store_command(self, command_hash, command)
    }

    fn fetch_command(&self, command_hash: LogHash) -> Result<Option<StoredCommand>> {
        RecorderFileStore::fetch_command(self, command_hash)
    }
}

fn proposal_ballot(proposal: &Proposal) -> Option<Ballot> {
    proposal.value.as_ref()?;
    Some(Ballot::new(
        proposal.proposal_id,
        proposal.priority.legacy_u128(),
        proposal.proposer_id.clone(),
    ))
}

fn record_summary(
    recorder_id: &str,
    state: &RecorderSlotState,
    decided: Option<DecisionProof>,
) -> RecordSummary {
    RecordSummary {
        recorder_id: recorder_id.to_string(),
        slot: state.slot,
        config_id: state.config_id,
        config_digest: state.config_digest,
        step: state.isr.step(),
        first_current: state.isr.first_current().cloned(),
        aggregate_prior: state.isr.aggregate_prior().cloned(),
        decided,
    }
}

fn proof_context(proof: &DecisionProof) -> (Slot, Epoch, ConfigId, LogHash) {
    match proof {
        DecisionProof::FastPath {
            slot,
            epoch,
            config_id,
            config_digest,
            ..
        }
        | DecisionProof::Phase2 {
            slot,
            epoch,
            config_id,
            config_digest,
            ..
        } => (*slot, *epoch, *config_id, *config_digest),
    }
}

fn proof_cluster_id(proof: &DecisionProof) -> &str {
    match proof {
        DecisionProof::FastPath { cluster_id, .. } | DecisionProof::Phase2 { cluster_id, .. } => {
            cluster_id
        }
    }
}

fn certificate_from_proof(proof: &DecisionProof) -> Result<DecisionCertificate> {
    let (slot, epoch, config_id, config_digest) = proof_context(proof);
    let proposal = proof.proposal();
    let value = proposal
        .value
        .clone()
        .ok_or(Error::Rejected(RejectReason::InvalidCertificate))?;
    let recorder_ids = match proof {
        DecisionProof::FastPath { summaries, .. } | DecisionProof::Phase2 { summaries, .. } => {
            summaries
                .iter()
                .map(|summary| summary.recorder_id.clone())
                .collect()
        }
    };
    Ok(DecisionCertificate {
        slot,
        epoch,
        config_id,
        config_digest,
        ballot: Ballot::new(
            proposal.proposal_id,
            proposal.priority.legacy_u128(),
            encode_certificate_proposer(proof_cluster_id(proof), &proposal.proposer_id),
        ),
        value,
        recorder_ids,
    })
}

const CERTIFICATE_PROPOSER_PREFIX: &str = "QDC1:";

fn encode_certificate_proposer(cluster_id: &str, proposer_id: &str) -> String {
    format!(
        "{CERTIFICATE_PROPOSER_PREFIX}{}:{cluster_id}{proposer_id}",
        cluster_id.len()
    )
}

fn decode_certificate_proposer(encoded: &str) -> Option<(&str, &str)> {
    let rest = encoded.strip_prefix(CERTIFICATE_PROPOSER_PREFIX)?;
    let (length, joined) = rest.split_once(':')?;
    let length: usize = length.parse().ok()?;
    Some((joined.get(..length)?, joined.get(length..)?))
}

impl Consensus for ThreeNodeConsensus {
    fn propose(&self, command: Command) -> Result<LogEntry> {
        self.propose_with_priority(command, Priority::MAX)
    }
}

fn request_slot(request: &RecorderRequest) -> Option<Slot> {
    match request {
        RecorderRequest::Inspect { slot, .. }
        | RecorderRequest::Observe { slot, .. }
        | RecorderRequest::Converge { slot, .. }
        | RecorderRequest::Decide { slot, .. } => Some(*slot),
        RecorderRequest::Identity
        | RecorderRequest::StoreCommand { .. }
        | RecorderRequest::FetchCommand { .. } => None,
    }
}

fn request_context(request: &RecorderRequest) -> Option<(&ClusterId, Epoch, ConfigId, LogHash)> {
    match request {
        RecorderRequest::StoreCommand {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            ..
        }
        | RecorderRequest::FetchCommand {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            ..
        }
        | RecorderRequest::Inspect {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            ..
        }
        | RecorderRequest::Observe {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            ..
        }
        | RecorderRequest::Converge {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            ..
        }
        | RecorderRequest::Decide {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            ..
        } => Some((cluster_id, *epoch, *config_id, *config_digest)),
        _ => None,
    }
}

fn stored_command(command: Command) -> Result<StoredCommand> {
    let entry_type = match command.kind() {
        CommandKind::Deterministic => EntryType::Command,
        CommandKind::ReadBarrier => EntryType::Noop,
    };
    Ok(StoredCommand::new(entry_type, command.payload().to_vec()))
}

fn encode_configuration_state(state: &ConfigurationState) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(b"QCON");
    put_u16(&mut out, CONFIGURATION_STATE_VERSION);
    put_u64(&mut out, state.config_id);
    out.extend_from_slice(state.config_digest.as_bytes());
    out.push(u8::from(state.activated));
    match state.max_accepted_or_decided_slot {
        Some(slot) => {
            out.push(1);
            put_u64(&mut out, slot);
        }
        None => out.push(0),
    }
    match &state.membership {
        Some(membership) => {
            out.push(membership.members().len() as u8);
            for member in membership.members() {
                put_bytes(&mut out, member.as_bytes())?;
            }
        }
        None => out.push(0),
    }
    encode_optional_seal(&mut out, state.predecessor.as_ref());
    encode_optional_seal(&mut out, state.seal.as_ref());
    let digest = LogHash::digest(&[&out]);
    out.extend_from_slice(digest.as_bytes());
    Ok(out)
}

fn decode_configuration_state(bytes: &[u8]) -> Result<ConfigurationState> {
    if bytes.len() < 4 + 2 + 32 || bytes.get(..4) != Some(b"QCON") {
        return Err(Error::Decode("invalid configuration state".into()));
    }
    let (body, digest) = bytes.split_at(bytes.len() - 32);
    if LogHash::digest(&[body]).as_bytes() != digest {
        return Err(Error::Decode("configuration digest mismatch".into()));
    }
    let mut cursor = 4;
    let version = read_u16(body, &mut cursor)?;
    if version != CONFIGURATION_STATE_VERSION {
        return Err(Error::MigrationRequired {
            format: "QCON",
            version,
        });
    }
    let config_id = read_u64(body, &mut cursor)?;
    let config_digest = read_hash(body, &mut cursor)?;
    let activated = match read_u8(body, &mut cursor)? {
        0 => false,
        1 => true,
        _ => return Err(Error::Decode("invalid activation flag".into())),
    };
    let max_accepted_or_decided_slot = match read_u8(body, &mut cursor)? {
        0 => None,
        1 => Some(read_u64(body, &mut cursor)?),
        _ => return Err(Error::Decode("invalid accepted-slot flag".into())),
    };
    let member_count = read_u8(body, &mut cursor)? as usize;
    let membership = if member_count == 0 {
        None
    } else {
        let members = (0..member_count)
            .map(|_| {
                String::from_utf8(read_bytes(body, &mut cursor)?)
                    .map_err(|err| Error::Decode(err.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        let membership = Membership::from_voters(members)?;
        if membership.digest() != config_digest {
            return Err(Error::Decode("membership digest mismatch".into()));
        }
        Some(membership)
    };
    let predecessor = decode_optional_seal(body, &mut cursor)?;
    let seal = decode_optional_seal(body, &mut cursor)?;
    if cursor != body.len() {
        return Err(Error::Decode("trailing configuration bytes".into()));
    }
    Ok(ConfigurationState {
        config_id,
        config_digest,
        membership,
        predecessor,
        seal,
        max_accepted_or_decided_slot,
        activated,
    })
}

fn encode_optional_seal(out: &mut Vec<u8>, seal: Option<&ConfigurationSeal>) {
    match seal {
        Some(seal) => {
            out.push(1);
            put_u64(out, seal.stop_slot);
            out.extend_from_slice(seal.command_hash.as_bytes());
            out.extend_from_slice(seal.prefix_hash.as_bytes());
        }
        None => out.push(0),
    }
}

fn decode_optional_seal(bytes: &[u8], cursor: &mut usize) -> Result<Option<ConfigurationSeal>> {
    match read_u8(bytes, cursor)? {
        0 => Ok(None),
        1 => Ok(Some(ConfigurationSeal {
            stop_slot: read_u64(bytes, cursor)?,
            command_hash: read_hash(bytes, cursor)?,
            prefix_hash: read_hash(bytes, cursor)?,
        })),
        _ => Err(Error::Decode("invalid configuration seal flag".into())),
    }
}

fn encode_transition_intent(
    slot: Slot,
    slot_bytes: &[u8],
    configuration_bytes: &[u8],
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(b"QINT");
    put_u16(&mut out, 1);
    put_u64(&mut out, slot);
    put_bytes(&mut out, slot_bytes)?;
    put_bytes(&mut out, configuration_bytes)?;
    let digest = LogHash::digest(&[&out]);
    out.extend_from_slice(digest.as_bytes());
    Ok(out)
}

fn decode_transition_intent(bytes: &[u8]) -> Result<(Slot, Vec<u8>, Vec<u8>)> {
    if bytes.len() < 4 + 2 + 32 || bytes.get(..4) != Some(b"QINT") {
        return Err(Error::Decode("invalid transition intent".into()));
    }
    let (body, digest) = bytes.split_at(bytes.len() - 32);
    if LogHash::digest(&[body]).as_bytes() != digest {
        return Err(Error::Decode("transition intent digest mismatch".into()));
    }
    let mut cursor = 4;
    if read_u16(body, &mut cursor)? != 1 {
        return Err(Error::Decode(
            "unsupported transition intent version".into(),
        ));
    }
    let slot = read_u64(body, &mut cursor)?;
    let slot_bytes = read_bytes(body, &mut cursor)?;
    let configuration_bytes = read_bytes(body, &mut cursor)?;
    if cursor != body.len() {
        return Err(Error::Decode("trailing transition intent bytes".into()));
    }
    Ok((slot, slot_bytes, configuration_bytes))
}

fn encode_recorder_state(state: &RecorderSlotState) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(b"QREC");
    put_u16(&mut out, RECORDER_STATE_VERSION);
    put_u64(&mut out, state.slot);
    put_u64(&mut out, state.epoch);
    put_u64(&mut out, state.config_id);
    out.extend_from_slice(state.config_digest.as_bytes());
    put_bytes(&mut out, state.cluster_id.as_bytes())?;
    encode_optional_ballot(&mut out, state.highest_promised.as_ref())?;
    match &state.accepted {
        Some(accepted) => {
            out.push(1);
            encode_ballot(&mut out, &accepted.ballot)?;
            encode_value(&mut out, &accepted.value);
        }
        None => out.push(0),
    }
    match &state.decided {
        Some(decided) => {
            out.push(1);
            encode_certificate(&mut out, decided)?;
        }
        None => out.push(0),
    }
    put_u64(&mut out, state.isr.step);
    encode_optional_proposal(&mut out, state.isr.first_current.as_ref())?;
    encode_optional_proposal(&mut out, state.isr.aggregate_current.as_ref())?;
    encode_optional_proposal(&mut out, state.isr.aggregate_prior.as_ref())?;
    encode_optional_proof(&mut out, state.decided_proof.as_ref())?;
    let digest = LogHash::digest(&[&out]);
    out.extend_from_slice(digest.as_bytes());
    Ok(out)
}

fn decode_recorder_state(bytes: &[u8]) -> Result<RecorderSlotState> {
    if bytes.len() < 4 + 2 + 32 || &bytes[..4] != b"QREC" {
        return Err(Error::Decode("invalid recorder magic".into()));
    }
    let (body, digest) = bytes.split_at(bytes.len() - 32);
    if LogHash::digest(&[body]).as_bytes() != digest {
        return Err(Error::Decode("recorder digest mismatch".into()));
    }
    let mut cursor = 4;
    let version = read_u16(body, &mut cursor)?;
    if version != RECORDER_STATE_VERSION {
        return Err(Error::MigrationRequired {
            format: "QREC",
            version,
        });
    }
    let slot = read_u64(body, &mut cursor)?;
    let epoch = read_u64(body, &mut cursor)?;
    let config_id = read_u64(body, &mut cursor)?;
    let config_digest = read_hash(body, &mut cursor)?;
    let cluster_id = String::from_utf8(read_bytes(body, &mut cursor)?)
        .map_err(|err| Error::Decode(err.to_string()))?;
    let highest_promised = decode_optional_ballot(body, &mut cursor)?;
    let accepted = match read_u8(body, &mut cursor)? {
        0 => None,
        1 => Some(AcceptedSummary {
            ballot: decode_ballot(body, &mut cursor)?,
            value: decode_value(body, &mut cursor)?,
        }),
        _ => return Err(Error::Decode("invalid accepted flag".into())),
    };
    let decided = match read_u8(body, &mut cursor)? {
        0 => None,
        1 => Some(decode_certificate(body, &mut cursor)?),
        _ => return Err(Error::Decode("invalid decided flag".into())),
    };
    let isr = IsrState {
        step: read_u64(body, &mut cursor)?,
        first_current: decode_optional_proposal(body, &mut cursor)?,
        aggregate_current: decode_optional_proposal(body, &mut cursor)?,
        aggregate_prior: decode_optional_proposal(body, &mut cursor)?,
    };
    let decided_proof = decode_optional_proof(body, &mut cursor)?;
    if cursor != body.len() {
        return Err(Error::Decode("trailing recorder bytes".into()));
    }
    if let Some(accepted) = &accepted {
        if highest_promised.as_ref() < Some(&accepted.ballot) {
            return Err(Error::Decode("accepted ballot exceeds promise".into()));
        }
    }
    if let Some(decided) = &decided {
        decided
            .validate_context(slot, epoch, config_id, config_digest)
            .map_err(|_| Error::Decode("invalid decision certificate".into()))?;
    }
    if let Some(proof) = &decided_proof {
        let proof_value = proof.proposal().value.as_ref();
        if proof_context(proof) != (slot, epoch, config_id, config_digest)
            || proof_cluster_id(proof) != cluster_id
            || !matches!((decided.as_ref(), proof_value), (Some(certificate), Some(value)) if &certificate.value == value)
        {
            return Err(Error::Decode("invalid persisted decision proof".into()));
        }
    }
    Ok(RecorderSlotState {
        slot,
        cluster_id,
        epoch,
        config_id,
        config_digest,
        highest_promised,
        accepted,
        decided,
        isr,
        decided_proof,
    })
}

fn encode_optional_proposal(out: &mut Vec<u8>, proposal: Option<&Proposal>) -> Result<()> {
    match proposal {
        None => out.push(0),
        Some(proposal) => {
            out.push(1);
            out.extend_from_slice(&proposal.priority.0);
            put_bytes(out, proposal.proposer_id.as_bytes())?;
            put_u64(out, proposal.proposal_id);
            match &proposal.value {
                None => out.push(0),
                Some(value) => {
                    out.push(1);
                    encode_value(out, value);
                }
            }
        }
    }
    Ok(())
}

fn decode_optional_proposal(bytes: &[u8], cursor: &mut usize) -> Result<Option<Proposal>> {
    match read_u8(bytes, cursor)? {
        0 => Ok(None),
        1 => {
            let end = cursor
                .checked_add(32)
                .ok_or_else(|| Error::Decode("priority overflow".into()))?;
            let priority = ProposalPriority(
                bytes
                    .get(*cursor..end)
                    .ok_or_else(|| Error::Decode("truncated priority".into()))?
                    .try_into()
                    .expect("checked priority length"),
            );
            *cursor = end;
            let proposer_id = String::from_utf8(read_bytes(bytes, cursor)?)
                .map_err(|error| Error::Decode(error.to_string()))?;
            let proposal_id = read_u64(bytes, cursor)?;
            let value = match read_u8(bytes, cursor)? {
                0 => None,
                1 => Some(decode_value(bytes, cursor)?),
                _ => return Err(Error::Decode("invalid proposal value flag".into())),
            };
            Ok(Some(Proposal {
                priority,
                proposer_id,
                proposal_id,
                value,
            }))
        }
        _ => Err(Error::Decode("invalid proposal flag".into())),
    }
}

fn encode_summary(out: &mut Vec<u8>, summary: &RecorderSummary) -> Result<()> {
    put_bytes(out, summary.recorder_id.as_bytes())?;
    put_u64(out, summary.slot);
    put_u64(out, summary.step);
    encode_optional_proposal(out, summary.first_current.as_ref())?;
    encode_optional_proposal(out, summary.aggregate_prior.as_ref())
}

fn decode_summary(bytes: &[u8], cursor: &mut usize) -> Result<RecorderSummary> {
    Ok(RecorderSummary {
        recorder_id: String::from_utf8(read_bytes(bytes, cursor)?)
            .map_err(|error| Error::Decode(error.to_string()))?,
        slot: read_u64(bytes, cursor)?,
        step: read_u64(bytes, cursor)?,
        first_current: decode_optional_proposal(bytes, cursor)?,
        aggregate_prior: decode_optional_proposal(bytes, cursor)?,
    })
}

fn encode_optional_proof(out: &mut Vec<u8>, proof: Option<&DecisionProof>) -> Result<()> {
    let Some(proof) = proof else {
        out.push(0);
        return Ok(());
    };
    let (tag, cluster_id, slot, epoch, config_id, digest, step, proposal, summaries) = match proof {
        DecisionProof::FastPath {
            cluster_id,
            slot,
            epoch,
            config_id,
            config_digest,
            proposal,
            summaries,
        } => (
            1,
            cluster_id,
            *slot,
            *epoch,
            *config_id,
            *config_digest,
            4,
            proposal,
            summaries,
        ),
        DecisionProof::Phase2 {
            cluster_id,
            slot,
            epoch,
            config_id,
            config_digest,
            step,
            proposal,
            summaries,
        } => (
            2,
            cluster_id,
            *slot,
            *epoch,
            *config_id,
            *config_digest,
            *step,
            proposal,
            summaries,
        ),
    };
    out.push(tag);
    put_bytes(out, cluster_id.as_bytes())?;
    put_u64(out, slot);
    put_u64(out, epoch);
    put_u64(out, config_id);
    out.extend_from_slice(digest.as_bytes());
    put_u64(out, step);
    encode_optional_proposal(out, Some(proposal))?;
    put_u16(
        out,
        u16::try_from(summaries.len())
            .map_err(|_| Error::Decode("too many proof summaries".into()))?,
    );
    for summary in summaries {
        encode_summary(out, summary)?;
    }
    Ok(())
}

fn decode_optional_proof(bytes: &[u8], cursor: &mut usize) -> Result<Option<DecisionProof>> {
    let tag = read_u8(bytes, cursor)?;
    if tag == 0 {
        return Ok(None);
    }
    let cluster_id = String::from_utf8(read_bytes(bytes, cursor)?)
        .map_err(|error| Error::Decode(error.to_string()))?;
    let slot = read_u64(bytes, cursor)?;
    let epoch = read_u64(bytes, cursor)?;
    let config_id = read_u64(bytes, cursor)?;
    let config_digest = read_hash(bytes, cursor)?;
    let step = read_u64(bytes, cursor)?;
    let proposal = decode_optional_proposal(bytes, cursor)?
        .ok_or_else(|| Error::Decode("nil decision proposal".into()))?;
    let summaries = (0..read_u16(bytes, cursor)? as usize)
        .map(|_| decode_summary(bytes, cursor))
        .collect::<Result<Vec<_>>>()?;
    match tag {
        1 if step == 4 => Ok(Some(DecisionProof::FastPath {
            cluster_id,
            slot,
            epoch,
            config_id,
            config_digest,
            proposal,
            summaries,
        })),
        2 => Ok(Some(DecisionProof::Phase2 {
            cluster_id,
            slot,
            epoch,
            config_id,
            config_digest,
            step,
            proposal,
            summaries,
        })),
        _ => Err(Error::Decode("invalid decision proof tag".into())),
    }
}

fn encode_optional_ballot(out: &mut Vec<u8>, ballot: Option<&Ballot>) -> Result<()> {
    match ballot {
        Some(ballot) => {
            out.push(1);
            encode_ballot(out, ballot)
        }
        None => {
            out.push(0);
            Ok(())
        }
    }
}

fn decode_optional_ballot(bytes: &[u8], cursor: &mut usize) -> Result<Option<Ballot>> {
    match read_u8(bytes, cursor)? {
        0 => Ok(None),
        1 => Ok(Some(decode_ballot(bytes, cursor)?)),
        _ => Err(Error::Decode("invalid ballot flag".into())),
    }
}

fn encode_ballot(out: &mut Vec<u8>, ballot: &Ballot) -> Result<()> {
    put_u64(out, ballot.round);
    put_u128(out, ballot.priority);
    put_bytes(out, ballot.proposer_id.as_bytes())
}

fn decode_ballot(bytes: &[u8], cursor: &mut usize) -> Result<Ballot> {
    Ok(Ballot {
        round: read_u64(bytes, cursor)?,
        priority: read_u128(bytes, cursor)?,
        proposer_id: String::from_utf8(read_bytes(bytes, cursor)?)
            .map_err(|err| Error::Decode(err.to_string()))?,
    })
}

fn encode_value(out: &mut Vec<u8>, value: &AcceptedValue) {
    out.extend_from_slice(value.command_hash.as_bytes());
    out.extend_from_slice(value.prev_hash.as_bytes());
    out.extend_from_slice(value.entry_hash.as_bytes());
}

fn decode_value(bytes: &[u8], cursor: &mut usize) -> Result<AcceptedValue> {
    Ok(AcceptedValue {
        command_hash: read_hash(bytes, cursor)?,
        prev_hash: read_hash(bytes, cursor)?,
        entry_hash: read_hash(bytes, cursor)?,
    })
}

fn encode_certificate(out: &mut Vec<u8>, decision: &DecisionCertificate) -> Result<()> {
    put_u64(out, decision.slot);
    put_u64(out, decision.epoch);
    put_u64(out, decision.config_id);
    out.extend_from_slice(decision.config_digest.as_bytes());
    encode_ballot(out, &decision.ballot)?;
    encode_value(out, &decision.value);
    put_u16(
        out,
        u16::try_from(decision.recorder_ids.len())
            .map_err(|_| Error::Decode("too many certificate recorders".into()))?,
    );
    for recorder_id in &decision.recorder_ids {
        put_bytes(out, recorder_id.as_bytes())?;
    }
    Ok(())
}

fn decode_certificate(bytes: &[u8], cursor: &mut usize) -> Result<DecisionCertificate> {
    let slot = read_u64(bytes, cursor)?;
    let epoch = read_u64(bytes, cursor)?;
    let config_id = read_u64(bytes, cursor)?;
    let config_digest = read_hash(bytes, cursor)?;
    let ballot = decode_ballot(bytes, cursor)?;
    let value = decode_value(bytes, cursor)?;
    let recorder_count = read_u16(bytes, cursor)? as usize;
    let recorder_ids = (0..recorder_count)
        .map(|_| {
            String::from_utf8(read_bytes(bytes, cursor)?)
                .map_err(|err| Error::Decode(err.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(DecisionCertificate {
        slot,
        epoch,
        config_id,
        config_digest,
        ballot,
        value,
        recorder_ids,
    })
}

fn encode_stored_command(command: &StoredCommand) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"QCMD");
    put_u16(&mut out, 1);
    out.push(command.entry_type.as_u8());
    put_u64(&mut out, command.payload.len() as u64);
    out.extend_from_slice(&command.payload);
    let digest = LogHash::digest(&[&out]);
    out.extend_from_slice(digest.as_bytes());
    out
}

fn decode_stored_command(bytes: &[u8]) -> Result<StoredCommand> {
    if bytes.len() < 4 + 2 + 1 + 8 + 32 || &bytes[..4] != b"QCMD" {
        return Err(Error::Decode("invalid command magic".into()));
    }
    let (body, digest) = bytes.split_at(bytes.len() - 32);
    if LogHash::digest(&[body]).as_bytes() != digest {
        return Err(Error::Decode("command digest mismatch".into()));
    }
    let mut cursor = 4;
    if read_u16(body, &mut cursor)? != 1 {
        return Err(Error::Decode("unsupported command version".into()));
    }
    let entry_type = EntryType::from_u8(read_u8(body, &mut cursor)?)
        .ok_or_else(|| Error::Decode("invalid command entry type".into()))?;
    let payload_len = usize::try_from(read_u64(body, &mut cursor)?)
        .map_err(|_| Error::Decode("command payload too large".into()))?;
    let end = cursor
        .checked_add(payload_len)
        .ok_or_else(|| Error::Decode("command payload length overflow".into()))?;
    let payload = body
        .get(cursor..end)
        .ok_or_else(|| Error::Decode("short command payload".into()))?
        .to_vec();
    cursor = end;
    if cursor != body.len() {
        return Err(Error::Decode("trailing command bytes".into()));
    }
    Ok(StoredCommand::new(entry_type, payload))
}

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::Io("atomic write path has no parent".into()))?;
    fs::create_dir_all(parent).map_err(|err| Error::Io(err.to_string()))?;
    let file_name = path
        .file_name()
        .ok_or_else(|| Error::Io("atomic write path has no file name".into()))?
        .to_string_lossy();
    let (temp_path, mut file) = loop {
        let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let temp_path = parent.join(format!(".{file_name}.tmp-{}-{counter}", std::process::id()));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
        {
            Ok(file) => break (temp_path, file),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(Error::Io(err.to_string())),
        }
    };
    let result = (|| -> io::Result<()> {
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp_path, path)?;
        fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if let Err(err) = result {
        let _ = fs::remove_file(&temp_path);
        return Err(Error::Io(err.to_string()));
    }
    Ok(())
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u128(out: &mut Vec<u8>, value: u128) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let len = u16::try_from(value.len())
        .map_err(|_| Error::Decode("recorder string is too long".into()))?;
    put_u16(out, len);
    out.extend_from_slice(value);
    Ok(())
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8> {
    let value = *bytes
        .get(*cursor)
        .ok_or_else(|| Error::Decode("short recorder u8".into()))?;
    *cursor += 1;
    Ok(value)
}

fn read_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16> {
    let end = cursor
        .checked_add(2)
        .ok_or_else(|| Error::Decode("recorder cursor overflow".into()))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::Decode("short recorder u16".into()))?;
    *cursor = end;
    Ok(u16::from_be_bytes(slice.try_into().expect("u16 slice")))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
    let end = cursor
        .checked_add(8)
        .ok_or_else(|| Error::Decode("recorder cursor overflow".into()))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::Decode("short recorder u64".into()))?;
    *cursor = end;
    Ok(u64::from_be_bytes(slice.try_into().expect("u64 slice")))
}

fn read_u128(bytes: &[u8], cursor: &mut usize) -> Result<u128> {
    let end = cursor
        .checked_add(16)
        .ok_or_else(|| Error::Decode("recorder cursor overflow".into()))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::Decode("short recorder u128".into()))?;
    *cursor = end;
    Ok(u128::from_be_bytes(slice.try_into().expect("u128 slice")))
}

fn read_hash(bytes: &[u8], cursor: &mut usize) -> Result<LogHash> {
    let end = cursor
        .checked_add(32)
        .ok_or_else(|| Error::Decode("recorder cursor overflow".into()))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::Decode("short recorder hash".into()))?;
    *cursor = end;
    let mut out = [0; 32];
    out.copy_from_slice(slice);
    Ok(LogHash::from_bytes(out))
}

fn read_bytes(bytes: &[u8], cursor: &mut usize) -> Result<Vec<u8>> {
    let len = read_u16(bytes, cursor)? as usize;
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| Error::Decode("recorder cursor overflow".into()))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::Decode("short recorder bytes".into()))?;
    *cursor = end;
    Ok(slice.to_vec())
}

#[derive(Debug)]
pub struct SingleNodeConsensus {
    cluster_id: String,
    epoch: Epoch,
    config_id: ConfigId,
    state: Mutex<SingleNodeState>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SingleNodeState {
    next_index: LogIndex,
    last_hash: LogHash,
}

impl SingleNodeConsensus {
    pub fn new(cluster_id: impl Into<String>, epoch: Epoch, config_id: ConfigId) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            epoch,
            config_id,
            state: Mutex::new(SingleNodeState {
                next_index: 1,
                last_hash: LogHash::ZERO,
            }),
        }
    }
}

impl Consensus for SingleNodeConsensus {
    fn propose(&self, command: Command) -> Result<LogEntry> {
        let mut state = self.state.lock().map_err(|_| Error::ProposeFailed)?;
        let command = stored_command(command)?;
        let prev_hash = state.last_hash;
        let hash = LogEntry::calculate_hash(
            &self.cluster_id,
            state.next_index,
            self.epoch,
            self.config_id,
            command.entry_type,
            prev_hash,
            &command.payload,
        );
        let entry = LogEntry {
            cluster_id: self.cluster_id.clone(),
            epoch: self.epoch,
            config_id: self.config_id,
            index: state.next_index,
            entry_type: command.entry_type,
            payload: command.payload,
            prev_hash,
            hash,
        };
        state.next_index += 1;
        state.last_hash = hash;
        Ok(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AcceptedValue, ConfigChange, Consensus, Membership, Proposal, ProposalPriority,
        ProposerProgress, RecorderFileStore, RecorderRpc, SingleNodeConsensus, ThreeNodeConsensus,
    };
    use queqlite_core::{Command, CommandKind, EntryType, LogHash, StoredCommand};

    #[test]
    fn single_node_consensus_commits_contiguous_hash_chain() {
        let consensus = SingleNodeConsensus::new("cluster-a", 1, 1);
        let first = consensus
            .propose(Command::new(CommandKind::Deterministic, b"first".to_vec()))
            .unwrap();
        let second = consensus
            .propose(Command::new(CommandKind::Deterministic, b"second".to_vec()))
            .unwrap();

        assert_eq!(first.index, 1);
        assert_eq!(first.prev_hash, LogHash::ZERO);
        assert_eq!(first.hash, first.recompute_hash());
        assert_eq!(second.index, 2);
        assert_eq!(second.prev_hash, first.hash);
        assert_eq!(second.hash, second.recompute_hash());
    }

    #[test]
    fn progress_remembers_config_change_after_later_normal_adoption() {
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
        let offered = StoredCommand::new(EntryType::Command, b"offered".to_vec());
        let transition = ConfigChange::stop(1, membership.digest()).to_stored_command();
        let adopted = StoredCommand::new(EntryType::Command, b"adopted".to_vec());
        for store in &stores {
            for command in [&transition, &adopted] {
                store
                    .store_command(command.hash(), command.clone())
                    .unwrap();
            }
        }
        let recorders = membership
            .members()
            .iter()
            .zip(&stores)
            .map(|(id, store)| (id.clone(), Box::new(store.clone()) as Box<dyn RecorderRpc>))
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
        let proposal = |command: &StoredCommand| {
            Proposal::new(
                ProposalPriority::from_u64(1),
                "other",
                1,
                AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, command),
            )
        };
        let mut progress = ProposerProgress::new(1, proposal(&offered)).with_command(offered);

        progress.proposal = proposal(&transition);
        consensus.ensure_progress_command(&mut progress).unwrap();
        progress.proposal = proposal(&adopted);
        consensus.ensure_progress_command(&mut progress).unwrap();

        assert_eq!(progress.command, Some(adopted));
        assert!(progress.transition_involved);
    }
}
