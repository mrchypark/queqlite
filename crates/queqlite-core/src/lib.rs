use sha2::{Digest, Sha256};

pub type LogIndex = u64;
pub type Epoch = u64;
pub type ConfigId = u64;
pub type NodeId = String;
pub type ClusterId = String;

pub const RECOVERY_ANCHOR_FORMAT_VERSION: u32 = 2;
pub const RECOVERY_ANCHOR_V1_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct LogHash([u8; 32]);

impl LogHash {
    pub const ZERO: Self = Self([0; 32]);

    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn digest(parts: &[&[u8]]) -> Self {
        let mut hasher = Sha256::new();
        for part in parts {
            hasher.update(part);
        }
        Self(hasher.finalize().into())
    }

    pub fn to_hex(self) -> String {
        let mut out = String::with_capacity(64);
        for byte in self.0 {
            out.push(hex_char(byte >> 4));
            out.push(hex_char(byte & 0x0f));
        }
        out
    }

    pub fn from_hex(hex: &str) -> Option<Self> {
        if hex.len() != 64 {
            return None;
        }

        let mut bytes = [0; 32];
        for (index, chunk) in hex.as_bytes().chunks_exact(2).enumerate() {
            bytes[index] = (hex_value(chunk[0])? << 4) | hex_value(chunk[1])?;
        }
        Some(Self(bytes))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogAnchor {
    index: LogIndex,
    hash: LogHash,
}

impl LogAnchor {
    pub const fn new(index: LogIndex, hash: LogHash) -> Self {
        Self { index, hash }
    }

    pub const fn index(&self) -> LogIndex {
        self.index
    }

    pub const fn hash(&self) -> LogHash {
        self.hash
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum StopBinding {
    #[default]
    Unknown,
    Unbound,
    Bound {
        successor: SuccessorDescriptor,
        stop_command_hash: LogHash,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConfigurationState {
    Active {
        config_id: ConfigId,
        digest: LogHash,
    },
    Stopped {
        config_id: ConfigId,
        digest: LogHash,
        stop: LogAnchor,
        #[serde(default)]
        binding: StopBinding,
    },
}

impl ConfigurationState {
    pub const fn active(config_id: ConfigId, digest: LogHash) -> Self {
        Self::Active { config_id, digest }
    }

    pub const fn stopped(config_id: ConfigId, digest: LogHash, stop: LogAnchor) -> Self {
        Self::Stopped {
            config_id,
            digest,
            stop,
            binding: StopBinding::Unbound,
        }
    }

    pub const fn config_id(&self) -> ConfigId {
        match self {
            Self::Active { config_id, .. } | Self::Stopped { config_id, .. } => *config_id,
        }
    }

    pub const fn digest(&self) -> LogHash {
        match self {
            Self::Active { digest, .. } | Self::Stopped { digest, .. } => *digest,
        }
    }

    pub const fn stop(&self) -> Option<&LogAnchor> {
        match self {
            Self::Active { .. } => None,
            Self::Stopped { stop, .. } => Some(stop),
        }
    }

    pub const fn is_active(&self) -> bool {
        matches!(self, Self::Active { .. })
    }

    pub fn validate_entry(&self, entry: &LogEntry) -> Result<Self, ConfigurationTransitionError> {
        if entry.recompute_hash() != entry.hash {
            return Err(ConfigurationTransitionError::EntryHashMismatch);
        }
        let change = if entry.entry_type == EntryType::ConfigChange {
            Some(
                ConfigChange::recognize_parts(entry.entry_type, &entry.payload)
                    .map_err(|_| ConfigurationTransitionError::InvalidConfigChange)?,
            )
        } else {
            None
        };

        match (self, change) {
            (
                Self::Active { config_id, digest },
                Some(ConfigChange::Stop {
                    config_id: stop_config_id,
                    config_digest,
                }),
            ) if entry.config_id == *config_id
                && stop_config_id == *config_id
                && (*digest == LogHash::ZERO || config_digest == *digest) =>
            {
                Ok(Self::stopped(
                    *config_id,
                    config_digest,
                    LogAnchor::new(entry.index, entry.hash),
                ))
            }
            (Self::Active { config_id, digest }, Some(ConfigChange::BoundStop { successor }))
                if entry.cluster_id == successor.cluster_id
                    && entry.config_id == *config_id
                    && successor.predecessor_config_id == *config_id
                    && successor.predecessor_config_digest == *digest =>
            {
                let stop_command_hash = (ConfigChange::BoundStop {
                    successor: successor.clone(),
                })
                .to_stored_command()
                .hash();
                Ok(Self::Stopped {
                    config_id: *config_id,
                    digest: *digest,
                    stop: LogAnchor::new(entry.index, entry.hash),
                    binding: StopBinding::Bound {
                        successor,
                        stop_command_hash,
                    },
                })
            }
            (Self::Active { config_id, .. }, None) if entry.config_id == *config_id => {
                Ok(self.clone())
            }
            (Self::Active { .. }, _) => Err(ConfigurationTransitionError::ConfigurationMismatch),
            (
                Self::Stopped {
                    config_id: predecessor_id,
                    stop,
                    binding: StopBinding::Unbound,
                    ..
                },
                Some(ConfigChange::ActivationBarrier {
                    config_id,
                    config_digest,
                    stop_slot,
                    prefix_hash,
                }),
            ) if predecessor_id.checked_add(1) == Some(config_id)
                && entry.config_id == config_id
                && stop.index().checked_add(1) == Some(entry.index)
                && entry.prev_hash == stop.hash()
                && stop_slot == stop.index()
                && prefix_hash == stop.hash() =>
            {
                Ok(Self::active(config_id, config_digest))
            }
            (
                Self::Stopped {
                    config_id: predecessor_id,
                    digest: predecessor_digest,
                    stop,
                    binding:
                        StopBinding::Bound {
                            successor: authorized_successor,
                            stop_command_hash: authorized_stop_command_hash,
                        },
                },
                Some(ConfigChange::BoundActivationBarrier {
                    successor,
                    stop_slot,
                    prefix_hash,
                    stop_command_hash,
                }),
            ) if successor.predecessor_config_id == *predecessor_id
                && successor.predecessor_config_digest == *predecessor_digest
                && &successor == authorized_successor
                && entry.cluster_id == successor.cluster_id
                && entry.config_id == successor.config_id
                && stop.index().checked_add(1) == Some(entry.index)
                && entry.prev_hash == stop.hash()
                && stop_slot == stop.index()
                && prefix_hash == stop.hash()
                // Reject a deserialized state whose cached authorization hash
                // does not match its bound successor descriptor.
                && *authorized_stop_command_hash
                    == (ConfigChange::BoundStop {
                        successor: authorized_successor.clone(),
                    })
                    .to_stored_command()
                    .hash()
                && stop_command_hash == *authorized_stop_command_hash =>
            {
                Ok(Self::active(successor.config_id, successor.config_digest))
            }
            (Self::Stopped { .. }, _) => Err(ConfigurationTransitionError::InvalidActivation),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigurationTransitionError {
    EntryHashMismatch,
    InvalidConfigChange,
    ConfigurationMismatch,
    InvalidActivation,
}

impl std::fmt::Display for ConfigurationTransitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid configuration transition: {self:?}")
    }
}

impl std::error::Error for ConfigurationTransitionError {}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotIdentity {
    snapshot_id: String,
    digest: LogHash,
    size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    executor_fingerprint: Option<LogHash>,
}

impl SnapshotIdentity {
    pub fn new(snapshot_id: impl Into<String>, digest: LogHash, size_bytes: u64) -> Self {
        Self {
            snapshot_id: snapshot_id.into(),
            digest,
            size_bytes,
            executor_fingerprint: None,
        }
    }

    pub fn with_executor_fingerprint(mut self, executor_fingerprint: LogHash) -> Self {
        self.executor_fingerprint = Some(executor_fingerprint);
        self
    }

    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    pub const fn digest(&self) -> LogHash {
        self.digest
    }

    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub const fn executor_fingerprint(&self) -> Option<LogHash> {
        self.executor_fingerprint
    }

    pub const fn is_legacy_executor_fingerprint(&self) -> bool {
        self.executor_fingerprint.is_none()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryAnchor {
    format_version: u32,
    cluster_id: ClusterId,
    epoch: Epoch,
    config_id: ConfigId,
    configuration_state: ConfigurationState,
    recovery_generation: u64,
    compacted: LogAnchor,
    snapshot: SnapshotIdentity,
}

impl RecoveryAnchor {
    pub fn new(
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        config_id: ConfigId,
        recovery_generation: u64,
        compacted: LogAnchor,
        snapshot: SnapshotIdentity,
    ) -> Self {
        Self {
            format_version: RECOVERY_ANCHOR_FORMAT_VERSION,
            cluster_id: cluster_id.into(),
            epoch,
            config_id,
            configuration_state: ConfigurationState::active(config_id, LogHash::ZERO),
            recovery_generation,
            compacted,
            snapshot,
        }
    }

    pub fn new_with_configuration(
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        configuration_state: ConfigurationState,
        recovery_generation: u64,
        compacted: LogAnchor,
        snapshot: SnapshotIdentity,
    ) -> Self {
        Self {
            format_version: RECOVERY_ANCHOR_FORMAT_VERSION,
            cluster_id: cluster_id.into(),
            epoch,
            config_id: configuration_state.config_id(),
            configuration_state,
            recovery_generation,
            compacted,
            snapshot,
        }
    }

    pub fn from_v1(
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        config_id: ConfigId,
        recovery_generation: u64,
        compacted: LogAnchor,
        snapshot: SnapshotIdentity,
    ) -> Self {
        Self {
            format_version: RECOVERY_ANCHOR_V1_FORMAT_VERSION,
            cluster_id: cluster_id.into(),
            epoch,
            config_id,
            configuration_state: ConfigurationState::active(config_id, LogHash::ZERO),
            recovery_generation,
            compacted,
            snapshot,
        }
    }

    pub const fn format_version(&self) -> u32 {
        self.format_version
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

    pub const fn configuration_state(&self) -> &ConfigurationState {
        &self.configuration_state
    }

    pub const fn recovery_generation(&self) -> u64 {
        self.recovery_generation
    }

    pub const fn compacted(&self) -> &LogAnchor {
        &self.compacted
    }

    pub const fn snapshot(&self) -> &SnapshotIdentity {
        &self.snapshot
    }

    pub const fn executor_fingerprint(&self) -> Option<LogHash> {
        self.snapshot.executor_fingerprint()
    }
}

impl<'de> serde::Deserialize<'de> for RecoveryAnchor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            format_version: u32,
            cluster_id: ClusterId,
            epoch: Epoch,
            config_id: ConfigId,
            #[serde(default)]
            configuration_state: Option<ConfigurationState>,
            recovery_generation: u64,
            compacted: LogAnchor,
            snapshot: SnapshotIdentity,
        }

        let wire = Wire::deserialize(deserializer)?;
        let configuration_state = match (wire.format_version, wire.configuration_state) {
            (RECOVERY_ANCHOR_V1_FORMAT_VERSION, None) => {
                ConfigurationState::active(wire.config_id, LogHash::ZERO)
            }
            (RECOVERY_ANCHOR_V1_FORMAT_VERSION, Some(state))
                if state == ConfigurationState::active(wire.config_id, LogHash::ZERO) =>
            {
                state
            }
            (RECOVERY_ANCHOR_FORMAT_VERSION, Some(state))
                if state.config_id() == wire.config_id =>
            {
                state
            }
            _ => {
                return Err(serde::de::Error::custom(
                    "invalid recovery anchor configuration state",
                ))
            }
        };
        Ok(Self {
            format_version: wire.format_version,
            cluster_id: wire.cluster_id,
            epoch: wire.epoch,
            config_id: wire.config_id,
            configuration_state,
            recovery_generation: wire.recovery_generation,
            compacted: wire.compacted,
            snapshot: wire.snapshot,
        })
    }
}

fn hex_char(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + value - 10) as char,
        _ => unreachable!("nibble out of range"),
    }
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct EntryId {
    pub epoch: Epoch,
    pub index: LogIndex,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum EntryType {
    Command,
    ConfigChange,
    SnapshotBarrier,
    SnapshotPublished,
    Noop,
}

impl EntryType {
    pub const fn as_u8(self) -> u8 {
        match self {
            Self::Command => 1,
            Self::ConfigChange => 2,
            Self::SnapshotBarrier => 3,
            Self::SnapshotPublished => 4,
            Self::Noop => 5,
        }
    }

    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Command),
            2 => Some(Self::ConfigChange),
            3 => Some(Self::SnapshotBarrier),
            4 => Some(Self::SnapshotPublished),
            5 => Some(Self::Noop),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum CommandKind {
    Deterministic,
    ReadBarrier,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Command {
    kind: CommandKind,
    payload: Vec<u8>,
}

impl Command {
    pub fn new(kind: CommandKind, payload: Vec<u8>) -> Self {
        Self { kind, payload }
    }

    pub const fn kind(&self) -> CommandKind {
        self.kind
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct StoredCommand {
    pub entry_type: EntryType,
    pub payload: Vec<u8>,
}

impl StoredCommand {
    pub fn new(entry_type: EntryType, payload: Vec<u8>) -> Self {
        Self {
            entry_type,
            payload,
        }
    }

    pub fn hash(&self) -> LogHash {
        let entry_type = [self.entry_type.as_u8()];
        LogHash::digest(&[b"queqlite-command-v1", &entry_type, &self.payload])
    }
}

const CONFIG_CHANGE_MAGIC: &[u8; 4] = b"QCFG";
const CONFIG_CHANGE_VERSION: u16 = 1;
const BOUND_CONFIG_CHANGE_VERSION: u16 = 2;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct SuccessorDescriptor {
    cluster_id: ClusterId,
    predecessor_config_id: ConfigId,
    predecessor_config_digest: LogHash,
    config_id: ConfigId,
    config_digest: LogHash,
    members: Vec<NodeId>,
}

impl<'de> serde::Deserialize<'de> for SuccessorDescriptor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            cluster_id: ClusterId,
            predecessor_config_id: ConfigId,
            predecessor_config_digest: LogHash,
            config_id: ConfigId,
            config_digest: LogHash,
            members: Vec<NodeId>,
        }

        let wire = Wire::deserialize(deserializer)?;
        if !wire.members.windows(2).all(|pair| pair[0] < pair[1]) {
            return Err(serde::de::Error::custom(
                "successor members are not canonical",
            ));
        }
        let encoded_digest = wire.config_digest;
        let descriptor = Self::new(
            wire.cluster_id,
            wire.predecessor_config_id,
            wire.predecessor_config_digest,
            wire.config_id,
            wire.members,
        )
        .map_err(serde::de::Error::custom)?;
        if descriptor.config_digest != encoded_digest {
            return Err(serde::de::Error::custom(
                "successor membership digest mismatch",
            ));
        }
        Ok(descriptor)
    }
}

impl SuccessorDescriptor {
    pub fn new(
        cluster_id: impl Into<ClusterId>,
        predecessor_config_id: ConfigId,
        predecessor_config_digest: LogHash,
        config_id: ConfigId,
        members: Vec<NodeId>,
    ) -> Result<Self, ConfigChangeDecodeError> {
        let cluster_id = cluster_id.into();
        if cluster_id.is_empty() || cluster_id.len() > usize::from(u16::MAX) {
            return Err(ConfigChangeDecodeError);
        }
        if predecessor_config_id.checked_add(1) != Some(config_id) {
            return Err(ConfigChangeDecodeError);
        }
        let mut canonical = members;
        let original_len = canonical.len();
        canonical.sort();
        canonical.dedup();
        if canonical.len() != original_len
            || !(3..=7).contains(&canonical.len())
            || canonical
                .iter()
                .any(|member| member.is_empty() || member.len() > usize::from(u16::MAX))
        {
            return Err(ConfigChangeDecodeError);
        }
        let config_digest = canonical_membership_digest(&canonical)?;
        Ok(Self {
            cluster_id,
            predecessor_config_id,
            predecessor_config_digest,
            config_id,
            config_digest,
            members: canonical,
        })
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub const fn predecessor_config_id(&self) -> ConfigId {
        self.predecessor_config_id
    }

    pub const fn predecessor_config_digest(&self) -> LogHash {
        self.predecessor_config_digest
    }

    pub const fn config_id(&self) -> ConfigId {
        self.config_id
    }

    pub const fn digest(&self) -> LogHash {
        self.config_digest
    }

    pub fn members(&self) -> &[NodeId] {
        &self.members
    }
}

pub fn canonical_membership_digest(members: &[NodeId]) -> Result<LogHash, ConfigChangeDecodeError> {
    if !(3..=7).contains(&members.len())
        || members
            .iter()
            .any(|member| member.is_empty() || member.len() > usize::from(u16::MAX))
        || !members.windows(2).all(|pair| pair[0] < pair[1])
    {
        return Err(ConfigChangeDecodeError);
    }
    let encoded_len = 14 + members.len() * 8 + members.iter().map(String::len).sum::<usize>();
    let mut encoded = Vec::with_capacity(encoded_len);
    encoded.extend_from_slice(b"QMEM\0\x01");
    encoded.extend_from_slice(&(members.len() as u64).to_be_bytes());
    for member in members {
        encoded.extend_from_slice(&(member.len() as u64).to_be_bytes());
        encoded.extend_from_slice(member.as_bytes());
    }
    Ok(LogHash::digest(&[&encoded]))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigChange {
    Stop {
        config_id: ConfigId,
        config_digest: LogHash,
    },
    ActivationBarrier {
        config_id: ConfigId,
        config_digest: LogHash,
        stop_slot: LogIndex,
        prefix_hash: LogHash,
    },
    BoundStop {
        successor: SuccessorDescriptor,
    },
    BoundActivationBarrier {
        successor: SuccessorDescriptor,
        stop_slot: LogIndex,
        prefix_hash: LogHash,
        stop_command_hash: LogHash,
    },
}

impl ConfigChange {
    pub const fn stop(config_id: ConfigId, config_digest: LogHash) -> Self {
        Self::Stop {
            config_id,
            config_digest,
        }
    }

    pub const fn activation_barrier(
        config_id: ConfigId,
        config_digest: LogHash,
        stop_slot: LogIndex,
        prefix_hash: LogHash,
    ) -> Self {
        Self::ActivationBarrier {
            config_id,
            config_digest,
            stop_slot,
            prefix_hash,
        }
    }

    pub fn bound_stop(
        cluster_id: impl Into<ClusterId>,
        predecessor_config_id: ConfigId,
        predecessor_config_digest: LogHash,
        successor_config_id: ConfigId,
        successor_members: Vec<NodeId>,
    ) -> Result<Self, ConfigChangeDecodeError> {
        Ok(Self::BoundStop {
            successor: SuccessorDescriptor::new(
                cluster_id,
                predecessor_config_id,
                predecessor_config_digest,
                successor_config_id,
                successor_members,
            )?,
        })
    }

    pub const fn bound_activation_barrier(
        successor: SuccessorDescriptor,
        stop_slot: LogIndex,
        prefix_hash: LogHash,
        stop_command_hash: LogHash,
    ) -> Self {
        Self::BoundActivationBarrier {
            successor,
            stop_slot,
            prefix_hash,
            stop_command_hash,
        }
    }

    pub const fn successor(&self) -> Option<&SuccessorDescriptor> {
        match self {
            Self::BoundStop { successor } | Self::BoundActivationBarrier { successor, .. } => {
                Some(successor)
            }
            _ => None,
        }
    }

    pub fn to_stored_command(&self) -> StoredCommand {
        let mut payload = Vec::with_capacity(87);
        payload.extend_from_slice(CONFIG_CHANGE_MAGIC);
        let version = if matches!(
            self,
            Self::BoundStop { .. } | Self::BoundActivationBarrier { .. }
        ) {
            BOUND_CONFIG_CHANGE_VERSION
        } else {
            CONFIG_CHANGE_VERSION
        };
        payload.extend_from_slice(&version.to_be_bytes());
        match self {
            Self::Stop {
                config_id,
                config_digest,
            } => {
                payload.push(1);
                payload.extend_from_slice(&config_id.to_be_bytes());
                payload.extend_from_slice(config_digest.as_bytes());
            }
            Self::ActivationBarrier {
                config_id,
                config_digest,
                stop_slot,
                prefix_hash,
            } => {
                payload.push(2);
                payload.extend_from_slice(&config_id.to_be_bytes());
                payload.extend_from_slice(config_digest.as_bytes());
                payload.extend_from_slice(&stop_slot.to_be_bytes());
                payload.extend_from_slice(prefix_hash.as_bytes());
            }
            Self::BoundStop { successor } => {
                payload.push(1);
                encode_successor(&mut payload, successor);
            }
            Self::BoundActivationBarrier {
                successor,
                stop_slot,
                prefix_hash,
                stop_command_hash,
            } => {
                payload.push(2);
                encode_successor(&mut payload, successor);
                payload.extend_from_slice(&stop_slot.to_be_bytes());
                payload.extend_from_slice(prefix_hash.as_bytes());
                payload.extend_from_slice(stop_command_hash.as_bytes());
            }
        }
        StoredCommand::new(EntryType::ConfigChange, payload)
    }

    pub fn recognize(command: &StoredCommand) -> Result<Self, ConfigChangeDecodeError> {
        Self::recognize_parts(command.entry_type, &command.payload)
    }

    pub fn recognize_parts(
        entry_type: EntryType,
        payload: &[u8],
    ) -> Result<Self, ConfigChangeDecodeError> {
        if entry_type != EntryType::ConfigChange {
            return Err(ConfigChangeDecodeError);
        }
        let bytes = payload;
        if bytes.get(..4) != Some(CONFIG_CHANGE_MAGIC) {
            return Err(ConfigChangeDecodeError);
        }
        let version = read_config_u16(bytes, 4)?;
        if version == BOUND_CONFIG_CHANGE_VERSION {
            let kind = *bytes.get(6).ok_or(ConfigChangeDecodeError)?;
            let mut cursor = 7;
            let successor = decode_successor(bytes, &mut cursor)?;
            let change = match kind {
                1 => Self::BoundStop { successor },
                2 => Self::BoundActivationBarrier {
                    successor,
                    stop_slot: read_config_u64_at(bytes, &mut cursor)?,
                    prefix_hash: read_config_hash_at(bytes, &mut cursor)?,
                    stop_command_hash: read_config_hash_at(bytes, &mut cursor)?,
                },
                _ => return Err(ConfigChangeDecodeError),
            };
            if cursor != bytes.len() {
                return Err(ConfigChangeDecodeError);
            }
            return Ok(change);
        }
        if version != CONFIG_CHANGE_VERSION {
            return Err(ConfigChangeDecodeError);
        }
        let kind = *bytes.get(6).ok_or(ConfigChangeDecodeError)?;
        let config_id = read_config_u64(bytes, 7)?;
        let config_digest = read_config_hash(bytes, 15)?;
        match kind {
            1 if bytes.len() == 47 => Ok(Self::stop(config_id, config_digest)),
            2 if bytes.len() == 87 => Ok(Self::activation_barrier(
                config_id,
                config_digest,
                read_config_u64(bytes, 47)?,
                read_config_hash(bytes, 55)?,
            )),
            _ => Err(ConfigChangeDecodeError),
        }
    }

    pub const fn binding(&self) -> (ConfigId, LogHash) {
        match self {
            Self::Stop {
                config_id,
                config_digest,
            }
            | Self::ActivationBarrier {
                config_id,
                config_digest,
                ..
            } => (*config_id, *config_digest),
            Self::BoundStop { successor } => (
                successor.predecessor_config_id,
                successor.predecessor_config_digest,
            ),
            Self::BoundActivationBarrier { successor, .. } => {
                (successor.config_id, successor.config_digest)
            }
        }
    }
}

fn encode_successor(out: &mut Vec<u8>, successor: &SuccessorDescriptor) {
    let cluster = successor.cluster_id.as_bytes();
    let encoded_len = 83
        + cluster.len()
        + successor
            .members
            .iter()
            .map(|member| 2 + member.len())
            .sum::<usize>();
    out.reserve(encoded_len);
    let cluster_length =
        u16::try_from(cluster.len()).expect("validated successor cluster length fits u16");
    out.extend_from_slice(&cluster_length.to_be_bytes());
    out.extend_from_slice(cluster);
    out.extend_from_slice(&successor.predecessor_config_id.to_be_bytes());
    out.extend_from_slice(successor.predecessor_config_digest.as_bytes());
    out.extend_from_slice(&successor.config_id.to_be_bytes());
    out.extend_from_slice(successor.config_digest.as_bytes());
    let member_count =
        u8::try_from(successor.members.len()).expect("validated successor member count fits u8");
    out.push(member_count);
    for member in &successor.members {
        let member_length =
            u16::try_from(member.len()).expect("validated successor member length fits u16");
        out.extend_from_slice(&member_length.to_be_bytes());
        out.extend_from_slice(member.as_bytes());
    }
}

fn decode_successor(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<SuccessorDescriptor, ConfigChangeDecodeError> {
    let cluster_id = read_config_string(bytes, cursor)?;
    let predecessor_config_id = read_config_u64_at(bytes, cursor)?;
    let predecessor_config_digest = read_config_hash_at(bytes, cursor)?;
    let config_id = read_config_u64_at(bytes, cursor)?;
    let encoded_digest = read_config_hash_at(bytes, cursor)?;
    let count = *bytes.get(*cursor).ok_or(ConfigChangeDecodeError)? as usize;
    if !(3..=7).contains(&count) {
        return Err(ConfigChangeDecodeError);
    }
    *cursor += 1;
    let members = (0..count)
        .map(|_| read_config_string(bytes, cursor))
        .collect::<Result<Vec<_>, _>>()?;
    if !members.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(ConfigChangeDecodeError);
    }
    let descriptor = SuccessorDescriptor::new(
        cluster_id,
        predecessor_config_id,
        predecessor_config_digest,
        config_id,
        members,
    )?;
    if descriptor.config_digest != encoded_digest {
        return Err(ConfigChangeDecodeError);
    }
    Ok(descriptor)
}

fn read_config_string(bytes: &[u8], cursor: &mut usize) -> Result<String, ConfigChangeDecodeError> {
    let length = read_config_u16(bytes, *cursor)? as usize;
    let value_start = cursor.checked_add(2).ok_or(ConfigChangeDecodeError)?;
    let value_end = value_start
        .checked_add(length)
        .ok_or(ConfigChangeDecodeError)?;
    let value = bytes
        .get(value_start..value_end)
        .ok_or(ConfigChangeDecodeError)?;
    *cursor = value_end;
    std::str::from_utf8(value)
        .map(str::to_owned)
        .map_err(|_| ConfigChangeDecodeError)
}

fn read_config_u64_at(bytes: &[u8], cursor: &mut usize) -> Result<u64, ConfigChangeDecodeError> {
    let value = read_config_u64(bytes, *cursor)?;
    *cursor = cursor.checked_add(8).ok_or(ConfigChangeDecodeError)?;
    Ok(value)
}

fn read_config_hash_at(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<LogHash, ConfigChangeDecodeError> {
    let value = read_config_hash(bytes, *cursor)?;
    *cursor = cursor.checked_add(32).ok_or(ConfigChangeDecodeError)?;
    Ok(value)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfigChangeDecodeError;

impl std::fmt::Display for ConfigChangeDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid ConfigChange payload")
    }
}

impl std::error::Error for ConfigChangeDecodeError {}

fn read_config_u16(bytes: &[u8], offset: usize) -> Result<u16, ConfigChangeDecodeError> {
    let end = offset.checked_add(2).ok_or(ConfigChangeDecodeError)?;
    let bytes = bytes.get(offset..end).ok_or(ConfigChangeDecodeError)?;
    Ok(u16::from_be_bytes(
        bytes.try_into().expect("u16 slice length"),
    ))
}

fn read_config_u64(bytes: &[u8], offset: usize) -> Result<u64, ConfigChangeDecodeError> {
    let end = offset.checked_add(8).ok_or(ConfigChangeDecodeError)?;
    let bytes = bytes.get(offset..end).ok_or(ConfigChangeDecodeError)?;
    Ok(u64::from_be_bytes(
        bytes.try_into().expect("u64 slice length"),
    ))
}

fn read_config_hash(bytes: &[u8], offset: usize) -> Result<LogHash, ConfigChangeDecodeError> {
    let end = offset.checked_add(32).ok_or(ConfigChangeDecodeError)?;
    let bytes: [u8; 32] = bytes
        .get(offset..end)
        .ok_or(ConfigChangeDecodeError)?
        .try_into()
        .expect("hash slice length");
    Ok(LogHash::from_bytes(bytes))
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct LogEntry {
    pub cluster_id: ClusterId,
    pub epoch: Epoch,
    pub config_id: ConfigId,
    pub index: LogIndex,
    pub entry_type: EntryType,
    pub payload: Vec<u8>,
    pub prev_hash: LogHash,
    pub hash: LogHash,
}

impl LogEntry {
    pub fn calculate_hash(
        cluster_id: &str,
        index: LogIndex,
        epoch: Epoch,
        config_id: ConfigId,
        entry_type: EntryType,
        prev_hash: LogHash,
        payload: &[u8],
    ) -> LogHash {
        let cluster_length = (cluster_id.len() as u64).to_be_bytes();
        let index = index.to_be_bytes();
        let epoch = epoch.to_be_bytes();
        let config_id = config_id.to_be_bytes();
        let entry_type = [entry_type.as_u8()];
        let payload_hash = LogHash::digest(&[payload]);
        LogHash::digest(&[
            b"queqlite-log-entry-v2\0",
            &cluster_length,
            cluster_id.as_bytes(),
            &index,
            &epoch,
            &config_id,
            &entry_type,
            prev_hash.as_bytes(),
            payload_hash.as_bytes(),
        ])
    }

    pub fn recompute_hash(&self) -> LogHash {
        Self::calculate_hash(
            &self.cluster_id,
            self.index,
            self.epoch,
            self.config_id,
            self.entry_type,
            self.prev_hash,
            &self.payload,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotManifest {
    snapshot_id: String,
    cluster_id: ClusterId,
    config_id: ConfigId,
    configuration_state: ConfigurationState,
    epoch: Epoch,
    index: LogIndex,
    applied_hash: LogHash,
    schema_version: u64,
    created_by: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    executor_fingerprint: Option<LogHash>,
}

impl SnapshotManifest {
    pub fn new(
        cluster_id: impl Into<ClusterId>,
        config_id: ConfigId,
        epoch: Epoch,
        index: LogIndex,
        applied_hash: LogHash,
        schema_version: u64,
        created_by: impl Into<NodeId>,
    ) -> Self {
        Self {
            snapshot_id: format!("snapshot-{index:015}"),
            cluster_id: cluster_id.into(),
            config_id,
            configuration_state: ConfigurationState::active(config_id, LogHash::ZERO),
            epoch,
            index,
            applied_hash,
            schema_version,
            created_by: created_by.into(),
            executor_fingerprint: None,
        }
    }

    pub fn new_with_configuration(
        cluster_id: impl Into<ClusterId>,
        configuration_state: ConfigurationState,
        epoch: Epoch,
        index: LogIndex,
        applied_hash: LogHash,
        schema_version: u64,
        created_by: impl Into<NodeId>,
    ) -> Self {
        Self {
            snapshot_id: format!("snapshot-{index:015}"),
            cluster_id: cluster_id.into(),
            config_id: configuration_state.config_id(),
            configuration_state,
            epoch,
            index,
            applied_hash,
            schema_version,
            created_by: created_by.into(),
            executor_fingerprint: None,
        }
    }

    pub fn with_executor_fingerprint(mut self, executor_fingerprint: LogHash) -> Self {
        self.executor_fingerprint = Some(executor_fingerprint);
        self
    }

    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
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

    pub const fn configuration_state(&self) -> &ConfigurationState {
        &self.configuration_state
    }

    pub const fn schema_version(&self) -> u64 {
        self.schema_version
    }

    pub fn created_by(&self) -> &str {
        &self.created_by
    }

    pub const fn index(&self) -> LogIndex {
        self.index
    }

    pub const fn snapshot_index(&self) -> LogIndex {
        self.index
    }

    pub const fn applied_hash(&self) -> LogHash {
        self.applied_hash
    }

    pub const fn executor_fingerprint(&self) -> Option<LogHash> {
        self.executor_fingerprint
    }

    pub const fn is_legacy_executor_fingerprint(&self) -> bool {
        self.executor_fingerprint.is_none()
    }
}

impl<'de> serde::Deserialize<'de> for SnapshotManifest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            snapshot_id: String,
            cluster_id: ClusterId,
            config_id: ConfigId,
            #[serde(default)]
            configuration_state: Option<ConfigurationState>,
            epoch: Epoch,
            index: LogIndex,
            applied_hash: LogHash,
            schema_version: u64,
            created_by: NodeId,
            #[serde(default)]
            executor_fingerprint: Option<LogHash>,
        }

        let wire = Wire::deserialize(deserializer)?;
        let configuration_state = wire
            .configuration_state
            .unwrap_or_else(|| ConfigurationState::active(wire.config_id, LogHash::ZERO));
        if configuration_state.config_id() != wire.config_id {
            return Err(serde::de::Error::custom(
                "snapshot configuration state does not match config_id",
            ));
        }
        Ok(Self {
            snapshot_id: wire.snapshot_id,
            cluster_id: wire.cluster_id,
            config_id: wire.config_id,
            configuration_state,
            epoch: wire.epoch,
            index: wire.index,
            applied_hash: wire.applied_hash,
            schema_version: wire.schema_version,
            created_by: wire.created_by,
            executor_fingerprint: wire.executor_fingerprint,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Snapshot {
    manifest: SnapshotManifest,
    db_bytes: Vec<u8>,
}

impl Snapshot {
    pub fn new(manifest: SnapshotManifest, db_bytes: Vec<u8>) -> Self {
        Self { manifest, db_bytes }
    }

    pub const fn manifest(&self) -> &SnapshotManifest {
        &self.manifest
    }

    pub fn db_bytes(&self) -> &[u8] {
        &self.db_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::{
        read_config_hash, read_config_hash_at, read_config_string, read_config_u16,
        read_config_u64, read_config_u64_at, ConfigChangeDecodeError,
    };

    #[test]
    fn config_decoder_rejects_overflowing_offsets_without_panicking() {
        let bytes = [];
        assert_eq!(
            read_config_u16(&bytes, usize::MAX),
            Err(ConfigChangeDecodeError)
        );
        assert_eq!(
            read_config_u64(&bytes, usize::MAX),
            Err(ConfigChangeDecodeError)
        );
        assert_eq!(
            read_config_hash(&bytes, usize::MAX),
            Err(ConfigChangeDecodeError)
        );

        let mut cursor = usize::MAX;
        assert_eq!(
            read_config_string(&bytes, &mut cursor),
            Err(ConfigChangeDecodeError)
        );
        assert_eq!(
            read_config_u64_at(&bytes, &mut cursor),
            Err(ConfigChangeDecodeError)
        );
        assert_eq!(
            read_config_hash_at(&bytes, &mut cursor),
            Err(ConfigChangeDecodeError)
        );
    }
}
