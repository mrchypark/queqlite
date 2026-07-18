use std::{fs, path::Path};

use rhiza_core::{EntryType, LogEntry, LogHash};
use rhiza_graph::{
    apply_lgfx_to_exact_base, capture_graph_entry_native_wal, diff_closed_ladybug_files,
    encode_replicated_graph_batch, graph_materializer_fingerprint, lgfx_chunks_digest,
    open_lgfx_readback, replay_native_ladybug_wal, Error, GraphCommandV1, GraphValueV1,
    LadybugFileChunkV1, LadybugFileEffectV1, LadybugStateMachine, LGFX_CHUNK_BYTES, LGFX_V1_MAGIC,
    MAX_LGFX_V1_BYTES,
};

fn graph_entry(index: u64, prev_hash: LogHash, commands: &[GraphCommandV1]) -> LogEntry {
    let payload = encode_replicated_graph_batch(commands).unwrap();
    let hash = LogEntry::calculate_hash(
        "cluster-1",
        index,
        7,
        3,
        EntryType::Command,
        prev_hash,
        &payload,
    );
    LogEntry {
        cluster_id: "cluster-1".into(),
        epoch: 7,
        config_id: 3,
        index,
        entry_type: EntryType::Command,
        payload,
        prev_hash,
        hash,
    }
}

fn seed_base(path: &Path) {
    let seed = LadybugStateMachine::open(path, "cluster-1", "node-1", 7, 3).unwrap();
    seed.create_snapshot(0).unwrap();
    drop(seed);
    assert_clean(path);
}

fn effect_for(
    base: &Path,
    target: &Path,
    base_log_index: u64,
    base_log_hash: LogHash,
    request_id: &str,
) -> LadybugFileEffectV1 {
    let chunks = diff_closed_ladybug_files(base, target).unwrap();
    LadybugFileEffectV1 {
        cluster_id: "cluster-1".into(),
        epoch: 7,
        configuration_id: 3,
        recovery_generation: 4,
        base_log_index,
        base_log_hash,
        base_db_digest: file_digest(base),
        base_file_bytes: fs::metadata(base).unwrap().len(),
        target_db_digest: file_digest(target),
        target_file_bytes: fs::metadata(target).unwrap().len(),
        storage_version: lbug::get_storage_version(),
        materializer_fingerprint: graph_materializer_fingerprint(),
        request_id: request_id.into(),
        request_digest: LogHash::digest(&[request_id.as_bytes()]),
        result_encoding_version: 1,
        bounded_result: b"result".to_vec(),
        chunks_digest: lgfx_chunks_digest(&chunks),
        chunks,
    }
}

fn dummy_effect(chunks: Vec<LadybugFileChunkV1>) -> LadybugFileEffectV1 {
    let mut effect = LadybugFileEffectV1 {
        cluster_id: "cluster-1".into(),
        epoch: 7,
        configuration_id: 3,
        recovery_generation: 4,
        base_log_index: 0,
        base_log_hash: LogHash::ZERO,
        base_db_digest: LogHash::digest(&[b"base"]),
        base_file_bytes: LGFX_CHUNK_BYTES as u64,
        target_db_digest: LogHash::digest(&[b"target"]),
        target_file_bytes: LGFX_CHUNK_BYTES as u64,
        storage_version: lbug::get_storage_version(),
        materializer_fingerprint: graph_materializer_fingerprint(),
        request_id: "request-1".into(),
        request_digest: LogHash::digest(&[b"request"]),
        result_encoding_version: 1,
        bounded_result: b"result".to_vec(),
        chunks_digest: LogHash::ZERO,
        chunks,
    };
    effect.chunks_digest = lgfx_chunks_digest(&effect.chunks);
    effect
}

#[test]
fn lgfx_envelope_roundtrips_through_one_bounded_canonical_encoding() {
    let effect = dummy_effect(vec![LadybugFileChunkV1 {
        chunk_index: 0,
        after_image: vec![7; LGFX_CHUNK_BYTES],
    }]);

    let encoded = effect.encode().unwrap();

    assert!(encoded.starts_with(LGFX_V1_MAGIC));
    assert!(encoded.len() <= MAX_LGFX_V1_BYTES);
    assert_eq!(LadybugFileEffectV1::decode(&encoded).unwrap(), effect);
}

#[test]
fn lgfx_rejects_unknown_result_encoding_version() {
    let mut effect = dummy_effect(vec![LadybugFileChunkV1 {
        chunk_index: 0,
        after_image: vec![7; LGFX_CHUNK_BYTES],
    }]);
    effect.result_encoding_version = 2;

    assert!(matches!(effect.encode(), Err(Error::InvalidEntry(_))));
}

#[test]
fn lgfx_decoder_rejects_corruption_truncation_oversize_and_noncanonical_bytes() {
    let encoded = dummy_effect(vec![LadybugFileChunkV1 {
        chunk_index: 0,
        after_image: vec![7; LGFX_CHUNK_BYTES],
    }])
    .encode()
    .unwrap();

    let mut corrupt = encoded.clone();
    *corrupt.last_mut().unwrap() ^= 0xff;
    assert!(matches!(
        LadybugFileEffectV1::decode(&corrupt),
        Err(Error::InvalidEntry(_))
    ));

    let mut truncated = encoded.clone();
    truncated.pop();
    assert!(matches!(
        LadybugFileEffectV1::decode(&truncated),
        Err(Error::InvalidEntry(_))
    ));

    let oversized = vec![0; MAX_LGFX_V1_BYTES + 1];
    assert!(matches!(
        LadybugFileEffectV1::decode(&oversized),
        Err(Error::ResourceExhausted(_))
    ));

    let mut noncanonical = encoded;
    let configuration_offset = LGFX_V1_MAGIC.len() + 1 + "cluster-1".len() + 1;
    assert_eq!(noncanonical[configuration_offset], 3);
    noncanonical[configuration_offset] = 0x83;
    noncanonical.insert(configuration_offset + 1, 0);
    assert!(matches!(
        LadybugFileEffectV1::decode(&noncanonical),
        Err(Error::InvalidEntry(_))
    ));
}

#[test]
fn lgfx_validation_enforces_chunk_order_bounds_digest_and_complete_growth() {
    let chunk = LadybugFileChunkV1 {
        chunk_index: 0,
        after_image: vec![0; LGFX_CHUNK_BYTES],
    };
    let mut duplicate = dummy_effect(vec![chunk.clone(), chunk.clone()]);
    duplicate.chunks_digest = lgfx_chunks_digest(&duplicate.chunks);
    assert!(duplicate.validate().is_err());

    let mut short = dummy_effect(vec![LadybugFileChunkV1 {
        chunk_index: 0,
        after_image: vec![0; LGFX_CHUNK_BYTES - 1],
    }]);
    short.chunks_digest = lgfx_chunks_digest(&short.chunks);
    assert!(short.validate().is_err());

    let mut outside = dummy_effect(vec![LadybugFileChunkV1 {
        chunk_index: 1,
        after_image: vec![0; LGFX_CHUNK_BYTES],
    }]);
    outside.chunks_digest = lgfx_chunks_digest(&outside.chunks);
    assert!(outside.validate().is_err());

    let mut bad_digest = dummy_effect(vec![chunk]);
    bad_digest.chunks_digest = LogHash::ZERO;
    assert!(bad_digest.validate().is_err());

    let mut incomplete_growth = dummy_effect(vec![LadybugFileChunkV1 {
        chunk_index: 2,
        after_image: vec![0; LGFX_CHUNK_BYTES],
    }]);
    incomplete_growth.target_file_bytes = (LGFX_CHUNK_BYTES * 3) as u64;
    incomplete_growth.chunks_digest = lgfx_chunks_digest(&incomplete_growth.chunks);
    assert!(incomplete_growth.validate().is_err());

    let oversized_chunks = (0..=MAX_LGFX_V1_BYTES / LGFX_CHUNK_BYTES)
        .map(|chunk_index| LadybugFileChunkV1 {
            chunk_index: chunk_index as u64,
            after_image: vec![0; LGFX_CHUNK_BYTES],
        })
        .collect::<Vec<_>>();
    let mut oversized = dummy_effect(oversized_chunks);
    oversized.target_file_bytes = (oversized.chunks.len() * LGFX_CHUNK_BYTES) as u64;
    oversized.chunks_digest = lgfx_chunks_digest(&oversized.chunks);
    assert!(matches!(
        oversized.encode(),
        Err(Error::ResourceExhausted(_))
    ));
}

#[test]
fn lgfx_apply_covers_complete_file_growth_and_truncation() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base.lbug");
    let grown = dir.path().join("grown.lbug");
    let applied_grown = dir.path().join("applied-grown.lbug");
    let shrunk = dir.path().join("shrunk.lbug");
    let applied_shrunk = dir.path().join("applied-shrunk.lbug");
    fs::write(&base, vec![1; LGFX_CHUNK_BYTES * 2]).unwrap();
    let mut grown_bytes = vec![1; LGFX_CHUNK_BYTES * 3];
    grown_bytes[LGFX_CHUNK_BYTES / 2] = 9;
    grown_bytes[LGFX_CHUNK_BYTES * 2..].fill(3);
    fs::write(&grown, &grown_bytes).unwrap();

    let growth = effect_for(&base, &grown, 0, LogHash::ZERO, "growth");
    apply_lgfx_to_exact_base(&base, &applied_grown, &growth).unwrap();
    assert_eq!(fs::read(&applied_grown).unwrap(), grown_bytes);

    fs::write(&shrunk, vec![5; LGFX_CHUNK_BYTES]).unwrap();
    let truncation = effect_for(&applied_grown, &shrunk, 1, LogHash::ZERO, "truncation");
    apply_lgfx_to_exact_base(&applied_grown, &applied_shrunk, &truncation).unwrap();
    assert_eq!(
        fs::read(&applied_shrunk).unwrap(),
        fs::read(&shrunk).unwrap()
    );
}

#[test]
fn lgfx_diff_rejects_changed_chunks_beyond_the_inline_budget() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base.lbug");
    let target = dir.path().join("target.lbug");
    fs::write(&base, vec![0; LGFX_CHUNK_BYTES]).unwrap();
    fs::write(
        &target,
        vec![1; LGFX_CHUNK_BYTES * (MAX_LGFX_V1_BYTES / LGFX_CHUNK_BYTES + 1)],
    )
    .unwrap();

    assert!(matches!(
        diff_closed_ladybug_files(&base, &target),
        Err(Error::ResourceExhausted(_))
    ));
}

#[test]
fn lgfx_apply_rejects_invalid_inputs_without_mutating_output() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base.lbug");
    let intended = dir.path().join("intended.lbug");
    let output = dir.path().join("output.lbug");
    fs::write(&base, vec![1; LGFX_CHUNK_BYTES]).unwrap();
    fs::write(&intended, vec![2; LGFX_CHUNK_BYTES]).unwrap();
    let effect = effect_for(&base, &intended, 0, LogHash::ZERO, "invalid-inputs");

    fs::write(&base, vec![1; LGFX_CHUNK_BYTES * 2]).unwrap();
    assert!(apply_lgfx_to_exact_base(&base, &output, &effect).is_err());
    assert!(!output.exists());

    fs::write(&base, vec![3; LGFX_CHUNK_BYTES]).unwrap();
    assert!(apply_lgfx_to_exact_base(&base, &output, &effect).is_err());
    assert!(!output.exists());

    fs::write(&base, vec![1; LGFX_CHUNK_BYTES]).unwrap();
    let mut bad_target_digest = effect.clone();
    bad_target_digest.target_db_digest = LogHash::ZERO;
    assert!(apply_lgfx_to_exact_base(&base, &output, &bad_target_digest).is_err());
    assert!(!output.exists());

    fs::write(&output, b"occupied target bytes").unwrap();
    assert!(apply_lgfx_to_exact_base(&base, &output, &effect).is_err());
    assert_eq!(fs::read(&output).unwrap(), b"occupied target bytes");
}

#[cfg(unix)]
#[test]
fn lgfx_apply_rejects_a_symlink_target_without_mutating_its_referent() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base.lbug");
    let intended = dir.path().join("intended.lbug");
    let victim = dir.path().join("victim.lbug");
    let output = dir.path().join("output.lbug");
    fs::write(&base, vec![1; LGFX_CHUNK_BYTES]).unwrap();
    fs::write(&intended, vec![2; LGFX_CHUNK_BYTES]).unwrap();
    fs::write(&victim, b"victim bytes").unwrap();
    symlink(&victim, &output).unwrap();
    let effect = effect_for(&base, &intended, 0, LogHash::ZERO, "symlink-target");

    assert!(apply_lgfx_to_exact_base(&base, &output, &effect).is_err());

    assert_eq!(fs::read(&victim).unwrap(), b"victim bytes");
    assert_eq!(fs::read_link(&output).unwrap(), victim);
}

#[cfg(unix)]
#[test]
fn native_clone_rejects_a_dangling_symlink_without_creating_its_referent() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base.lbug");
    let victim = dir.path().join("missing-victim.lbug");
    let staging = dir.path().join("staging.lbug");
    seed_base(&base);
    symlink(&victim, &staging).unwrap();
    let entry = representative_entry();

    assert!(
        capture_graph_entry_native_wal(&base, &staging, "cluster-1", "node-1", 7, 3, &entry,)
            .is_err()
    );

    assert!(!victim.exists());
    assert_eq!(fs::read_link(&staging).unwrap(), victim);
}

#[test]
fn native_clone_rejects_an_occupied_destination_without_mutating_it() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base.lbug");
    let staging = dir.path().join("staging.lbug");
    seed_base(&base);
    fs::write(&staging, b"occupied staging bytes").unwrap();
    let entry = representative_entry();

    assert!(
        capture_graph_entry_native_wal(&base, &staging, "cluster-1", "node-1", 7, 3, &entry,)
            .is_err()
    );

    assert_eq!(fs::read(&staging).unwrap(), b"occupied staging bytes");
}

#[test]
fn lgfx_applied_bytes_equal_the_direct_checkpoint_target() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base.lbug");
    let direct = dir.path().join("direct.lbug");
    let applied = dir.path().join("applied.lbug");
    seed_base(&base);
    let entry = representative_entry();
    capture_graph_entry_native_wal(&base, &direct, "cluster-1", "node-1", 7, 3, &entry).unwrap();
    let effect = effect_for(&base, &direct, 0, LogHash::ZERO, "transaction-1");
    let decoded = LadybugFileEffectV1::decode(&effect.encode().unwrap()).unwrap();

    let applied_digest = apply_lgfx_to_exact_base(&base, &applied, &decoded).unwrap();

    assert_eq!(applied_digest, file_digest(&direct));
    assert_eq!(fs::read(&applied).unwrap(), fs::read(&direct).unwrap());
    assert_clean(&applied);
}

#[test]
fn native_wal_replay_restores_logical_graph_state() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base.lbug");
    let direct = dir.path().join("direct.lbug");
    let replayed = dir.path().join("replayed.lbug");
    seed_base(&base);
    let entry = representative_entry();
    let capture =
        capture_graph_entry_native_wal(&base, &direct, "cluster-1", "node-1", 7, 3, &entry)
            .unwrap();

    replay_native_ladybug_wal(&base, &replayed, &capture.wal_payload).unwrap();
    assert_clean(&replayed);
    let recovered = open_lgfx_readback(&replayed, "cluster-1", "node-1", 7, 3).unwrap();
    assert_representative_state(&recovered);
}

#[test]
fn clean_lgfx_reopen_preserves_bytes_and_logical_state() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base.lbug");
    let direct = dir.path().join("direct.lbug");
    let applied = dir.path().join("applied.lbug");
    seed_base(&base);
    let entry = representative_entry();
    capture_graph_entry_native_wal(&base, &direct, "cluster-1", "node-1", 7, 3, &entry).unwrap();
    let effect = effect_for(&base, &direct, 0, LogHash::ZERO, "transaction-1");
    apply_lgfx_to_exact_base(&base, &applied, &effect).unwrap();
    let before = fs::read(&applied).unwrap();

    let reopened = open_lgfx_readback(&applied, "cluster-1", "node-1", 7, 3).unwrap();
    assert_representative_state(&reopened);
    drop(reopened);

    assert_clean(&applied);
    assert_eq!(fs::read(&applied).unwrap(), before);
}

#[test]
fn lgfx_readback_rejects_ladybug_sidecars_without_modifying_the_database() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base.lbug");
    seed_base(&base);
    let original = fs::read(&base).unwrap();

    for suffix in [".wal", ".wal.checkpoint", ".shadow", ".tmp"] {
        let sidecar = appended_suffix(&base, suffix);
        fs::write(&sidecar, b"stale sidecar bytes").unwrap();

        assert!(open_lgfx_readback(&base, "cluster-1", "node-1", 7, 3).is_err());
        assert_eq!(fs::read(&base).unwrap(), original);
        assert_eq!(fs::read(&sidecar).unwrap(), b"stale sidecar bytes");

        fs::remove_file(sidecar).unwrap();
    }
}

#[cfg(unix)]
#[test]
fn lgfx_readback_rejects_dangling_sidecar_symlinks_without_creating_referents() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base.lbug");
    seed_base(&base);
    let original = fs::read(&base).unwrap();

    for suffix in [".wal", ".wal.checkpoint", ".shadow", ".tmp"] {
        let sidecar = appended_suffix(&base, suffix);
        let referent = dir.path().join(format!("missing-{suffix:?}"));
        symlink(&referent, &sidecar).unwrap();

        assert!(open_lgfx_readback(&base, "cluster-1", "node-1", 7, 3).is_err());
        assert_eq!(fs::read(&base).unwrap(), original);
        assert_eq!(fs::read_link(&sidecar).unwrap(), referent);
        assert!(!referent.exists());

        fs::remove_file(sidecar).unwrap();
    }
}

#[test]
fn sequential_lgfx_effects_chain_from_the_exact_prior_target() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base.lbug");
    let direct_one = dir.path().join("direct-one.lbug");
    let target_one = dir.path().join("target-one.lbug");
    let direct_two = dir.path().join("direct-two.lbug");
    let target_two = dir.path().join("target-two.lbug");
    seed_base(&base);
    let first = graph_entry(
        1,
        LogHash::ZERO,
        &[GraphCommandV1::put_document("first", "item", GraphValueV1::U64(1)).unwrap()],
    );
    capture_graph_entry_native_wal(&base, &direct_one, "cluster-1", "node-1", 7, 3, &first)
        .unwrap();
    let effect_one = effect_for(&base, &direct_one, 0, LogHash::ZERO, "first");
    apply_lgfx_to_exact_base(&base, &target_one, &effect_one).unwrap();

    let second = graph_entry(
        2,
        first.hash,
        &[
            GraphCommandV1::put_document("second", "item", GraphValueV1::U64(2)).unwrap(),
            GraphCommandV1::put_document("temporary", "gone", GraphValueV1::Bool(true)).unwrap(),
            GraphCommandV1::delete_document("remove", "gone").unwrap(),
        ],
    );
    capture_graph_entry_native_wal(
        &target_one,
        &direct_two,
        "cluster-1",
        "node-1",
        7,
        3,
        &second,
    )
    .unwrap();
    let effect_two = effect_for(&target_one, &direct_two, 1, first.hash, "second");
    apply_lgfx_to_exact_base(&target_one, &target_two, &effect_two).unwrap();

    assert_eq!(
        fs::read(&target_two).unwrap(),
        fs::read(&direct_two).unwrap()
    );
    let reopened = open_lgfx_readback(&target_two, "cluster-1", "node-1", 7, 3).unwrap();
    assert_eq!(
        reopened.get_document("item").unwrap(),
        Some(GraphValueV1::U64(2))
    );
    assert_eq!(reopened.get_document("gone").unwrap(), None);
}

fn representative_entry() -> LogEntry {
    graph_entry(
        1,
        LogHash::ZERO,
        &[
            GraphCommandV1::put_document("put", "kept", GraphValueV1::U64(1)).unwrap(),
            GraphCommandV1::put_document("update", "kept", GraphValueV1::U64(2)).unwrap(),
            GraphCommandV1::put_document("put-deleted", "deleted", GraphValueV1::Bool(true))
                .unwrap(),
            GraphCommandV1::delete_document("delete", "deleted").unwrap(),
        ],
    )
}

fn assert_representative_state(state: &LadybugStateMachine) {
    assert_eq!(
        state.get_document("kept").unwrap(),
        Some(GraphValueV1::U64(2))
    );
    assert_eq!(state.get_document("deleted").unwrap(), None);
}

fn file_digest(path: &Path) -> LogHash {
    LogHash::digest(&[&fs::read(path).unwrap()])
}

fn assert_clean(path: &Path) {
    for suffix in [".wal", ".wal.checkpoint", ".shadow", ".tmp"] {
        let mut sidecar = path.as_os_str().to_os_string();
        sidecar.push(suffix);
        assert!(!Path::new(&sidecar).exists(), "stale sidecar: {sidecar:?}");
    }
}

fn appended_suffix(path: &Path, suffix: &str) -> std::path::PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    value.into()
}
