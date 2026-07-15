use std::{fs, path::Path};

use rhiza_node::{run_e2e, E2eConfig};
use rhiza_obj_store::ObjStoreConfig;

#[tokio::test]
async fn e2e_restores_sqlite_state_from_snapshot_and_archived_log() {
    let dir = tempfile::tempdir().unwrap();
    let report = run_e2e(E2eConfig {
        data_dir: dir.path().join("data"),
        object_store: ObjStoreConfig::Local {
            root: dir.path().join("objects"),
        },
        cluster_id: "cluster-a".into(),
        node_id: "node-1".into(),
    })
    .await
    .unwrap();

    assert_eq!(report.applied_index, 2);
    assert_eq!(report.restored_value, "charlie");
    assert!(report.object_keys.iter().any(|key| key.ends_with(".qlog")));
    assert!(report
        .object_keys
        .iter()
        .any(|key| key.contains("/archive/snapshots/") && key.ends_with(".snapshot")));
    assert!(report
        .object_keys
        .iter()
        .any(|key| key.ends_with("/archive/manifest.json")));
}

#[tokio::test]
async fn e2e_rejects_repeat_run_without_mutating_existing_data() {
    let dir = tempfile::tempdir().unwrap();
    let config = E2eConfig {
        data_dir: dir.path().join("data"),
        object_store: ObjStoreConfig::Local {
            root: dir.path().join("objects"),
        },
        cluster_id: "cluster-a".into(),
        node_id: "node-1".into(),
    };
    run_e2e(config.clone()).await.unwrap();
    let before = snapshot_tree(dir.path());

    let error = run_e2e(config).await.unwrap_err().to_string();

    assert!(
        error.contains("data directory is not fresh"),
        "unexpected error: {error}"
    );
    assert_eq!(snapshot_tree(dir.path()), before);
}

fn snapshot_tree(root: &Path) -> Vec<(String, Option<Vec<u8>>)> {
    fn visit(root: &Path, path: &Path, entries: &mut Vec<(String, Option<Vec<u8>>)>) {
        for entry in fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            let relative_path = path.strip_prefix(root).unwrap().to_string_lossy().into();
            if path.is_dir() {
                entries.push((relative_path, None));
                visit(root, &path, entries);
            } else {
                entries.push((relative_path, Some(fs::read(path).unwrap())));
            }
        }
    }

    let mut entries = Vec::new();
    visit(root, root, &mut entries);
    entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    entries
}
