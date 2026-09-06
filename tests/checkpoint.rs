use std::path::PathBuf;

use durable_session::Checkpoint;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/v0")
        .join(name)
}

#[test]
fn load_golden_tools_view() {
    let cp = Checkpoint::load(fixture("tools")).unwrap();
    assert_eq!(cp.session_id().as_str(), "sess_demo");
    let expected: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/v0/tools.view.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(cp.view(), expected);
}

#[test]
fn reject_seq_gap() {
    match Checkpoint::load(fixture("invalid-seq-gap")) {
        Err(err) => {
            let msg = err.to_string();
            assert!(msg.contains("seq gap"), "{msg}");
        }
        Ok(_) => panic!("expected seq gap"),
    }
}

#[test]
fn reject_unsupported_version() {
    match Checkpoint::load(fixture("invalid-version")) {
        Err(err) => {
            let msg = err.to_string();
            assert!(msg.contains("checkpoint_version"), "{msg}");
        }
        Ok(_) => panic!("expected version error"),
    }
}
