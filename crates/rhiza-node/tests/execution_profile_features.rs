use rhiza_core::ExecutionProfile;
use rhiza_node::execution_profile_compiled;

#[test]
fn compiled_profiles_match_enabled_engine_features() {
    assert_eq!(
        execution_profile_compiled(ExecutionProfile::Sqlite),
        cfg!(feature = "sql")
    );
    assert_eq!(
        execution_profile_compiled(ExecutionProfile::Graph),
        cfg!(feature = "graph")
    );
    assert_eq!(
        execution_profile_compiled(ExecutionProfile::Kv),
        cfg!(feature = "kv")
    );
}
