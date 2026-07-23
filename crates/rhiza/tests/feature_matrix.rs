use rhizadb::ExecutionProfile;

#[test]
fn sql_profile_is_always_available() {
    assert_eq!(ExecutionProfile::Sqlite.as_str(), "sql");
}
