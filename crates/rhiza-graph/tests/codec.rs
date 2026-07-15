use rhiza_graph::{GraphCommandV1, GraphValueV1};

#[test]
fn graph_command_round_trips_canonical_typed_values() {
    let values = [
        GraphValueV1::Null,
        GraphValueV1::Bool(true),
        GraphValueV1::I64(-42),
        GraphValueV1::U64(42),
        GraphValueV1::from_f64(3.5).unwrap(),
        GraphValueV1::String("rhiza".into()),
        GraphValueV1::Bytes(vec![0, 1, 2, 255]),
    ];

    for (number, value) in values.into_iter().enumerate() {
        let command = GraphCommandV1::put_document(
            format!("request-{number}"),
            format!("document-{number}"),
            value,
        )
        .unwrap();
        let encoded = command.encode();

        assert_eq!(GraphCommandV1::decode(&encoded).unwrap(), command);
        assert_eq!(command.encode(), encoded);
    }
}

#[test]
fn graph_command_rejects_noncanonical_or_nondeterministic_values() {
    assert!(GraphValueV1::from_f64(f64::NAN).is_err());
    assert!(GraphValueV1::from_f64(f64::INFINITY).is_err());

    let command = GraphCommandV1::delete_document("request-1", "document-1").unwrap();
    let mut encoded = command.encode();
    encoded.push(0);
    assert!(GraphCommandV1::decode(&encoded).is_err());
}

#[test]
fn graph_command_validates_request_and_document_limits() {
    assert!(GraphCommandV1::delete_document("", "document").is_err());
    assert!(GraphCommandV1::delete_document("request", "").is_err());
    assert!(GraphCommandV1::put_document(
        "request",
        "document",
        GraphValueV1::Bytes(vec![0; 4097]),
    )
    .is_err());
}
