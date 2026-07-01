use chrono::{TimeZone, Utc};
use futures::{StreamExt, stream};
use vector_lib::event::{BatchNotifier, BatchStatus, Event, LogEvent};

use crate::{
    config::{SinkConfig, SinkContext},
    sinks::{duckdb::config::DuckdbConfig, util::test::load_sink},
    test_util::{
        components::{COMPONENT_ERROR_TAGS, run_and_assert_sink_error_with_events},
        random_table_name, temp_file,
    },
};

fn create_event(id: i32, message: &str) -> Event {
    let mut event = LogEvent::from(message);
    event.insert("id", id);
    event.insert("message", message);
    event.into()
}

fn prepare_config() -> (DuckdbConfig, String, String) {
    let path = temp_file().with_extension("duckdb");
    let endpoint = path.to_string_lossy().to_string();
    let table = random_table_name();

    let conn = duckdb::Connection::open(&path).expect("open duckdb database");
    conn.execute(
        &format!("CREATE TABLE {table} (id INTEGER NOT NULL, message VARCHAR)"),
        [],
    )
    .expect("create test table");
    drop(conn);

    let config_str = format!(
        r#"
            endpoint = "{endpoint}"
            table = "{table}"
            batch.max_events = 2
        "#,
    );
    let (config, _) = load_sink::<DuckdbConfig>(&config_str).unwrap();

    (config, endpoint, table)
}

#[tokio::test]
async fn build_fails_for_missing_table() {
    let path = temp_file().with_extension("duckdb");
    let endpoint = path.to_string_lossy().to_string();
    let conn = duckdb::Connection::open(&path).expect("open duckdb database");
    conn.execute("CREATE TABLE other_table (id INTEGER)", [])
        .expect("create table");
    drop(conn);

    let config_str = format!(
        r#"
            endpoint = "{endpoint}"
            table = "missing_table"
        "#,
    );
    let (config, _) = load_sink::<DuckdbConfig>(&config_str).unwrap();

    let result = config.build(SinkContext::default()).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn build_fails_for_unsupported_type() {
    let path = temp_file().with_extension("duckdb");
    let endpoint = path.to_string_lossy().to_string();
    let table = random_table_name();
    let conn = duckdb::Connection::open(&path).expect("open duckdb database");
    conn.execute(&format!("CREATE TABLE {table} (id UUID)"), [])
        .expect("create table");
    drop(conn);

    let config_str = format!(
        r#"
            endpoint = "{endpoint}"
            table = "{table}"
        "#,
    );
    let (config, _) = load_sink::<DuckdbConfig>(&config_str).unwrap();

    let result = config.build(SinkContext::default()).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn writes_to_configured_database() {
    let path = temp_file().with_extension("duckdb");
    let endpoint = path.to_string_lossy().to_string();
    let table = random_table_name();
    let conn = duckdb::Connection::open(&path).expect("open duckdb database");
    conn.execute("CREATE SCHEMA analytics", [])
        .expect("create schema");
    conn.execute(
        &format!("CREATE TABLE analytics.{table} (id INTEGER NOT NULL, message VARCHAR)"),
        [],
    )
    .expect("create test table");
    drop(conn);

    let config_str = format!(
        r#"
            endpoint = "{endpoint}"
            database = "analytics"
            table = "{table}"
            batch.max_events = 1
        "#,
    );
    let (config, _) = load_sink::<DuckdbConfig>(&config_str).unwrap();

    let (sink, healthcheck) = config
        .build(SinkContext::default())
        .await
        .expect("sink should build successfully");
    healthcheck.await.expect("healthcheck should pass");

    sink.run(Box::pin(
        stream::iter(vec![create_event(1, "one")]).map(Into::into),
    ))
    .await
    .unwrap();

    let conn = duckdb::Connection::open(endpoint).expect("open duckdb database");
    let count: i64 = conn
        .query_row(
            &format!("SELECT count(*) FROM analytics.{table}"),
            [],
            |row| row.get(0),
        )
        .expect("count rows");
    assert_eq!(count, 1);
}

#[tokio::test]
async fn missing_required_field_rejects_batch() {
    let (config, _endpoint, _table) = prepare_config();
    let (sink, healthcheck) = config
        .build(SinkContext::default())
        .await
        .expect("sink should build successfully");
    healthcheck.await.expect("healthcheck should pass");

    let mut event = LogEvent::from("missing id");
    event.insert("message", "missing id");
    let mut events = vec![Event::from(event)];
    let receiver = BatchNotifier::apply_to(&mut events);

    run_and_assert_sink_error_with_events(
        sink,
        stream::iter(events),
        &["EncoderNullConstraintError", "CallError"],
        &COMPONENT_ERROR_TAGS,
    )
    .await;
    assert_eq!(receiver.await, BatchStatus::Rejected);
}

#[tokio::test]
async fn healthcheck_passes() {
    let (config, _endpoint, _table) = prepare_config();
    let (_sink, healthcheck) = config
        .build(SinkContext::default())
        .await
        .expect("sink should build successfully");
    assert!(healthcheck.await.is_ok());
}

#[tokio::test]
async fn writes_supported_scalar_types() {
    let path = temp_file().with_extension("duckdb");
    let endpoint = path.to_string_lossy().to_string();
    let table = random_table_name();
    let conn = duckdb::Connection::open(&path).expect("open duckdb database");
    conn.execute(
        &format!(
            "CREATE TABLE {table} (\
             bool_col BOOLEAN, \
             tiny_col TINYINT, \
             small_col SMALLINT, \
             int_col INTEGER, \
             big_col BIGINT, \
            utiny_col UTINYINT, \
             usmall_col USMALLINT, \
             uint_col UINTEGER, \
             ubig_col UBIGINT, \
             float_col FLOAT, \
             double_col DOUBLE, \
             text_col VARCHAR, \
             timestamp_col TIMESTAMP, \
             decimal_col DECIMAL(10, 2), \
             nullable_col INTEGER)"
        ),
        [],
    )
    .expect("create scalar type table");
    drop(conn);

    let config_str = format!(
        r#"
            endpoint = "{endpoint}"
            table = "{table}"
            batch.max_events = 1
        "#,
    );
    let (config, _) = load_sink::<DuckdbConfig>(&config_str).unwrap();
    let (sink, healthcheck) = config
        .build(SinkContext::default())
        .await
        .expect("sink should build successfully");
    healthcheck.await.expect("healthcheck should pass");

    let mut event = LogEvent::default();
    event.insert("bool_col", true);
    event.insert("tiny_col", 12);
    event.insert("small_col", 32000);
    event.insert("int_col", 1_000_000);
    event.insert("big_col", 9_000_000_000_i64);
    event.insert("utiny_col", 255);
    event.insert("usmall_col", 65_535);
    event.insert("uint_col", 4_000_000_000_i64);
    event.insert("ubig_col", 9_000_000_000_i64);
    event.insert("float_col", 3.5);
    event.insert("double_col", 7.25);
    event.insert("text_col", "hello");
    event.insert(
        "timestamp_col",
        Utc.with_ymd_and_hms(2026, 7, 1, 12, 34, 56)
            .single()
            .unwrap(),
    );
    event.insert("decimal_col", 99.99);

    let mut events = vec![Event::from(event)];
    let receiver = BatchNotifier::apply_to(&mut events);

    sink.run(Box::pin(stream::iter(events).map(Into::into)))
        .await
        .unwrap();
    assert_eq!(receiver.await, BatchStatus::Delivered);

    let conn = duckdb::Connection::open(endpoint).expect("open duckdb database");
    let (bool_col, int_col, text_col, nullable_is_null): (bool, i32, String, bool) = conn
        .query_row(
            &format!("SELECT bool_col, int_col, text_col, nullable_col IS NULL FROM {table}"),
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("query scalar row");
    assert!(bool_col);
    assert_eq!(int_col, 1_000_000);
    assert_eq!(text_col, "hello");
    assert!(nullable_is_null);
}

#[tokio::test]
async fn writes_events() {
    let (config, endpoint, table) = prepare_config();
    let (sink, healthcheck) = config
        .build(SinkContext::default())
        .await
        .expect("sink should build successfully");
    healthcheck.await.expect("healthcheck should pass");

    let mut events = vec![create_event(1, "one"), create_event(2, "two")];
    let receiver = BatchNotifier::apply_to(&mut events);

    sink.run(Box::pin(stream::iter(events).map(Into::into)))
        .await
        .unwrap();
    assert_eq!(receiver.await, BatchStatus::Delivered);

    let conn = duckdb::Connection::open(endpoint).expect("open duckdb database");
    let count: i64 = conn
        .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .expect("count rows");
    assert_eq!(count, 2);

    let message: String = conn
        .query_row(
            &format!("SELECT message FROM {table} WHERE id = 2"),
            [],
            |row| row.get(0),
        )
        .expect("query row");
    assert_eq!(message, "two");
}
