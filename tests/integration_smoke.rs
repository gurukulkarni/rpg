//! Smoke tests that verify Samo can connect to a real Postgres instance.
//!
//! These tests require a running Postgres server.  Start one with:
//!
//! ```sh
//! docker compose -f docker-compose.test.yml up -d --wait
//! ```
//!
//! Then run with:
//!
//! ```sh
//! cargo test --features integration
//! ```

#![cfg(feature = "integration")]

mod common;

use common::TestDb;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Macro that skips a test with a human-readable message when the DB is
/// unreachable (e.g. Docker not running locally), rather than panicking.
macro_rules! connect_or_skip {
    () => {
        match TestDb::connect().await {
            Ok(db) => db,
            Err(e) => {
                eprintln!(
                    "skipping integration test — cannot connect to test DB: {e}\n\
                     Start Postgres with: \
                     docker compose -f docker-compose.test.yml up -d --wait"
                );
                return;
            }
        }
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Verify basic connectivity: `SELECT 1` must return the integer 1.
#[tokio::test]
async fn smoke_select_one() {
    let db = connect_or_skip!();
    let rows = db.query("select 1 as n").await.expect("select 1 failed");
    assert_eq!(rows.len(), 1, "expected exactly one row");
    let n: i32 = rows[0].get("n");
    assert_eq!(n, 1, "expected value 1");
}

/// Verify that `execute` works for DDL/DML statements.
#[tokio::test]
async fn smoke_execute() {
    let db = connect_or_skip!();
    // set_config is a void-returning function; execute is appropriate here.
    let affected = db
        .execute("select set_config('application_name', 'samo-test', true)")
        .await
        .expect("execute failed");
    // SELECT returns 1 row, execute returns rows affected
    assert_eq!(affected, 1, "expected 1 row affected");
}

/// Verify that the server version is Postgres 16.
#[tokio::test]
async fn smoke_server_version() {
    let db = connect_or_skip!();
    let rows = db
        .query("select current_setting('server_version_num')::int as v")
        .await
        .expect("server_version_num query failed");
    let version: i32 = rows[0].get("v");
    // server_version_num for PG 16.x is 160000–169999
    assert!(
        (160_000..170_000).contains(&version),
        "expected Postgres 16, got server_version_num={version}"
    );
}

/// Load the test schema fixture and run basic queries against it.
#[tokio::test]
async fn smoke_schema_and_data() {
    let db = connect_or_skip!();

    // Clean slate: drop tables if they exist from a previous run.
    db.teardown_schema().await.expect("teardown failed");

    // Apply the fixture schema + seed data.
    db.run_fixture("schema.sql")
        .await
        .expect("schema fixture failed");

    // Verify row counts match the seed data.
    let users = db.query("select count(*) as n from users").await.unwrap();
    let user_count: i64 = users[0].get("n");
    assert_eq!(user_count, 10, "expected 10 seed users, got {user_count}");

    let products = db
        .query("select count(*) as n from products")
        .await
        .unwrap();
    let product_count: i64 = products[0].get("n");
    assert_eq!(
        product_count, 10,
        "expected 10 seed products, got {product_count}"
    );

    let orders = db.query("select count(*) as n from orders").await.unwrap();
    let order_count: i64 = orders[0].get("n");
    assert_eq!(
        order_count, 12,
        "expected 12 seed orders, got {order_count}"
    );

    // Verify a join across tables works.
    let rows = db
        .query(
            "select
                 u.name as user_name,
                 count(o.id) as order_count
             from users as u
             left join orders as o
                 on o.user_id = u.id
             group by
                 u.id,
                 u.name
             order by
                 u.id",
        )
        .await
        .expect("join query failed");
    assert_eq!(rows.len(), 10, "expected 10 rows (one per user)");

    // Teardown to leave DB clean for subsequent runs.
    db.teardown_schema().await.expect("teardown failed");
}
