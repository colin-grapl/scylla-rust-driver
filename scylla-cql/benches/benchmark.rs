use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};

use scylla_cql::frame::request::Request;
use scylla_cql::frame::value::SerializedValues;
use scylla_cql::frame::value::ValueList;
use scylla_cql::frame::{compress_append, request::query, Compression, SerializedRequest};
const HEADER_SIZE: usize = 9;

// Macro to avoid lifetime issues
fn make_query<'a>(contents: &'a str, values: &'a SerializedValues) -> query::Query<'a> {
    query::Query {
        contents,
        parameters: query::QueryParameters {
            consistency: scylla_cql::Consistency::LocalQuorum,
            serial_consistency: None,
            values,
            page_size: None,
            paging_state: None,
            timestamp: None,
        },
    }
}

fn serialized_request_make_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("LZ4Compression.SerializedRequest");
    let query_args = [
        ("INSERT foo INTO ks.table_name (?)", &(1234,).serialized().unwrap()),
        ("INSERT foo, bar, baz INTO ks.table_name (?, ?, ?)", &(1234, "a value", "i am storing a string").serialized().unwrap()),
        (
            "INSERT foo, bar, baz, boop, blah INTO longer_keyspace.a_big_table_name (?, ?, ?, ?, 1000)", 
            &(1234, "a value", "i am storing a string", "dc0c8cd7-d954-47c1-8722-a857941c43fb").serialized().unwrap()
        ),
    ];
    let queries = query_args.map(|(q, v)| make_query(q, v));

    for query in queries {
        let query_size = query.to_bytes().unwrap().len();
        group.bench_with_input(BenchmarkId::new("make", query_size), &query, |b, query| {
            b.iter(|| {
                let _ = criterion::black_box(SerializedRequest::make(
                    query,
                    Some(Compression::Lz4),
                    false,
                ));
            })
        });
    }
}

fn compress_append_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("LZ4Compression.compress_append");

    let query_args = [(
        "INSERT foo INTO ks.table_name (?)",
        (1234,).serialized().unwrap(),
    )];
    let queries = [make_query(&query_args[0].0, &query_args[0].1)];

    for query in queries {
        let query = query.to_bytes().unwrap();
        let mut data = Vec::with_capacity(16);
        data.resize(HEADER_SIZE, 0);

        group.throughput(Throughput::Bytes(query.len() as u64));
        let query_size = query.len();

        group.bench_with_input(BenchmarkId::new("New", query_size), &query, |b, query| {
            b.iter_batched(
                || data.clone(),
                |mut data| {
                    compress_append(&query[..], Compression::Lz4, &mut data).unwrap();
                },
                BatchSize::SmallInput,
            )
        });
    }
}

criterion_group!(
    benches,
    compress_append_bench,
    serialized_request_make_bench
);
//criterion_main!(benches);

//use iai::main;

fn serialize_lz4_for_iai() {
    let query_args = (
        "INSERT foo, bar, baz, boop, blah INTO longer_keyspace.a_big_table_name (?, ?, ?, ?, 1000)",
        &(
            1234,
            "a value",
            "i am storing a string",
            "dc0c8cd7-d954-47c1-8722-a857941c43fb",
        )
            .serialized()
            .unwrap(),
    );
    let query = make_query(query_args.0, query_args.1);

    let _ = criterion::black_box(SerializedRequest::make(
        &query,
        Some(Compression::Lz4),
        false,
    ));
}
fn serialize_none_for_iai() {
    let query_args = (
        "INSERT foo, bar, baz, boop, blah INTO longer_keyspace.a_big_table_name (?, ?, ?, ?, 1000)",
        &(
            1234,
            "a value",
            "i am storing a string",
            "dc0c8cd7-d954-47c1-8722-a857941c43fb",
        )
            .serialized()
            .unwrap(),
    );
    let query = make_query(query_args.0, query_args.1);

    let _ = criterion::black_box(SerializedRequest::make(&query, None, false));
}

iai::main!(serialize_lz4_for_iai, serialize_none_for_iai);
