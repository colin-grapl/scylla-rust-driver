use criterion::{criterion_group, criterion_main, Criterion};

use scylla_cql::frame::{compress_append, old_compress_append, Compression};

const HEADER_SIZE: usize = 9;

fn compress_append_bench(c: &mut Criterion) {
    let compression = Compression::Lz4;

    c.bench_function("compress_append_bench.small_request", |b| {
        let mut data = vec![0; HEADER_SIZE];
        let uncomp_body = b"SELECT * FROM table_name;".to_vec();
        b.iter(|| {
            compress_append(&uncomp_body[..], compression, &mut data).unwrap();
            data.clear();
            data.resize(HEADER_SIZE, 0);
        })
    });

    c.bench_function("compress_append_bench.medium_request", |b| {
        let mut data = vec![0; HEADER_SIZE];
        let mut uncomp_body = b"INSERT foo, bar, baz INTO ks.table_name VALUES (?, ?, ?);".to_vec();
        uncomp_body.extend([0, 0, 0, 1]);
        uncomp_body.extend([0, 0, 0, 2]);
        uncomp_body.extend([0, 0, 0, 3]);

        b.iter(|| {
            compress_append(&uncomp_body[..], compression, &mut data).unwrap();
            data.clear();
            data.resize(HEADER_SIZE, 0);
        })
    });

    c.bench_function("compress_append_bench.larger_request", |b| {
        let mut data = vec![0; HEADER_SIZE];
        let mut uncomp_body =
            b"INSERT blorp, foo, bar, baz INTO ks.table_name VALUES (?, ?, ?, ?);".to_vec();
        uncomp_body.extend([0, 0, 0, 1]);
        uncomp_body.extend([0, 0, 0, 2]);
        uncomp_body.extend([0, 0, 0, 3]);
        uncomp_body.extend([0, 0, 0, 4]);

        b.iter(|| {
            compress_append(&uncomp_body[..], compression, &mut data).unwrap();
            data.clear();
            data.resize(HEADER_SIZE, 0);
        })
    });
}

criterion_group!(benches, compress_append_bench);
criterion_main!(benches);
