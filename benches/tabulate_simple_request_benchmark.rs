use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};

use cimdea::request::{DataRequest, SimpleRequest};
use cimdea::tabulate::tabulate;

fn tabulate_simple_request_benchmark(c: &mut Criterion) {
    let data_root = String::from("tests/data_root");
    let (ctx, rq) = SimpleRequest::from_names(
        "usa",
        &["us2015b"],
        &["MARST", "GQ"],
        Some("P".to_string()),
        None,
        Some(data_root),
    )
    .expect("Should be able to set up request and context");

    c.bench_function("tabulate simple request", |b| {
        b.iter(|| {
            tabulate(black_box(&ctx), black_box(rq.clone())).ok();
        })
    });
}

criterion_group!(benches, tabulate_simple_request_benchmark);
criterion_main!(benches);
