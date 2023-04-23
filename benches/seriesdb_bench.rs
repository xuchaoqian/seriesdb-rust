use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use seriesdb::prelude::*;

fn bench_db(c: &mut Criterion) {
  let db = NormalDb::open("./data/seriesdb_bench", &mut Options::new()).unwrap();
  let db = Arc::new(db);
  c.bench_function("open_table", |b| {
    b.iter(|| {
      for _ in 0..black_box(1000000) {
        db.open_table("huobi.btc.usdt.1min").unwrap();
      }
    })
  });
  c.bench_function("create_table", |b| {
    b.iter(|| {
      for _ in 0..black_box(1000) {
        db.create_table("huobi.btc.usdt.1min").unwrap();
      }
    })
  });
}

criterion_group!(benches, bench_db);
criterion_main!(benches);
