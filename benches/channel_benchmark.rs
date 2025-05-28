use criterion::{criterion_group, criterion_main, Criterion};
use flume;
use logicsponge_core::channel;
use std::hint::black_box;
use std::sync::mpsc;

fn criterion_benchmark(c: &mut Criterion) {
    const N: usize = 1000;

    c.bench_function("logicsponge_core send-recv", |b| {
        b.iter(|| {
            let (tx, rx) = channel::channel();
            for k in 1..N {
                tx.send(black_box(k)).unwrap();
            }
            for _ in 1..N {
                black_box(rx.recv().unwrap());
            }
        })
    });

    c.bench_function("mpsc send-recv", |b| {
        b.iter(|| {
            let (tx, rx) = mpsc::channel();
            for k in 1..N {
                tx.send(black_box(k)).unwrap();
            }
            for _ in 1..N {
                black_box(rx.recv().unwrap());
            }
        })
    });

    c.bench_function("crossbeam send-recv", |b| {
        b.iter(|| {
            let (s, r) = crossbeam::channel::unbounded();
            for k in 1..N {
                s.send(black_box(k)).unwrap();
            }
            for _ in 1..N {
                black_box(r.recv().unwrap());
            }
        })
    });

    c.bench_function("flume send-recv", |b| {
        b.iter(|| {
            let (tx, rx) = flume::unbounded();
            for k in 1..N {
                tx.send(black_box(k)).unwrap();
            }
            for _ in 1..N {
                black_box(rx.recv().unwrap());
            }
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
