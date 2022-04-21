.PHONY : compile test clean

CARGO=cargo

compile:
	${CARGO} build --color=always --all --all-targets

release:
	${CARGO} build --release --color=always --all --all-targets

test:
	RUST_BACKTRACE=1 ${CARGO} test -- --nocapture

clean:
	${CARGO} clean
