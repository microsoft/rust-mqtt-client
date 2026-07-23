.PHONY: default
default:
	cargo build


.PHONY: clean
clean:
	rm -rf Cargo.lock target/ freestanding/Cargo.lock freestanding/target/


.PHONY: test
test:
	cargo test --lib
	set -eu; \
	for feature_set in '__integration' 'websockets,__integration'; do \
		cargo test --features "$$feature_set" --test '*'; \
	done


.PHONY: coverage
coverage:
	cargo llvm-cov clean --workspace
	cargo llvm-cov --no-report --lib
	set -eu; \
	# Run tests with different feature sets to get coverage for all code paths.
	for feature_set in '__integration' 'websockets,__integration'; do \
		cargo llvm-cov --no-report --features "$$feature_set" --test '*'; \
	done
	cargo llvm-cov report --html
	cargo llvm-cov report --summary-only


.PHONY: check
check:
	cargo fmt --verbose --all --check
	set -eu; \
	for feature_set in '__integration' 'websockets,__integration'; do \
		cargo clippy \
			--features "$$feature_set" \
			--tests \
			--examples \
			-- \
			--deny=warnings; \
	done
	cargo machete
	# Advisories are deliberately excluded here: they depend on the RustSec
	# database (which updates independently of our code) and so belong in the
	# scheduled `nightly` workflow, not the deterministic PR gate. bans,
	# licenses, and sources only change when our dependencies do.
	cargo deny check bans licenses sources
