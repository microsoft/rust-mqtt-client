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
		cargo clippy \
			--features "$$feature_set" \
			--tests \
			--examples \
			-- \
			--deny=warnings; \
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
	cargo machete
	cargo deny check	# TODO: split out advisory checks from bans/licenses/sources when CI is more fleshed out
