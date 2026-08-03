.PHONY: default
default:
	cargo build


.PHONY: clean
clean:
	rm -rf Cargo.lock target/ freestanding/Cargo.lock freestanding/target/


.PHONY: test
test:
	cargo test --lib
# `--tests` rather than `--test '*'`: the glob counts as explicit target selection,
# so Cargo hard-errors on a target whose `required-features` are unmet (the live
# network suites). Bulk selection skips those instead.
	set -eu; \
	for feature_set in '__integration' 'websockets,__integration'; do \
		cargo test --features "$$feature_set" --tests; \
	done


# Live tests against a real broker. BROKER selects only which broker gets
# provisioned -- the suite itself is broker-agnostic. Each broker directory exposes
# up.sh/down.sh, so a broker that isn't a container (AIO MQ needs a k3d cluster)
# plugs in the same way.
# Deliberately not part of `test`: these need a broker running.
BROKER ?= mosquitto
NETWORK_BROKER_DIR = tests/network/brokers/$(BROKER)

.PHONY: network-test
network-test:
	$(NETWORK_BROKER_DIR)/up.sh
# Teardown shares one shell with the test run so it happens even on failure.
	set -u; \
	MQTT_BROKER=$(BROKER) cargo test --features __network --test network; \
	status=$$?; \
	$(NETWORK_BROKER_DIR)/down.sh; \
	exit $$status


.PHONY: coverage
coverage:
	cargo llvm-cov clean --workspace
	cargo llvm-cov --no-report --lib
	set -eu; \
	# Run tests with different feature sets to get coverage for all code paths.
	for feature_set in '__integration' 'websockets,__integration'; do \
		cargo llvm-cov --no-report --features "$$feature_set" --tests; \
	done
	cargo llvm-cov report --html
	cargo llvm-cov report --summary-only


.PHONY: check
check:
	cargo fmt --verbose --all --check
# `__network` is enabled here (and nowhere in `test`) so the live network suites
# are compiled and linted on every check, without ever being run.
	set -eu; \
	for feature_set in '__integration,__network' 'websockets,__integration,__network'; do \
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
