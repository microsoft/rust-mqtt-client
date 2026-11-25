.PHONY: default
default:
	cargo build


.PHONY: clean
clean:
	rm -rf Cargo.lock target/ freestanding/Cargo.lock freestanding/target/


.PHONY: test
test:
	cargo test --lib
# TEMPORARY, remove with the feature. Lib-only: the strict integration tests reject what it allows.
	cargo test --lib --features __allow_omitted_auth_method
# `--tests` skips test targets whose required features are disabled.
# `--test '*'` explicitly selects them and fails on the disabled network tests.
	set -eu; \
	for feature_set in '__integration' 'websockets,__integration'; do \
		cargo test --features "$$feature_set" --tests; \
	done


# Tests against a real MQTT server. BROKER selects the server and its setup scripts.
# Each broker provides up.sh/down.sh, including AIO MQ's local Kubernetes setup.
# These are separate from `test` because they need a running server.
BROKER ?= mosquitto
# Extra cargo features for the network suite, e.g. __allow_omitted_auth_method for aio-mq-1.6.1.
FEATURES ?=

.PHONY: network-test
network-test:
	tests/network/fixtures/up.sh $(BROKER)
# Teardown shares one shell with the test run so it happens even on failure.
	set -u; \
	MQTT_SERVER=$(BROKER) cargo test --features "__network,websockets $(FEATURES)" --test network; \
	status=$$?; \
	tests/network/fixtures/down.sh $(BROKER); \
	exit $$status


.PHONY: coverage
coverage:
	cargo llvm-cov clean --workspace
	cargo llvm-cov --no-report --features websockets,__integration --tests
	cargo llvm-cov report --html
	cargo llvm-cov report --summary-only


.PHONY: network-coverage
network-coverage:
	cargo llvm-cov clean --workspace
	tests/network/fixtures/up.sh $(BROKER)
	# Run every test with one feature set so LLVM coverage maps stay compatible.
	set -u; \
	MQTT_SERVER=$(BROKER) cargo llvm-cov --no-report \
		--features websockets,__integration,__network \
		--tests; \
	test_status=$$?; \
	tests/network/fixtures/down.sh $(BROKER); \
	down_status=$$?; \
	if test "$$test_status" -ne 0; then exit "$$test_status"; fi; \
	exit "$$down_status"
	cargo llvm-cov report --html
	cargo llvm-cov report --summary-only


.PHONY: check
check:
	python3 -B -m unittest discover -s .github/scripts -p 'test_*.py'
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
	# Advisory scans use the separately updated RustSec database, so they run
	# in the Dependency advisories workflow, not on every ordinary source PR.
	# That workflow also scans PRs changing its workflow, manifests, toolchain,
	# or reporting script. Other cargo-deny checks remain here.
	cargo deny check bans licenses sources
