.PHONY: default
default:
	cargo build


.PHONY: clean
clean:
	rm -rf Cargo.lock target/ freestanding/Cargo.lock freestanding/target/


.PHONY: test
test:
	cargo test \
		--workspace \
		--features tests
	cargo clippy \
		--workspace \
		--tests \
		--examples \
		--features tests \
		-- \
		--deny=warnings


.PHONY: check
check:
	cargo fmt --verbose --all --check
	cargo machete
