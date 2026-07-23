#!/bin/bash

cargo install \
    --version '^0.8' \
    --locked \
    cargo-machete

cargo install \
    --version '^0.6' \
    --locked \
    cargo-llvm-cov

cargo install \
    --version '^0.18' \
    --locked \
    cargo-deny
