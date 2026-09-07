#!/bin/sh

# This assumes you have already installed cargo-sort:
# cargo install cargo-sort
#
# The best way to do this however is to run scripts/dev_setup.sh
#
# If you want to run this from anywhere in aptos-core, try adding this wrapper
# script to your path:
# https://gist.github.com/banool/e6a2b85e2fff067d3a215cbfaf808032

# Make sure we're in the root of the repo.
if [ ! -f "scripts/rust_lint.sh" ] 
then
    echo "Please run this from the aptos-indexer-processors-v2 directory." 
    exit 1
fi

# Run in check mode if requested.
CHECK_ARG=""
if [ "$1" = "--check" ]; then
    CHECK_ARG="--check"
fi

set -e
set -x

# Run clippy on the pinned STABLE toolchain (from rust-toolchain.toml), NOT
# nightly. Latest nightly makes Infallible an alias of !, which breaks
# allocative (impl Allocative for both). Stable clippy matches the build toolchain.
cargo xclippy

# We require the nightly build of cargo fmt
# to provide stricter rust formatting.
cargo +nightly fmt $CHECK_ARG

# Once cargo-sort correctly handles workspace dependencies,
# we can move to cleaner workspace dependency notation.
# See: https://github.com/DevinR528/cargo-sort/issues/47
cargo sort --grouped --workspace $CHECK_ARG