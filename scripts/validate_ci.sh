#!/bin/bash
# scripts/validate_ci.sh

set -e

echo "=== Validating CI Pipeline ==="

echo "1. Checking workflow syntax..."
# Requires 'yq' or similar, skipping strict syntax check for now if not present
if command -v yq &> /dev/null; then
    for f in .github/workflows/*.yml; do
        echo "  Validating $f"
        yq . "$f" > /dev/null || { echo "Invalid YAML: $f"; exit 1; }
    done
else
    echo "  (Skipping YAML syntax check: yq not found)"
fi

echo "2. Running local CI simulation..."
echo "  - Checking formatting..."
cargo fmt --all -- --check

echo "  - Running Clippy..."
cargo clippy --all-targets --all-features -- -D warnings

echo "  - Running Rust tests..."
cargo test --all-features

echo "  - Running Crypto Vector tests..."
cargo test --test crypto_vectors

echo "=== CI Validation Complete ==="
