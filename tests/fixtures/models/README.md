# Model Test Fixtures

These files are used by tests that check model installation state.
They are NOT valid ONNX models but serve as presence-check fixtures.

## Files

- `model.onnx.placeholder` - Binary placeholder for model.onnx presence tests
- `tokenizer.json` - Minimal valid tokenizer config
- `config.json` - Minimal valid model config
- `special_tokens_map.json` - Standard BERT special tokens
- `tokenizer_config.json` - Tokenizer configuration

## Usage

Tests should copy these fixtures to temp directories rather than
creating synthetic "fake" content dynamically.

## No-Mock Policy

Per the project's no-mock policy (see TESTING.md), tests should use
real fixtures with documented provenance rather than synthetic data.
