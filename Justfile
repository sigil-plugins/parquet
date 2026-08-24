set shell := ["bash", "-euo", "pipefail", "-c"]

wasm_tools := env_var_or_default("WASM_TOOLS", "wasm-tools")
sigil := env_var_or_default("SIGIL", "sigil")
python := env_var_or_default("PYTHON", "python3")

build:
    mkdir -p build
    {{wasm_tools}} component embed wit/plugin.wit --world plugin src/plugin.core.wat -o build/plugin.embedded.wasm
    {{wasm_tools}} component new build/plugin.embedded.wasm -o plugin.wasm
    {{wasm_tools}} component targets wit/plugin.wit --world example:plugin/plugin@1.0.0 plugin.wasm

check: build
    {{wasm_tools}} validate --features all plugin.wasm

sigil-check: check
    {{sigil}} plugin validate plugin.toml
    {{sigil}} plugin inspect plugin.toml --format json

dist: check
    mkdir -p dist
    {{python}} scripts/pack.py plugin.toml dist

release-dist source_commit: check
    mkdir -p dist
    {{python}} scripts/pack.py plugin.toml dist --source-commit "{{source_commit}}"
