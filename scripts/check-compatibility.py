#!/usr/bin/env python3
"""Keep the stable Parquet and S3 composition documentation truthful."""

from pathlib import Path
import tomllib


ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    manifest = tomllib.loads((ROOT / "plugin.toml").read_text(encoding="utf-8"))
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    expected = {
        "version": "0.2.0",
        "host_api": "^1.0",
        "sigil": ">=0.31.0, <1.0.0",
    }
    observed = {
        "version": manifest["version"],
        "host_api": manifest["requires"]["host_api"],
        "sigil": manifest["requires"]["sigil"],
    }
    if observed != expected:
        raise SystemExit(f"incompatible Parquet contract: {observed!r} != {expected!r}")

    for claim in (
        "requires Sigil 0.31.0 or newer",
        "public stable S3 0.3.0 tagged-auth API",
        "stable Sigil 0.33.2 or newer",
        "S3 0.3.0 requires Host API 1.2",
        'auth = { tag = "sigv4", value = "object-store-read" }',
        "sigil plugin add s3@0.3.0",
        "sigil plugin add parquet@0.1.1",
        "unreleased 0.2.0 candidate",
        '["is-adjusted-to-utc"] == false',
        "dist/parquet-0.2.0.sigil-plugin.tar.zst",
        "just candidate-check",
    ):
        if claim not in readme:
            raise SystemExit(f"README is missing compatibility claim: {claim!r}")

    stale = (
        "public stable S3 0.1.0 endpoint API",
        "sigil plugin add s3@0.1.0",
    )
    for claim in stale:
        if claim in readme:
            raise SystemExit(f"README retains stale primary composition: {claim!r}")

    wit = (ROOT / "wit/plugin.wit").read_text(encoding="utf-8")
    for declaration in (
        "package sigil:parquet@0.2.0;",
        "is-adjusted-to-utc: option<bool>,",
        "is-adjusted-to-utc: bool,",
    ):
        if declaration not in wit:
            raise SystemExit(f"WIT is missing temporal contract: {declaration!r}")


if __name__ == "__main__":
    main()
