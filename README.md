# Sigil plugin template

This public template is the minimal standalone shape for a pure Sigil
WebAssembly Component Model plugin. It deliberately has no `sigil-plugin`
topic: repositories created from it become discoverable only after their
source, manifest, release policy, and canonical package are reviewed.

Before publishing a derived plugin:

1. Replace the example package/interface/name/source in `wit/`, `src/`, and
   `plugin.toml`.
2. Keep the plugin capability-free unless it imports a ratified Sigil host
   contract and declares exactly the matching capabilities.
3. Run `just check` with `wasm-tools 1.252.0`, then run `just sigil-check`
   with a Sigil release that provides the `plugin` command.
4. Configure the protected `release` environment to allow only `main` and to
   require an explicit human reviewer.
5. Dispatch `prepare-release` from the reviewed `main` commit, reproduce its
   package, `SHA256SUMS`, and canonical `release-manifest.json` locally, then
   review the exact candidate run and digests.
6. Dispatch `publish-release` once with that approved tuple. Existing or
   partial tags, releases, or attestations burn the SemVer; recovery always
   prepares and approves a new version.

The component is built from checked-in WIT and core WAT, then validated and
packed into Sigil's canonical P3 archive:

```bash
just check
just dist
just sigil-check
```

The workflows pin `wasm-tools`, zstd 1.5.7 source, and every Action commit. The
small compatibility packer is byte-identical to Sigil and avoids depending on
an unreleased Sigil command. The publisher uses only the ephemeral GitHub token
and GitHub OIDC: there is no long-lived signing secret. Its exact
`workflow_dispatch`/`main`/`release` identity is part of the Sigstore proof.

The immutable release contains exactly `NAME-VERSION.sigil-plugin.tar.zst`,
`SHA256SUMS`, and `release-manifest.json`; the attestation is read through
GitHub's artifact-attestations API. Sigil's closed official provenance profile
applies only to reviewed `sigil-plugins/*` repositories. A derived third-party
repository remains third-party evidence even if it uses the same workflow. A
capability request is not a capability grant, and installation is not a
project evaluation lock.
