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
4. Review the exact tag, source commit, package, and `SHA256SUMS` bytes.
5. Configure the repository's protected `release` environment before pushing
   a tag. Published versions are immutable; recovery always uses a new SemVer.

The component is built from checked-in WIT and core WAT, then validated and
packed into Sigil's canonical P3 archive:

```bash
just check
just dist
just sigil-check
```

The bootstrap workflows pin `wasm-tools`, zstd 1.5.7 source, and every Action
commit. The small compatibility packer is byte-identical to Sigil P3 and
avoids depending on an unreleased Sigil command; installation still performs
Sigil's complete manifest, component, archive, and digest validation.

The release workflow produces only `NAME-VERSION.sigil-plugin.tar.zst` and
`SHA256SUMS`. A capability request is not a capability grant, and installation
is not a project evaluation lock.
