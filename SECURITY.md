# Security policy

Report vulnerabilities privately through GitHub's security-advisory flow.
Do not include credentials or non-public Sigil scenarios in an issue.

Release versions are immutable. If a public tag or canonical asset is wrong,
publish a new SemVer after review; never replace or delete released bytes.
Release workflows use only GitHub's scoped token and public, checksum-verified
tool archives. Plugin packages must contain no credentials or ambient host
authority.

