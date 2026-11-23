<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Release process

How a Pulsar committer releases the Rust client, as the other Pulsar clients are released and as the
[ASF release policy](https://www.apache.org/legal/release-policy.html) requires. A release is a source tarball, signed
by the release manager and voted on dev@pulsar.apache.org. Once the vote passes, the crates are published to crates.io
from that tarball.

**Nothing goes to crates.io before the vote passes.** The release candidate carries the release's own version, and a
version published to crates.io can't be replaced.

A release is made of:

| What | Where |
|---|---|
| The source release: `apache-pulsar-client-rust-X.Y.Z-src.tar.gz`, its signature `.asc` and its checksum `.sha512` | `pulsar-client-rust-X.Y.Z-candidate-N/` in [dist dev](https://dist.apache.org/repos/dist/dev/pulsar/) during the vote, then `pulsar-client-rust-X.Y.Z/` in [dist release](https://dist.apache.org/repos/dist/release/pulsar/) |
| The crates `apache-pulsar-protocol` and `apache-pulsar`, published in that order. `apache-pulsar-testkit` isn't published. | [crates.io](https://crates.io/crates/apache-pulsar) |
| The tags `vX.Y.Z-candidate-N`, then `vX.Y.Z`, on one commit of the branch `branch-X.Y` | This repository |
| The release notes: what changed, the version of the Scalable Topics client specification the release conforms to, and the optional capabilities it implements ([docs/8-quality.md](docs/8-quality.md) §3) | The [GitHub release](https://github.com/apache/pulsar-client-rust/releases), and the Pulsar website |

Versions follow Cargo's semver: while the major version is 0, a new minor version may break the API and a patch
version doesn't. The two published crates share the workspace's version, and `apache-pulsar` depends on that exact
version of `apache-pulsar-protocol`.

## Before you start

- **A GPG key** from your apache.org address, in Pulsar's [KEYS](https://downloads.apache.org/pulsar/KEYS) file:
  [Create GPG keys to sign release artifacts](https://pulsar.apache.org/contribute/create-gpg-keys/). KEYS is in the
  release area of dist.apache.org, which takes a PMC member to change. Make the key the default in
  `~/.gnupg/gpg.conf` (`default-key <fingerprint>`), and run `export GPG_TTY=$(tty)` in the shell you sign from.
- **Tools:** git, gpg, shasum, svn, and a Rust toolchain no older than the `rust-version` in `Cargo.toml`. Checking the
  license headers uses [jbang](https://www.jbang.dev) to run Apache RAT, and it fetches a JDK if there's none.
- **A crates.io account** with an API token that can publish new crates and new versions. Whoever publishes a crate
  first is its only owner, so after the first release add other PMC members (step 7).

The commands below take the version, its branch and the candidate number from the shell:

```bash
export VERSION=0.1.0
export BRANCH=branch-${VERSION%.*}
export CANDIDATE=1
```

## 1. Prepare the branch

A minor release, X.Y.0, starts the branch `branch-X.Y` from main; a patch release reuses the branch, with its fixes
cherry-picked onto it. Start from a fresh clone, so that no local file gets in:

```bash
git clone https://github.com/apache/pulsar-client-rust.git
cd pulsar-client-rust
git checkout -b $BRANCH origin/main    # a patch release: git checkout $BRANCH
```

On the branch, commit:

- **The version, in two places in `Cargo.toml`:** `version` in `[workspace.package]`, and the exact requirement
  `version = "=X.Y.Z"` on `apache-pulsar-protocol` in `[workspace.dependencies]`. Then `cargo update --workspace`
  updates `Cargo.lock`. 0.1.0 needs no change.
- **The README's Conformance section**, if it isn't right for the release: the version of the specification the
  release conforms to, and the optional capabilities it implements. The vote links it, and the release notes copy it.

Changes that main needs too go there through a pull request.

## 2. Tag the candidate

```bash
git tag -u $USER@apache.org v$VERSION-candidate-$CANDIDATE -m "Release v$VERSION-candidate-$CANDIDATE"
git push origin $BRANCH v$VERSION-candidate-$CANDIDATE
```

CI runs on the branch; wait for it to pass.

## 3. Build, sign and stage the source release

`REPO` is the clone of step 1. In an empty directory:

```bash
svn co --depth empty https://dist.apache.org/repos/dist/dev/pulsar pulsar-dist-dev
cd pulsar-dist-dev
svn mkdir pulsar-client-rust-$VERSION-candidate-$CANDIDATE
cd pulsar-client-rust-$VERSION-candidate-$CANDIDATE
$REPO/build-support/generate-source-archive.sh $VERSION v$VERSION-candidate-$CANDIDATE .
```

The script writes `apache-pulsar-client-rust-$VERSION-src.tar.gz` from the tag, every file under
`apache-pulsar-client-rust-$VERSION-src/`, and stops if the version in the tag's `Cargo.toml` isn't `$VERSION` in both
places. Check the archive as the voters will, from "Unpack it" in
[Verifying a release candidate](#verifying-a-release-candidate). Then sign it and stage it:

```bash
$REPO/build-support/sign-files.sh apache-pulsar-client-rust-$VERSION-src.tar.gz
svn add *
svn ci -m "Staging Apache Pulsar Client Rust $VERSION candidate $CANDIDATE"
```

## 4. Vote

Send the vote from your apache.org address, in plain text:

```
To: dev@pulsar.apache.org
Subject: [VOTE] Pulsar Client Rust Release X.Y.Z Candidate N

Hi everyone,

This is the Nth release candidate for Apache Pulsar Client Rust, version X.Y.Z:
the apache-pulsar crate and its protocol core, apache-pulsar-protocol.

It conforms to version 1.0 of the Scalable Topics client specification. Its
README lists the optional capabilities it implements:
https://github.com/apache/pulsar-client-rust/blob/vX.Y.Z-candidate-N/README.md#conformance

The changes since the previous release, vP.Q.R:
https://github.com/apache/pulsar-client-rust/compare/vP.Q.R...vX.Y.Z-candidate-N

*** Please download, test and vote on this release. This vote will stay open
for at least 72 hours ***

The source release, its signature and its checksum:
https://dist.apache.org/repos/dist/dev/pulsar/pulsar-client-rust-X.Y.Z-candidate-N/

SHA-512 checksum:
<the content of apache-pulsar-client-rust-X.Y.Z-src.tar.gz.sha512>

The tag to be voted upon: vX.Y.Z-candidate-N (<commit id>)
https://github.com/apache/pulsar-client-rust/releases/tag/vX.Y.Z-candidate-N

Pulsar's KEYS file, with the PGP keys that sign releases:
https://downloads.apache.org/pulsar/KEYS

How to verify the release candidate:
https://github.com/apache/pulsar-client-rust/blob/main/RELEASE.md#verifying-a-release-candidate

The crates are published to crates.io from this source release once the vote
passes, and not before.

[ ] +1, Approve the release
[ ] -1, Do not approve the release (please provide specific comments)
```

`git rev-parse v$VERSION-candidate-$CANDIDATE^{commit}` gives the commit id. The first release has no previous one,
so its vote leaves out the changes.

The vote passes after at least 72 hours with at least three binding +1 votes, from PMC members, and more +1 than -1
votes. If the candidate has to change, fix the branch, remove the candidate from dist dev (`svn rm`), and go back to
step 2 with the next candidate number. Then send the result:

```
To: dev@pulsar.apache.org
Subject: [RESULT][VOTE] Pulsar Client Rust Release X.Y.Z Candidate N

The vote to release Apache Pulsar Client Rust X.Y.Z, candidate N, has passed
with B binding +1 votes, C non-binding +1 votes and no -1 votes.

Binding votes:
- ...

Non-binding votes:
- ...

Thanks to everyone who verified the release. I'll release it and publish the
crates.

Vote thread: https://lists.apache.org/thread/...
```

## Verifying a release candidate

For voters, and for the release manager before staging. Download the candidate's three files, with svn or curl:

```bash
export VERSION=0.1.0
export CANDIDATE=1
svn export https://dist.apache.org/repos/dist/dev/pulsar/pulsar-client-rust-$VERSION-candidate-$CANDIDATE
cd pulsar-client-rust-$VERSION-candidate-$CANDIDATE
```

```bash
mkdir pulsar-client-rust-$VERSION-candidate-$CANDIDATE && cd pulsar-client-rust-$VERSION-candidate-$CANDIDATE
for f in apache-pulsar-client-rust-$VERSION-src.tar.gz{,.asc,.sha512}; do
  curl -sSfLO https://dist.apache.org/repos/dist/dev/pulsar/pulsar-client-rust-$VERSION-candidate-$CANDIDATE/$f
done
```

The signature is good, from a key in Pulsar's KEYS file, and the checksum matches:

```bash
curl -sSfL https://downloads.apache.org/pulsar/KEYS | gpg --import
gpg --verify apache-pulsar-client-rust-$VERSION-src.tar.gz.asc apache-pulsar-client-rust-$VERSION-src.tar.gz
shasum -a 512 -c apache-pulsar-client-rust-$VERSION-src.tar.gz.sha512
```

Unpack it. It holds what the tag holds, no more and no less:

```bash
tar -xzf apache-pulsar-client-rust-$VERSION-src.tar.gz
git clone --quiet --depth 1 --branch v$VERSION-candidate-$CANDIDATE https://github.com/apache/pulsar-client-rust.git tag
diff -r --exclude .git tag apache-pulsar-client-rust-$VERSION-src && echo "Same files as the tag"
cd apache-pulsar-client-rust-$VERSION-src
```

`LICENSE` and `NOTICE` are at the top, and there's no compiled code. Every file carries the ASF license header, except
those `.rat-excludes` lists: Apache RAT reports `0 Unknown Licenses`. Use RAT 0.16.1, as CI does: later versions take
Rust sources for binary files and skip them.

```bash
curl -Ls https://sh.jbang.dev | bash -s - run org.apache.rat:apache-rat:0.16.1 -E .rat-excludes -d .
```

It builds and its tests pass. The integration tests, `cargo test -p apache-pulsar --test integration`, need Docker:

```bash
cargo test --locked
```

The crates package and build as they will be published, `apache-pulsar` against the `apache-pulsar-protocol` packaged
with it:

```bash
cargo publish --dry-run --locked -p apache-pulsar-protocol -p apache-pulsar
```

## 5. Release the source

After the vote passes, a PMC member moves the candidate to the release area:

```bash
svn mv -m "Release Apache Pulsar Client Rust $VERSION" \
  https://dist.apache.org/repos/dist/dev/pulsar/pulsar-client-rust-$VERSION-candidate-$CANDIDATE \
  https://dist.apache.org/repos/dist/release/pulsar/pulsar-client-rust-$VERSION
```

The release area keeps the latest release only; the [archive](https://archive.apache.org/dist/pulsar/) keeps every one.
Remove the previous release, if there's one:

```bash
svn ls https://dist.apache.org/repos/dist/release/pulsar | grep client-rust
svn rm -m "Remove Apache Pulsar Client Rust <previous version>" \
  https://dist.apache.org/repos/dist/release/pulsar/pulsar-client-rust-<previous version>
```

Tag the release, on the candidate's commit:

```bash
git tag -u $USER@apache.org v$VERSION "v$VERSION-candidate-$CANDIDATE^{commit}" -m "Release v$VERSION"
git push origin v$VERSION
```

## 6. Publish the crates

From the released source, in an empty directory:

```bash
svn export https://dist.apache.org/repos/dist/release/pulsar/pulsar-client-rust-$VERSION
cd pulsar-client-rust-$VERSION
gpg --verify apache-pulsar-client-rust-$VERSION-src.tar.gz.asc apache-pulsar-client-rust-$VERSION-src.tar.gz
shasum -a 512 -c apache-pulsar-client-rust-$VERSION-src.tar.gz.sha512
tar -xzf apache-pulsar-client-rust-$VERSION-src.tar.gz
cd apache-pulsar-client-rust-$VERSION-src
cargo login
cargo publish --locked -p apache-pulsar-protocol -p apache-pulsar
```

This is the command the voters ran with `--dry-run`. cargo publishes in dependency order: `apache-pulsar-protocol`
first, since `apache-pulsar` requires its exact version, and then `apache-pulsar` once crates.io serves the first. If
it stops in between, `cargo publish --locked -p apache-pulsar` publishes the second. docs.rs then builds the
documentation of both.

## 7. After the first release: crate owners

crates.io makes whoever published a crate its only owner. Add other Pulsar PMC members, who accept the invitation on
crates.io:

```bash
cargo owner --add <crates.io login> apache-pulsar-protocol
cargo owner --add <crates.io login> apache-pulsar
```

## 8. Announce

- **GitHub:** a [release](https://github.com/apache/pulsar-client-rust/releases/new) from the tag `vX.Y.Z`, whose
  notes are the release notes. They open with the statement, as the README's Conformance section has it at the tag:
  the specification version, and the table of optional capabilities. Then the changes: *Generate release notes* lists
  the pull requests merged since the previous release's tag. Then the known limitations, if there are any.
- **Website:** a pull request to [pulsar-site](https://github.com/apache/pulsar-site) that adds the release to the
  download page and its notes to the client release notes. The announcement links to the download page, so this
  comes first.
- **ASF reporter:** record the release at [reporter.apache.org](https://reporter.apache.org/addrelease.html?pulsar).
- **Email**, from your apache.org address, in plain text, since announce@apache.org rejects HTML:

```
To: dev@pulsar.apache.org, users@pulsar.apache.org, announce@apache.org
Subject: [ANNOUNCE] Apache Pulsar Client Rust X.Y.Z released

The Apache Pulsar team is proud to announce Apache Pulsar Client Rust
version X.Y.Z.

Pulsar is a highly scalable, low latency messaging platform running on
commodity hardware. It provides simple pub-sub semantics over topics,
guaranteed at-least-once delivery of messages, automatic cursor management for
subscribers, and cross-datacenter replication.

The Rust client is the apache-pulsar crate, for the scalable topics of
Pulsar 5.0 and later. It conforms to version 1.0 of the Scalable Topics client
specification.

The crates are on crates.io:
https://crates.io/crates/apache-pulsar

For the source release and its signatures, visit:
https://pulsar.apache.org/download/

Release notes:
https://github.com/apache/pulsar-client-rust/releases/tag/vX.Y.Z

We would like to thank the contributors that made the release possible.

Regards,

The Pulsar Team
```

## 9. Move main on

On main, through a pull request: the next version in both places in `Cargo.toml`.
