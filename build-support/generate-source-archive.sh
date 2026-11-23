#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Writes the source release of a git tree, apache-pulsar-client-rust-<version>-src.tar.gz, to a directory. Every file
# in it is under apache-pulsar-client-rust-<version>-src/. See RELEASE.md.
#
# Usage: build-support/generate-source-archive.sh <version> <tree-ish> <destination directory>
# Example: build-support/generate-source-archive.sh 0.1.0 v0.1.0-candidate-1 .

set -euo pipefail

if [[ $# -ne 3 ]]; then
    echo "Usage: $0 <version> <tree-ish> <destination directory>" >&2
    exit 1
fi

VERSION=$1
TREE=$2
DEST=$3

if [[ ! $VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "The version must be X.Y.Z, without a candidate suffix: $VERSION" >&2
    exit 1
fi

mkdir -p "$DEST"
DEST=$(cd "$DEST" && pwd)
cd "$(dirname "$0")/.."
git rev-parse --verify --quiet "$TREE^{tree}" > /dev/null || { echo "Not a git tree-ish: $TREE" >&2; exit 1; }

# The crates' version is in two places: the workspace's, and apache-pulsar's exact requirement on
# apache-pulsar-protocol. Both must be the version being released.
MANIFEST=$(git show "$TREE:Cargo.toml")
WORKSPACE_VERSION=$(sed -n '/^\[workspace\.package\]/,/^\[/s/^version = "\(.*\)"$/\1/p' <<< "$MANIFEST")
PROTOCOL_REQUIREMENT=$(sed -n 's/^apache-pulsar-protocol = .*version = "=\([^"]*\)".*$/\1/p' <<< "$MANIFEST")
if [[ $WORKSPACE_VERSION != "$VERSION" || $PROTOCOL_REQUIREMENT != "$VERSION" ]]; then
    echo "Cargo.toml at $TREE has version \"$WORKSPACE_VERSION\" and requires apache-pulsar-protocol" \
        "\"=$PROTOCOL_REQUIREMENT\"; both must be $VERSION" >&2
    exit 1
fi

NAME=apache-pulsar-client-rust-$VERSION-src
git archive --format=tar.gz --prefix="$NAME/" --output="$DEST/$NAME.tar.gz" "$TREE"
echo "$DEST/$NAME.tar.gz"
