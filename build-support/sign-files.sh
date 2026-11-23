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

# Signs each file with the default GPG key, into <file>.asc, and writes its SHA-512 checksum to <file>.sha512. See
# RELEASE.md.
#
# Usage: build-support/sign-files.sh <file>...

set -euo pipefail

if [[ $# -eq 0 ]]; then
    echo "Usage: $0 <file>..." >&2
    exit 1
fi

for FILE in "$@"; do
    echo "Signing $FILE"
    # In the file's directory, so that the checksum file names the file alone and `shasum -c` works where it lands.
    (
        cd "$(dirname "$FILE")"
        NAME=$(basename "$FILE")
        gpg --armor --output "$NAME.asc" --detach-sig "$NAME"
        shasum -a 512 "$NAME" > "$NAME.sha512"
    )
done
