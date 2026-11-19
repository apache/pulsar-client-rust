// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Repository tasks.
//!
//! `cargo run -p xtask -- codegen` regenerates `crates/apache-pulsar-protocol/src/proto/` from the vendored
//! `PulsarApi.proto`. `cargo run -p xtask -- codegen --check` fails if the committed code is out of date.
//! Code generation needs `protoc` (v21.12 or later); building the client does not.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

const HEADER: &str = include_str!("license_header.txt");

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let result = match args.as_slice() {
        ["codegen"] => codegen(false),
        ["codegen", "--check"] => codegen(true),
        _ => Err("usage: cargo run -p xtask -- codegen [--check]".into()),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().to_path_buf()
}

fn codegen(check: bool) -> Result<(), String> {
    let crate_dir = workspace_root().join("crates/apache-pulsar-protocol");
    let proto_dir = crate_dir.join("proto");
    let committed = crate_dir.join("src/proto");
    let tmp = std::env::temp_dir().join(format!("apache-pulsar-codegen-{}", std::process::id()));
    let _ = fs::remove_dir_all(&tmp);
    fs::create_dir_all(&tmp).map_err(|e| e.to_string())?;

    buffa_build::Config::new()
        .files(&[proto_dir.join("PulsarApi.proto")])
        .includes(&[&proto_dir])
        // An unknown command type must decode as unknown, not as the first variant (CONNECT).
        .open_enums_in(&[".pulsar.proto.BaseCommand.type"])
        .out_dir(&tmp)
        .compile()
        .map_err(|e| format!("buffa codegen failed: {e}"))?;

    let mut generated = Vec::new();
    for entry in fs::read_dir(&tmp).map_err(|e| e.to_string())? {
        let path = entry.map_err(|e| e.to_string())?.path();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        let body = fs::read_to_string(&path).map_err(|e| e.to_string())?;
        generated.push((name, format!("{HEADER}\n{body}")));
    }
    generated.sort();
    let _ = fs::remove_dir_all(&tmp);

    if check {
        let mut stale = Vec::new();
        for (name, body) in &generated {
            if fs::read_to_string(committed.join(name)).ok().as_deref() != Some(body.as_str()) {
                stale.push(name.clone());
            }
        }
        let expected: Vec<&str> = generated.iter().map(|(n, _)| n.as_str()).collect();
        for entry in fs::read_dir(&committed).map_err(|e| e.to_string())? {
            let name = entry.map_err(|e| e.to_string())?.file_name().to_string_lossy().into_owned();
            if !expected.contains(&name.as_str()) {
                stale.push(format!("{name} (no longer generated)"));
            }
        }
        if !stale.is_empty() {
            return Err(format!(
                "generated protobuf code is out of date: {}. Run `cargo run -p xtask -- codegen`.",
                stale.join(", ")
            ));
        }
        println!("generated protobuf code is up to date");
    } else {
        let _ = fs::remove_dir_all(&committed);
        fs::create_dir_all(&committed).map_err(|e| e.to_string())?;
        for (name, body) in &generated {
            fs::write(committed.join(name), body).map_err(|e| e.to_string())?;
        }
        println!("wrote {} files to {}", generated.len(), committed.display());
    }
    Ok(())
}
