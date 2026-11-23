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

//! The environment: a Pulsar standalone broker and a Pulsar proxy on one Docker network, with TLS, token and
//! mTLS authentication.
//!
//! The certificates and tokens are generated here and copied into the containers. The host ports are fixed
//! because the broker advertises them in its lookup answers (`broker.conf`), so only one environment runs on a
//! host at a time.

use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

use base64::Engine;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose,
};
use ring::rand::{SecureRandom, SystemRandom};
use testcontainers::core::{CmdWaitFor, ExecCommand, IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

const IMAGE: (&str, &str) = ("apachepulsar/pulsar", "5.0.1");
/// A JDK, to compile the Java test tools: the broker image has a Java runtime only.
const JDK_IMAGE: (&str, &str) = ("eclipse-temurin", "21-jdk-alpine");
/// What the Java test tools compile against: the broker's V5 API, and, for v4's schemas through `V5Interop`, its V5
/// client and the v4 API.
const JAVA_TOOLS_CLASSPATH: [&str; 4] = [
    "org.apache.pulsar-pulsar-client-api-v5-5.0.1.jar",
    "org.apache.pulsar-pulsar-client-v5-5.0.1.jar",
    "org.apache.pulsar-pulsar-client-api-5.0.1.jar",
    "com.google.protobuf-protobuf-java-4.36.2.jar",
];
/// The Java test tools, by file name.
const JAVA_TOOLS: [(&str, &[u8]); 2] = [
    ("CheckpointTool.java", include_bytes!("CheckpointTool.java")),
    ("SchemaTool.java", include_bytes!("SchemaTool.java")),
];
const NETWORK: &str = "apr-it-net";
const BROKER: &str = "apr-it-broker";
const PROXY: &str = "apr-it-proxy";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(180);

pub const BROKER_URL: &str = "pulsar://localhost:26650";
pub const BROKER_TLS_URL: &str = "pulsar+ssl://localhost:26651";
pub const PROXY_URL: &str = "pulsar://localhost:26652";
pub const PROXY_TLS_URL: &str = "pulsar+ssl://localhost:26653";

/// The role of [`Env::token`]; the client certificate's role is its CN, `apr-it-mtls-client`.
pub const TOKEN_ROLE: &str = "apr-it-client";

pub struct Env {
    /// A token for [`TOKEN_ROLE`].
    pub token: String,
    dir: PathBuf,
    broker: Container<GenericImage>,
    proxy: Container<GenericImage>,
    /// Set once the Java test tools are in the broker container.
    java_tools: OnceLock<()>,
}

impl Env {
    /// Generates the secrets, starts the broker, then the proxy, and returns once both accept clients.
    pub fn start() -> Env {
        let secrets = Secrets::generate();
        let dir = std::env::temp_dir().join(format!("apr-it-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        for (name, contents) in [
            ("ca.cert.pem", &secrets.ca),
            ("client.cert.pem", &secrets.client.0),
            ("client.key.pem", &secrets.client.1),
        ] {
            std::fs::write(dir.join(name), contents).unwrap();
        }

        let broker = with_secrets(image(), &secrets)
            .with_copy_to("/it/broker.cert.pem", secrets.broker.0.clone().into_bytes())
            .with_copy_to("/it/broker.key.pem", secrets.broker.1.clone().into_bytes())
            .with_copy_to("/it/admin.token", secrets.token("apr-it-admin").into_bytes())
            .with_copy_to("/it-conf/broker.conf", include_bytes!("broker.conf").to_vec())
            .with_container_name(BROKER)
            .with_mapped_port(26650, 26650.tcp())
            .with_mapped_port(26651, 26651.tcp())
            .with_env_var("PULSAR_MEM", "-Xms256m -Xmx256m -XX:MaxDirectMemorySize=256m")
            .with_cmd([
                "bash",
                "-c",
                "cat /it-conf/broker.conf >> conf/standalone.conf && \
                 exec bin/pulsar standalone --no-functions-worker --no-stream-storage",
            ]);
        // Standalone creates public/default after the broker is up; the tests use it.
        let broker = start(broker, "namespace=public/default");

        let proxy = with_secrets(image(), &secrets)
            .with_copy_to("/it/proxy.cert.pem", secrets.proxy.0.clone().into_bytes())
            .with_copy_to("/it/proxy.key.pem", secrets.proxy.1.clone().into_bytes())
            .with_copy_to("/it/proxy.token", secrets.token("apr-it-proxy").into_bytes())
            .with_copy_to("/it-conf/proxy.conf", include_bytes!("proxy.conf").to_vec())
            .with_container_name(PROXY)
            .with_mapped_port(26652, 6650.tcp())
            .with_mapped_port(26653, 6651.tcp())
            .with_env_var("PULSAR_MEM", "-Xms128m -Xmx128m -XX:MaxDirectMemorySize=128m")
            .with_cmd(["bash", "-c", "cat /it-conf/proxy.conf >> conf/proxy.conf && exec bin/pulsar proxy"]);
        // The web server starts after the binary protocol's listeners.
        let proxy = start(proxy, "Server started at end point");

        Env { token: secrets.token(TOKEN_ROLE), dir, broker, proxy, java_tools: OnceLock::new() }
    }

    pub fn ca(&self) -> PathBuf {
        self.dir.join("ca.cert.pem")
    }

    pub fn client_cert(&self) -> PathBuf {
        self.dir.join("client.cert.pem")
    }

    pub fn client_key(&self) -> PathBuf {
        self.dir.join("client.key.pem")
    }

    /// Runs `pulsar-admin` in the broker container, as the admin, and returns what it printed.
    pub fn admin(&self, args: &[&str]) -> String {
        self.try_admin(args).unwrap_or_else(|error| panic!("pulsar-admin {args:?} failed:\n{error}"))
    }

    /// Runs `pulsar-admin` as [`Env::admin`] does, and returns what it printed, or, if it failed, what it printed on
    /// its error output.
    pub fn try_admin(&self, args: &[&str]) -> Result<String, String> {
        let mut command = vec!["bin/pulsar-admin", "--admin-url", "http://localhost:8080"];
        command.extend(TOKEN_AUTH);
        command.extend(args);
        let (code, stdout, stderr) = self.run(&command);
        if code == Some(0) { Ok(stdout) } else { Err(stderr) }
    }

    /// Starts a Java v4 consumer of `topic` on `subscription`, of type `kind` (`Exclusive`, `Shared`, …), from the
    /// earliest message, in the broker container: a `pulsar-client consume` that acknowledges each message it prints,
    /// and runs until [`Env::stop`].
    pub fn v4_consumer(&self, topic: &str, subscription: &str, kind: &str) -> Background {
        let mut command = vec!["bin/pulsar-client", "--url", "pulsar://localhost:6650"];
        command.extend(TOKEN_AUTH);
        command.extend(["consume", "-s", subscription, "-t", kind, "-p", "Earliest", "-n", "0", topic]);
        self.spawn(&command)
    }

    /// Starts a Java v4 producer of `topic` in the broker container: a `pulsar-perf produce` of ten messages a second
    /// that runs until [`Env::stop`].
    pub fn v4_producer(&self, topic: &str) -> Background {
        let mut command = vec!["bin/pulsar-perf", "produce", "-u", "pulsar://localhost:6650"];
        command.extend(TOKEN_AUTH);
        command.extend(["-r", "10", "-m", "0", topic]);
        self.spawn(&command)
    }

    /// Stops a Java consumer started in the background, and returns the messages it printed.
    pub fn stop(&self, background: Background) -> Vec<Consumed> {
        self.stop_output(background).lines().filter_map(Consumed::parse).collect()
    }

    /// What a Java client started in the background has printed so far.
    pub fn output(&self, background: &Background) -> String {
        self.exec(&["cat", &background.log])
    }

    /// Stops a Java client started in the background, and returns all it printed.
    pub fn stop_output(&self, background: Background) -> String {
        let Background { pid, log } = background;
        // The broker is the container's first process and reaps no child: a client that exited stays a zombie.
        let state = format!("awk '{{print $3}}' /proc/{pid}/stat 2>/dev/null");
        let stop = format!(
            "kill {pid}; for _ in $(seq 100); do s=$({state}); [ -z \"$s\" ] || [ \"$s\" = Z ] && break; sleep 0.1; \
             done; cat {log}"
        );
        self.exec(&["bash", "-c", &stop])
    }

    /// Runs `command` in the background in the broker container, its output going to a file of its own.
    fn spawn(&self, command: &[&str]) -> Background {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let log = format!("/tmp/background-{}.log", NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
        let quoted: Vec<String> = command.iter().map(|arg| format!("'{arg}'")).collect();
        let start = format!("nohup {} > {log} 2>&1 & echo $!", quoted.join(" "));
        Background { pid: self.exec(&["bash", "-c", &start]).trim().to_string(), log }
    }

    /// Reads `count` messages of `topic` from the beginning with the Java client in the broker container (its V5
    /// client for a `topic://` topic, a Queue consumer), as the admin, and returns them as it printed them.
    pub fn consume(&self, topic: &str, count: usize) -> Vec<Consumed> {
        let count = count.to_string();
        let subscription = format!("it-{}", std::process::id());
        let mut command = vec!["bin/pulsar-client", "--url", "pulsar://localhost:6650"];
        command.extend(TOKEN_AUTH);
        // `-mp` prints each message's id too.
        command.extend(["consume", "-s", &subscription, "-p", "Earliest", "-mp", "-n", &count, topic]);
        let output = self.exec(&command);
        output.lines().filter_map(Consumed::parse).collect()
    }

    /// Publishes with the Java client in the broker container, as the admin: `pulsar-client produce`, whose V5
    /// client serves a `topic://` topic.
    pub fn produce(&self, topic: &str, args: &[&str]) {
        let mut command = vec!["bin/pulsar-client", "--url", "pulsar://localhost:6650"];
        command.extend(TOKEN_AUTH);
        command.push("produce");
        command.extend(args);
        command.push(topic);
        self.exec(&command);
    }

    /// Publishes `count` messages of `size` bytes to a regular topic with `pulsar-perf`, the Java v4 client, in
    /// batches compressed with `compression`, as the admin.
    pub fn perf_produce(&self, topic: &str, compression: &str, count: usize, size: usize) {
        let (count, size) = (count.to_string(), size.to_string());
        let mut command = vec!["bin/pulsar-perf", "produce", "-u", "pulsar://localhost:6650"];
        command.extend(TOKEN_AUTH);
        // Half a second of messages per batch window.
        command.extend(["-z", compression, "-m", &count, "-s", &size, "-r", "200", "-b", "500", topic]);
        self.exec(&command);
    }

    /// Deletes a segment's topic, as the controller's garbage collection does once the segment is past retention:
    /// `DELETE /admin/v2/segments/…`, which `pulsar-admin` 5.0.1 has no command for. The layout still has the segment.
    pub fn delete_segment(&self, segment_topic: &str) {
        let path = segment_topic.strip_prefix("segment://").expect("a segment topic");
        let url = format!("http://localhost:8080/admin/v2/segments/{path}?force=true");
        let delete = format!("curl -sSf -X DELETE -H \"Authorization: Bearer $(cat /it/admin.token)\" '{url}'");
        self.exec(&["bash", "-c", &delete]);
    }

    /// Turns off the automatic splits and merges of scalable topic `tenant/namespace/name`, as Java's tests do to keep
    /// a layout: `POST /admin/v2/scalable/…/autoScalePolicy`, which `pulsar-admin` 5.0.1 has no command for.
    pub fn disable_auto_scale(&self, name: &str) {
        let url = format!("http://localhost:8080/admin/v2/scalable/{name}/autoScalePolicy");
        let auth = "-H \"Authorization: Bearer $(cat /it/admin.token)\"";
        let post =
            format!("curl -sSf -X POST {auth} -H 'Content-Type: application/json' -d '{{\"enabled\":false}}' '{url}'");
        self.exec(&["bash", "-c", &post]);
    }

    /// Reads `count` messages of `topic` with the Java client's Checkpoint consumer in the broker container, as the
    /// admin, from `start`: `earliest`, `latest`, or a checkpoint's bytes in hex. Returns the messages as
    /// `(key, value)`, and the consumer's checkpoint after them, in hex.
    pub fn java_checkpoint_read(&self, topic: &str, start: &str, count: usize) -> (Vec<(String, String)>, String) {
        let count = count.to_string();
        let output = self.java(&["CheckpointTool", topic, start, &count]);
        let mut messages = Vec::new();
        let mut checkpoint = None;
        for line in output.lines() {
            if let Some(message) = line.strip_prefix("message ") {
                let (key, value) = message.split_once(' ').expect("a key and a value");
                messages.push((key.to_string(), value.to_string()));
            } else if let Some(hex) = line.strip_prefix("checkpoint ") {
                checkpoint = Some(hex.to_string());
            }
        }
        (messages, checkpoint.unwrap_or_else(|| panic!("the Java tool printed no checkpoint:\n{output}")))
    }

    /// Publishes `values` to `topic` with the Java client's V5 producer, as the admin, with `schema`: the name of the
    /// V5 factory that makes it, such as `int32`. `SchemaTool.java` lists them.
    pub fn java_produce(&self, schema: &str, topic: &str, values: &[&str]) {
        self.java(&[&["SchemaTool", "produce", schema, topic], values].concat());
    }

    /// Reads `count` messages of `topic` from its start with a Java Queue consumer of `schema`, as [`Env::java_produce`]
    /// names it, and returns their values as Java prints them.
    pub fn java_consume(&self, schema: &str, topic: &str, count: usize) -> Vec<String> {
        let output = self.java(&["SchemaTool", "consume", schema, topic, &count.to_string()]);
        output.lines().filter_map(|line| line.strip_prefix("value ")).map(str::to_string).collect()
    }

    /// Why a Java Queue consumer of `schema` fails to read `count` messages of `topic`, as [`Env::java_consume`] reads
    /// them: what the Java tool printed.
    pub fn java_consume_fails(&self, schema: &str, topic: &str, count: usize) -> String {
        let read = self.try_java(&["SchemaTool", "consume", schema, topic, &count.to_string()]);
        read.expect_err("the Java consumer read the messages")
    }

    /// The schema Java declares for a Protobuf message, `PROTOBUF` or `PROTOBUF_NATIVE` as `kind` says, as the Java
    /// tool prints it: its name, type, definition and properties, a line each.
    pub fn java_schema_info(&self, kind: &str, message: &str) -> String {
        self.java(&["SchemaTool", "info", kind, message])
    }

    /// Runs one of the Java test tools in the broker container, with its arguments, and returns what it printed.
    fn java(&self, args: &[&str]) -> String {
        self.try_java(args).unwrap_or_else(|error| panic!("{args:?} failed:\n{error}"))
    }

    /// Runs one of the Java test tools as [`Env::java`] does, and returns what it printed, or, if it fails, that and
    /// what it printed on its error output.
    fn try_java(&self, args: &[&str]) -> Result<String, String> {
        self.java_tools.get_or_init(|| self.install_java_tools());
        let (code, stdout, stderr) = self.run(&[&["java", "-cp", "/pulsar/lib/*:/tmp/tools.jar"], args].concat());
        if code == Some(0) { Ok(stdout) } else { Err(format!("{stdout}{stderr}")) }
    }

    /// Compiles the Java test tools against the broker's client jars in a JDK container, and copies them into the
    /// broker container, as `/tmp/tools.jar`.
    fn install_java_tools(&self) {
        let mut jdk = GenericImage::new(JDK_IMAGE.0, JDK_IMAGE.1).with_cmd(["sleep", "600"]);
        for jar in JAVA_TOOLS_CLASSPATH {
            let bytes = self.broker.copy_file_from(format!("/pulsar/lib/{jar}"), Vec::new()).unwrap();
            jdk = jdk.with_copy_to(format!("/work/lib/{jar}"), bytes);
        }
        for (name, source) in JAVA_TOOLS {
            jdk = jdk.with_copy_to(format!("/work/src/{name}"), source.to_vec());
        }
        let jdk = jdk.start().unwrap_or_else(|error| panic!("starting {JDK_IMAGE:?} failed: {error}"));
        let build = "javac --release 21 -cp '/work/lib/*' -d /work/classes /work/src/*.java \
                     && jar cf /work/tools.jar -C /work/classes .";
        let mut result =
            jdk.exec(ExecCommand::new(["sh", "-c", build]).with_cmd_ready_condition(CmdWaitFor::exit())).unwrap();
        let errors = String::from_utf8_lossy(&result.stderr_to_vec().unwrap()).into_owned();
        assert_eq!(result.exit_code().unwrap(), Some(0), "building the Java tools failed:\n{errors}");
        let jar = jdk.copy_file_from("/work/tools.jar", Vec::new()).unwrap();
        let jar = base64::engine::general_purpose::STANDARD.encode(jar);
        self.exec(&["bash", "-c", &format!("echo {jar} | base64 -d > /tmp/tools.jar")]);
    }

    fn exec(&self, command: &[&str]) -> String {
        let (code, stdout, stderr) = self.run(command);
        assert_eq!(code, Some(0), "{command:?} failed:\n{stdout}\n{stderr}");
        stdout
    }

    /// Runs `command` in the broker container, and returns its exit code, and what it printed on its output and on its
    /// error output.
    fn run(&self, command: &[&str]) -> (Option<i64>, String, String) {
        let exec = ExecCommand::new(command.iter().copied()).with_cmd_ready_condition(CmdWaitFor::exit());
        let mut result = self.broker.exec(exec).unwrap_or_else(|e| panic!("{command:?}: {e}"));
        let stdout = String::from_utf8_lossy(&result.stdout_to_vec().unwrap()).into_owned();
        let stderr = String::from_utf8_lossy(&result.stderr_to_vec().unwrap()).into_owned();
        (result.exit_code().unwrap(), stdout, stderr)
    }

    /// Prints the last lines of both containers' logs, for a failed run.
    pub fn print_logs(&self) {
        for (name, container) in [(BROKER, &self.broker), (PROXY, &self.proxy)] {
            let logs = container.stdout_to_vec().unwrap_or_default();
            let logs = String::from_utf8_lossy(&logs);
            let lines: Vec<&str> = logs.lines().collect();
            eprintln!("---- last log lines of {name} ----");
            for line in &lines[lines.len().saturating_sub(200)..] {
                eprintln!("{line}");
            }
        }
    }
}

/// The Java CLIs' options to authenticate as the admin.
const TOKEN_AUTH: [&str; 4] = [
    "--auth-plugin",
    "org.apache.pulsar.client.impl.auth.AuthenticationToken",
    "--auth-params",
    "file:///it/admin.token",
];

/// A Java client running in the background in the broker container: its process id there, and its output's file.
#[must_use = "stop it with `Env::stop`"]
pub struct Background {
    pid: String,
    log: String,
}

/// A message as `pulsar-client consume` printed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Consumed {
    pub key: Option<String>,
    pub value: String,
    /// The position it was stored at: `(segment, ledger, entry)`, from the V5 message id.
    pub position: Option<(u64, i64, i64)>,
}

impl Consumed {
    /// Parses `…key:[k], properties:[…], content:v, message-id:MessageIdV5{segment=s, id=l:e:…}…`.
    fn parse(line: &str) -> Option<Consumed> {
        let key = line.split_once("key:[")?.1.split_once(']')?.0;
        let content = line.split_once("content:")?.1;
        // With `-mp`, the V5 client prints the message id next, and the v4 client the event time.
        let end = [", message-id:", ", event-time:"].iter().filter_map(|field| content.find(field)).min();
        let value = &content[..end.unwrap_or(content.len())];
        let position = line.split_once("segment=").and_then(|(_, rest)| {
            let (segment, rest) = rest.split_once(", id=")?;
            let mut id = rest.split(':');
            Some((segment.parse().ok()?, id.next()?.parse().ok()?, id.next()?.parse().ok()?))
        });
        Some(Consumed {
            key: (key != "null" && !key.is_empty()).then(|| key.to_string()),
            value: value.trim_end().to_string(),
            position,
        })
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn image() -> GenericImage {
    GenericImage::new(IMAGE.0, IMAGE.1)
}

/// The network, and the files both containers read: the CA and the token secret.
fn with_secrets(image: GenericImage, secrets: &Secrets) -> testcontainers::ContainerRequest<GenericImage> {
    image
        .with_network(NETWORK)
        .with_copy_to("/it/ca.cert.pem", secrets.ca.clone().into_bytes())
        .with_copy_to("/it/secret.key", secrets.token_key.clone())
        .with_startup_timeout(STARTUP_TIMEOUT)
}

fn start(request: testcontainers::ContainerRequest<GenericImage>, ready: &str) -> Container<GenericImage> {
    request.with_ready_conditions(vec![WaitFor::message_on_either_std(ready)]).start().unwrap_or_else(|error| {
        panic!(
            "starting the integration environment failed: {error}\n\
             (Docker must be running; a container left from an aborted run is removed with \
             `docker rm -f {BROKER} {PROXY}`)"
        )
    })
}

/// A CA, the broker's, the proxy's and a client's certificates as `(certificate, key)` PEM pairs, and the HS256
/// key that signs the tokens.
struct Secrets {
    ca: String,
    broker: (String, String),
    proxy: (String, String),
    client: (String, String),
    token_key: Vec<u8>,
}

impl Secrets {
    fn generate() -> Secrets {
        let ca_key = KeyPair::generate().unwrap();
        let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca.distinguished_name.push(DnType::CommonName, "apr-it-ca");
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca_pem = ca.self_signed(&ca_key).unwrap().pem();
        let issuer = Issuer::new(ca, ca_key);

        let leaf = |cn: &str, names: &[&str], usage: ExtendedKeyUsagePurpose| {
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::new(names.iter().map(|n| n.to_string()).collect::<Vec<_>>()).unwrap();
            params.distinguished_name.push(DnType::CommonName, cn);
            params.key_usages = vec![KeyUsagePurpose::DigitalSignature, KeyUsagePurpose::KeyEncipherment];
            params.extended_key_usages = vec![usage];
            (params.signed_by(&key, &issuer).unwrap().pem(), key.serialize_pem())
        };
        let server = ExtendedKeyUsagePurpose::ServerAuth;
        let broker = leaf(BROKER, &["localhost", BROKER, "127.0.0.1"], server.clone());
        let proxy = leaf(PROXY, &["localhost", PROXY, "127.0.0.1"], server);
        let client = leaf("apr-it-mtls-client", &[], ExtendedKeyUsagePurpose::ClientAuth);

        let mut token_key = vec![0; 32];
        SystemRandom::new().fill(&mut token_key).unwrap();
        Secrets { ca: ca_pem, broker, proxy, client, token_key }
    }

    /// An HS256 JWT for `role`, as `bin/pulsar tokens create` makes it: the role is the subject, with no expiry.
    fn token(&self, role: &str) -> String {
        let claims = std::collections::BTreeMap::from([("sub", role)]);
        let key = EncodingKey::from_secret(&self.token_key).unwrap();
        jsonwebtoken::encode(&Header::new(Algorithm::HS256), &claims, &key).unwrap()
    }
}
