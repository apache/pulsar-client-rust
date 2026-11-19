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
use std::time::Duration;

use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose,
};
use ring::rand::{SecureRandom, SystemRandom};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

const IMAGE: (&str, &str) = ("apachepulsar/pulsar", "5.0.1");
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

        Env { token: secrets.token(TOKEN_ROLE), dir, broker, proxy }
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
