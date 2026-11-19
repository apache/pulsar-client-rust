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

//! Lookups across redirects and proxies, against simulated brokers.

use apache_pulsar_protocol::lookup::{BrokerAddress, Lookup, LookupError, MAX_LOOKUP_REDIRECTS, ServiceUrl};
use apache_pulsar_protocol::proto::ServerError;
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_testkit::{LookupAnswer, SimBroker, SimCluster, hp};

const TOPIC: &str = "segment://t/n/x/0000-ffff-1";

fn broker_answering(answer: LookupAnswer) -> SimBroker {
    let mut broker = SimBroker::new();
    broker.default_lookup = answer;
    broker
}

#[test]
fn direct_answer() {
    let mut cluster = SimCluster::default();
    cluster.add(hp("svc:6650"), broker_answering(LookupAnswer::Connect { broker: hp("b1:6650"), proxy: false }));
    let found = cluster.run_lookup(Lookup::new(TOPIC, hp("svc:6650"), false)).unwrap();
    assert_eq!(found, BrokerAddress::direct(hp("b1:6650")));
    assert!(!found.through_proxy());
}

#[test]
fn redirects_carry_the_authoritative_flag() {
    let mut cluster = SimCluster::default();
    cluster
        .add(hp("svc:6650"), broker_answering(LookupAnswer::Redirect { broker: hp("b1:6650"), authoritative: true }));
    cluster.add(hp("b1:6650"), broker_answering(LookupAnswer::Connect { broker: hp("b2:6650"), proxy: false }));
    let found = cluster.run_lookup(Lookup::new(TOPIC, hp("svc:6650"), false)).unwrap();
    assert_eq!(found, BrokerAddress::direct(hp("b2:6650")));
    let second = cluster.brokers[&hp("b1:6650")].received_of(Type::LOOKUP)[0].lookupTopic.as_option().unwrap().clone();
    assert_eq!(second.authoritative, Some(true));
    let first = cluster.brokers[&hp("svc:6650")].received_of(Type::LOOKUP)[0].lookupTopic.as_option().unwrap().clone();
    assert_eq!(first.authoritative, Some(false));
}

#[test]
fn proxy_answer_keeps_the_physical_address() {
    let mut cluster = SimCluster::default();
    cluster.add(hp("proxy:6650"), broker_answering(LookupAnswer::Connect { broker: hp("b1:6650"), proxy: true }));
    let found = cluster.run_lookup(Lookup::new(TOPIC, hp("proxy:6650"), false)).unwrap();
    assert_eq!(found, BrokerAddress { logical: hp("b1:6650"), physical: hp("proxy:6650") });
    assert!(found.through_proxy());
}

#[test]
fn tls_uses_the_tls_url() {
    let mut cluster = SimCluster::default();
    cluster.add(hp("svc:6651"), broker_answering(LookupAnswer::Connect { broker: hp("b1:6651"), proxy: false }));
    let found = cluster.run_lookup(Lookup::new(TOPIC, hp("svc:6651"), true)).unwrap();
    assert_eq!(found, BrokerAddress::direct(hp("b1:6651")));
}

#[test]
fn redirect_loop_stops_after_the_limit() {
    let mut cluster = SimCluster::default();
    cluster.add(hp("a:6650"), broker_answering(LookupAnswer::Redirect { broker: hp("b:6650"), authoritative: false }));
    cluster.add(hp("b:6650"), broker_answering(LookupAnswer::Redirect { broker: hp("a:6650"), authoritative: false }));
    let error = cluster.run_lookup(Lookup::new(TOPIC, hp("a:6650"), false)).unwrap_err();
    assert_eq!(error, LookupError::TooManyRedirects);
    let asked: usize = cluster.brokers.values().map(|b| b.received_of(Type::LOOKUP).len()).sum();
    assert_eq!(asked as u32, MAX_LOOKUP_REDIRECTS + 1);
}

#[test]
fn failure_carries_the_server_error() {
    let mut cluster = SimCluster::default();
    cluster.add(hp("svc:6650"), SimBroker::new());
    let error = cluster.run_lookup(Lookup::new(TOPIC, hp("svc:6650"), false)).unwrap_err();
    assert!(matches!(error, LookupError::Server { error: buffa::EnumValue::Known(ServerError::TopicNotFound), .. }));
}

#[test]
fn service_urls() {
    let url = ServiceUrl::parse("pulsar+ssl://a,b:7000,[::1]:6000/").unwrap();
    assert!(url.tls);
    assert_eq!(
        url.hosts,
        vec![hp("a:6651"), hp("b:7000"), apache_pulsar_protocol::lookup::HostPort { host: "::1".into(), port: 6000 }]
    );
    assert!(ServiceUrl::parse("http://a:8080").is_err());
    assert!(ServiceUrl::parse("pulsar://a:x").is_err());
    assert!(ServiceUrl::parse("pulsar://").is_err());
}
