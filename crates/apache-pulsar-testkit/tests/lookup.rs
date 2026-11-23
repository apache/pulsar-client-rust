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

use apache_pulsar_protocol::frame::{Frame, FrameDecoder};
use apache_pulsar_protocol::lookup::{
    BrokerAddress, HostPort, Lookup, LookupError, LookupStep, MAX_LOOKUP_REDIRECTS, ServiceUrl, parse_broker_url,
};
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::command_lookup_topic_response::LookupType;
use apache_pulsar_protocol::proto::{BaseCommand, CommandLookupTopicResponse, ServerError};
use apache_pulsar_testkit::{LookupAnswer, SimBroker, SimCluster, frame, hp, reply};
use bytes::{Bytes, BytesMut};

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
    // A web service URL over TLS is refused, the reason naming the scheme to use. Java's
    // `PulsarClientBuilderV5Test.testRejectsHttpsWithGuidance`.
    let error = ServiceUrl::parse("https://a:8443").unwrap_err();
    assert!(error.reason.contains("pulsar+ssl://"), "{error}");
    assert!(ServiceUrl::parse("pulsar://a:x").is_err());
    assert!(ServiceUrl::parse("pulsar://").is_err());
}

fn as_frame(command: &BaseCommand) -> Frame {
    FrameDecoder::default().decode(&mut BytesMut::from(&frame(command)[..])).unwrap().unwrap()
}

fn answer(response: CommandLookupTopicResponse) -> Frame {
    as_frame(&BaseCommand {
        r#type: Type::LOOKUP_RESPONSE.into(),
        lookupTopicResponse: response.into(),
        ..Default::default()
    })
}

#[test]
fn a_lookup_tracks_its_topic_and_redirects() {
    let mut lookup = Lookup::new(TOPIC, hp("svc:6650"), false);
    assert_eq!(lookup.topic(), TOPIC);
    assert_eq!(lookup.redirects(), 0);
    let redirect = as_frame(&reply::lookup(0, &LookupAnswer::Redirect { broker: hp("b1:6650"), authoritative: false }));
    assert_eq!(lookup.on_response(&redirect).unwrap(), LookupStep::Ask(BrokerAddress::direct(hp("b1:6650"))));
    assert_eq!(lookup.redirects(), 1);
    assert_eq!(lookup.target(), &BrokerAddress::direct(hp("b1:6650")));
}

#[test]
fn answers_a_lookup_cannot_follow() {
    let lookup = || Lookup::new(TOPIC, hp("svc:6650"), false);
    // Not a lookup response, or not a command at all.
    assert_eq!(lookup().on_response(&as_frame(&reply::success(0))), Err(LookupError::EmptyResponse));
    let garbage = Frame { command: Bytes::from_static(&[0xff, 0xff]), payload: None };
    assert!(matches!(lookup().on_response(&garbage), Err(LookupError::Decode(_))));
    // Failed without saying why, or no answer type at all.
    let failed = CommandLookupTopicResponse { response: Some(LookupType::Failed), ..Default::default() };
    assert_eq!(lookup().on_response(&answer(failed)), Err(LookupError::EmptyResponse));
    assert_eq!(lookup().on_response(&answer(CommandLookupTopicResponse::default())), Err(LookupError::EmptyResponse));

    // A broker to connect to, without the URL the lookup needs, or with one that isn't a broker URL.
    let connect = |plain: Option<&str>, tls: Option<&str>| {
        answer(CommandLookupTopicResponse {
            response: Some(LookupType::Connect),
            brokerServiceUrl: plain.map(String::from),
            brokerServiceUrlTls: tls.map(String::from),
            ..Default::default()
        })
    };
    let only_tls = connect(None, Some("pulsar+ssl://b:6651"));
    assert_eq!(lookup().on_response(&only_tls), Err(LookupError::MissingUrl("plaintext")));
    let mut tls = Lookup::new(TOPIC, hp("svc:6651"), true);
    assert_eq!(tls.on_response(&connect(Some("pulsar://b:6650"), None)), Err(LookupError::MissingUrl("TLS")));
    assert!(matches!(lookup().on_response(&connect(Some("http://b:8080"), None)), Err(LookupError::Url(_))));
}

#[test]
fn malformed_urls() {
    let reason = |url| ServiceUrl::parse(url).unwrap_err().reason;
    assert_eq!(reason("pulsar://a:6650/path"), "a service URL has no path");
    assert_eq!(reason("pulsar://[::1"), "bad host or port");
    assert_eq!(reason("pulsar://[::1]:x"), "bad host or port");
    assert_eq!(reason("pulsar://[::1]6650"), "bad host or port");
    assert_eq!(reason("pulsar://:6650"), "bad host or port");
    assert_eq!(parse_broker_url("pulsar://a:6650,b:6650").unwrap_err().reason, "expected a single host");
    assert!(parse_broker_url("http://a:8080").is_err());
}

#[test]
fn ipv6_hosts() {
    let v6 = HostPort { host: "::1".into(), port: 6650 };
    assert_eq!(ServiceUrl::parse("pulsar://[::1]").unwrap().hosts, vec![v6.clone()]);
    assert_eq!(v6.to_string(), "[::1]:6650");
    assert_eq!(parse_broker_url("pulsar://[::1]:6650").unwrap(), v6);
}
