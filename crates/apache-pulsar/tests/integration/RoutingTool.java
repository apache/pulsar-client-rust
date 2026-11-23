/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.schema.Schema;

/**
 * Publishes a message for each key with the Java client's V5 producer, for the Rust client's integration tests, in the
 * broker container, as the admin: {@code RoutingTool <topic> <key>...}. Prints, for each key,
 * {@code routed <key> <id>}, the message id as the V5 client prints it, which names the segment the producer routed
 * the key to.
 */
public final class RoutingTool {
    public static void main(String[] args) throws Exception {
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .authentication("org.apache.pulsar.client.impl.auth.AuthenticationToken", "file:///it/admin.token")
                .build();
             Producer<String> producer = client.newProducer(Schema.string()).topic(args[0]).create()) {
            for (int i = 1; i < args.length; i++) {
                MessageId id = producer.newMessage().key(args[i]).value("java-" + args[i]).send();
                System.out.println("routed " + args[i] + " " + id);
            }
        }
    }
}
