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

import java.time.Duration;
import java.util.HexFormat;
import org.apache.pulsar.client.api.v5.Checkpoint;
import org.apache.pulsar.client.api.v5.CheckpointConsumer;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.schema.Schema;

/**
 * Reads a scalable topic with the Java client's Checkpoint consumer, for the Rust client's integration tests, in the
 * broker container, as the admin: {@code CheckpointTool <topic> <start> <count>}, where the start is
 * {@code earliest}, {@code latest}, or a checkpoint's bytes in hex. Prints each message it receives as
 * {@code message <key> <value>}, then the consumer's checkpoint as {@code checkpoint <hex>}.
 */
public final class CheckpointTool {
    public static void main(String[] args) throws Exception {
        String topic = args[0];
        Checkpoint start = switch (args[1]) {
            case "earliest" -> Checkpoint.earliest();
            case "latest" -> Checkpoint.latest();
            default -> Checkpoint.fromByteArray(HexFormat.of().parseHex(args[1]));
        };
        int count = Integer.parseInt(args[2]);
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .authentication("org.apache.pulsar.client.impl.auth.AuthenticationToken", "file:///it/admin.token")
                .build();
             CheckpointConsumer<String> consumer = client.newCheckpointConsumer(Schema.string())
                .topic(topic)
                .startPosition(start)
                .create()) {
            for (int i = 0; i < count; i++) {
                Message<String> message = consumer.receive(Duration.ofSeconds(30));
                if (message == null) {
                    throw new IllegalStateException("received " + i + " of " + count + " messages");
                }
                System.out.println("message " + message.key().orElse("") + " " + message.value());
            }
            System.out.println("checkpoint " + HexFormat.of().formatHex(consumer.checkpoint().toByteArray()));
        }
    }
}
