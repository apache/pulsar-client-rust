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
import java.util.function.Function;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.v5.V5Interop;
import org.apache.pulsar.common.schema.KeyValue;

/**
 * Produces and reads a scalable topic with the Java client's schemas, for the Rust client's integration tests, in the
 * broker container, as the admin:
 * <ul>
 *   <li>{@code SchemaTool produce <schema> <topic> <value>...} sends each value, parsed for the schema;</li>
 *   <li>{@code SchemaTool consume <schema> <topic> <count>} reads that many messages from the start with a Queue
 *       consumer, and prints each value as {@code value <value>}.</li>
 * </ul>
 * The schemas are named as the V5 factories that make them: {@code string}, {@code bool}, {@code int8},
 * {@code int16}, {@code int32}, {@code int64}, {@code float32} and {@code float64}. {@code kv} is a key/value of a
 * string and an int32, written {@code key=value}: the V5 factory has none, so it is v4's, through {@code V5Interop}.
 */
public final class SchemaTool {
    /** A schema, and how its values are read from the command line and printed. */
    private record Codec<T>(Schema<T> schema, Function<String, T> parse, Function<T, String> format) {
        Codec(Schema<T> schema, Function<String, T> parse) {
            this(schema, parse, String::valueOf);
        }
    }

    public static void main(String[] args) throws Exception {
        Codec<?> codec = switch (args[1]) {
            case "string" -> new Codec<>(Schema.string(), value -> value);
            case "bool" -> new Codec<>(Schema.bool(), Boolean::parseBoolean);
            case "int8" -> new Codec<>(Schema.int8(), Byte::parseByte);
            case "int16" -> new Codec<>(Schema.int16(), Short::parseShort);
            case "int32" -> new Codec<>(Schema.int32(), Integer::parseInt);
            case "int64" -> new Codec<>(Schema.int64(), Long::parseLong);
            case "float32" -> new Codec<>(Schema.float32(), Float::parseFloat);
            case "float64" -> new Codec<>(Schema.float64(), Double::parseDouble);
            case "kv" -> new Codec<>(
                    V5Interop.toV5Schema(org.apache.pulsar.client.api.Schema.KeyValue(
                            org.apache.pulsar.client.api.Schema.STRING, org.apache.pulsar.client.api.Schema.INT32)),
                    text -> new KeyValue<>(text.split("=", 2)[0], Integer.parseInt(text.split("=", 2)[1])),
                    pair -> pair.getKey() + "=" + pair.getValue());
            default -> throw new IllegalArgumentException("no schema " + args[1]);
        };
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .authentication("org.apache.pulsar.client.impl.auth.AuthenticationToken", "file:///it/admin.token")
                .build()) {
            switch (args[0]) {
                case "produce" -> produce(client, codec, args[2], java.util.Arrays.copyOfRange(args, 3, args.length));
                case "consume" -> consume(client, codec, args[2], Integer.parseInt(args[3]));
                default -> throw new IllegalArgumentException("no command " + args[0]);
            }
        }
    }

    private static <T> void produce(PulsarClient client, Codec<T> codec, String topic, String[] values)
            throws Exception {
        try (Producer<T> producer = client.newProducer(codec.schema()).topic(topic).create()) {
            for (String value : values) {
                producer.newMessage().value(codec.parse().apply(value)).send();
            }
        }
    }

    private static <T> void consume(PulsarClient client, Codec<T> codec, String topic, int count) throws Exception {
        try (QueueConsumer<T> consumer = client.newQueueConsumer(codec.schema())
                .topic(topic)
                .subscriptionName("schema-tool")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe()) {
            for (int i = 0; i < count; i++) {
                Message<T> message = consumer.receive(Duration.ofSeconds(30));
                if (message == null) {
                    throw new IllegalStateException("received " + i + " of " + count + " messages");
                }
                System.out.println("value " + codec.format().apply(message.value()));
                consumer.acknowledge(message.id());
            }
        }
    }
}
