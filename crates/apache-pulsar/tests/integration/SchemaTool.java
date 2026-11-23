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
import org.apache.pulsar.client.api.v5.schema.GenericRecord;
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
 * {@code json} and {@code avro} are records of a name and an age, written {@code name:age}; {@code avro-v1} and
 * {@code avro-v2} the two versions of Java's V5 evolution test, written {@code i} and {@code i:j}.
 * {@code protobuf} is a {@code google.protobuf.Timestamp} of V5's {@code Schema.protobuf}, written
 * {@code seconds:nanos}, and {@code protobuf-native} one of v4's {@code Schema.PROTOBUF_NATIVE}: the V5 factory has
 * none, so it goes through {@code V5Interop}. {@code auto} only consumes, with {@code Schema.autoConsume()}: it prints
 * each generic record's schema type and value, a key/value as {@code key=value}, a Protobuf message on one line and
 * bytes as text.
 * {@code SchemaTool info <kind> <message>} prints the schema Java declares for a Protobuf message, {@code protobuf} or
 * {@code protobuf-native}.
 */
public final class SchemaTool {
    /** A schema, and how its values are read from the command line and printed. */
    private record Codec<T>(Schema<T> schema, Function<String, T> parse, Function<T, String> format) {
        Codec(Schema<T> schema, Function<String, T> parse) {
            this(schema, parse, String::valueOf);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args[0].equals("info")) {
            info(args[1], message(args[2]));
            return;
        }
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
            case "json" -> new Codec<>(Schema.json(Pojo.class), Pojo::parse);
            case "avro" -> new Codec<>(Schema.avro(Pojo.class), Pojo::parse);
            case "avro-v1" -> new Codec<>(Schema.avro(V1.class), V1::parse);
            case "avro-v2" -> new Codec<>(Schema.avro(V2.class), V2::parse);
            case "protobuf" -> timestamps(Schema.protobuf(com.google.protobuf.Timestamp.class));
            case "protobuf-native" -> timestamps(V5Interop.toV5Schema(
                    org.apache.pulsar.client.api.Schema.PROTOBUF_NATIVE(com.google.protobuf.Timestamp.class)));
            case "auto" -> new Codec<GenericRecord>(
                    Schema.autoConsume(),
                    text -> {
                        throw new IllegalArgumentException("auto only consumes");
                    },
                    record -> record.schemaType() + " " + text(record.nativeObject()));
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

    /**
     * A record of the JSON and Avro schemas, written {@code name:age}, {@code null} for no name. A static nested class,
     * as Java's V5 schema tests have, which Avro names in a namespace that ends in {@code $}.
     */
    public static final class Pojo {
        private String name;
        private int age;

        public Pojo() {
        }

        Pojo(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        static Pojo parse(String text) {
            String[] parts = text.split(":", 2);
            return new Pojo("null".equals(parts[0]) ? null : parts[0], Integer.parseInt(parts[1]));
        }

        @Override
        public String toString() {
            return name + ":" + age;
        }
    }

    /** The first version of Java's V5 evolution test's record: an int. */
    public static final class V1 {
        public int i;

        static V1 parse(String text) {
            V1 record = new V1();
            record.i = Integer.parseInt(text);
            return record;
        }

        @Override
        public String toString() {
            return String.valueOf(i);
        }
    }

    /** The second version, which adds a nullable int, {@code null} in the first version's records. */
    public static final class V2 {
        public int i;
        public Integer j;

        static V2 parse(String text) {
            String[] parts = text.split(":", 2);
            V2 record = new V2();
            record.i = Integer.parseInt(parts[0]);
            record.j = "null".equals(parts[1]) ? null : Integer.valueOf(parts[1]);
            return record;
        }

        @Override
        public String toString() {
            return i + ":" + j;
        }
    }

    /** {@code google.protobuf.Timestamp}s of a schema, written {@code seconds:nanos}. */
    private static Codec<com.google.protobuf.Timestamp> timestamps(Schema<com.google.protobuf.Timestamp> schema) {
        return new Codec<>(schema, SchemaTool::timestamp, time -> time.getSeconds() + ":" + time.getNanos());
    }

    /** A {@code google.protobuf.Timestamp}, written {@code seconds:nanos}. */
    private static com.google.protobuf.Timestamp timestamp(String text) {
        String[] parts = text.split(":", 2);
        return com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(Long.parseLong(parts[0]))
                .setNanos(Integer.parseInt(parts[1]))
                .build();
    }

    /** The Protobuf messages the tests use: well-known types, which the broker's classpath has. */
    private static Class<? extends com.google.protobuf.Message> message(String name) {
        return switch (name) {
            case "timestamp" -> com.google.protobuf.Timestamp.class;
            case "duration" -> com.google.protobuf.Duration.class;
            case "struct" -> com.google.protobuf.Struct.class;
            case "type" -> com.google.protobuf.Type.class;
            case "descriptor" -> com.google.protobuf.DescriptorProtos.FileDescriptorSet.class;
            default -> throw new IllegalArgumentException("no message " + name);
        };
    }

    /**
     * Prints the schema Java declares for a message: {@code protobuf} is V5's {@code Schema.protobuf},
     * {@code protobuf-native} v4's {@code PROTOBUF_NATIVE}.
     */
    private static void info(String kind, Class<? extends com.google.protobuf.Message> message) {
        switch (kind) {
            case "protobuf" -> {
                org.apache.pulsar.client.api.v5.schema.SchemaInfo info = Schema.protobuf(message).schemaInfo();
                print(info.name(), info.type(), info.schema(), info.properties());
            }
            case "protobuf-native" -> {
                org.apache.pulsar.common.schema.SchemaInfo info =
                        org.apache.pulsar.client.api.Schema.PROTOBUF_NATIVE(message).getSchemaInfo();
                print(info.getName(), info.getType(), info.getSchema(), info.getProperties());
            }
            default -> throw new IllegalArgumentException("no kind " + kind);
        }
    }

    private static void print(String name, Object type, byte[] definition, java.util.Map<String, String> properties) {
        System.out.println("name " + name);
        System.out.println("type " + type);
        System.out.println("definition " + new String(definition, java.nio.charset.StandardCharsets.UTF_8));
        new java.util.TreeMap<>(properties)
                .forEach((key, value) -> System.out.println("property " + key + "=" + value));
    }

    /** A generic record's value as text: a key/value as {@code key=value}, a Protobuf message on one line, bytes as
     * UTF-8. */
    private static String text(Object value) {
        if (value instanceof com.google.protobuf.MessageOrBuilder message) {
            return com.google.protobuf.TextFormat.shortDebugString(message);
        }
        if (value instanceof org.apache.pulsar.client.api.v5.schema.KeyValue<?, ?> pair) {
            return text(pair.key()) + "=" + text(pair.value());
        }
        if (value instanceof byte[] bytes) {
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }
        return String.valueOf(value);
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
