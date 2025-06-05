/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.table;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.marshalling.ByteArrayMarshaller;
import org.apache.ignite.marshalling.Marshaller;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Data streamer receiver descriptor test.
 */
public class DataStreamerReceiverDescriptorTest {
    @Test
    void testReceiverDescriptorBuilderPopulatesMarshallers() {
        DataStreamerReceiverDescriptor<Integer, String, UUID> desc = DataStreamerReceiverDescriptor.builder(new TestReceiver()).build();

        assertInstanceOf(IntMarshaller.class, desc.payloadMarshaller());
        assertInstanceOf(StrMarshaller.class, desc.argumentMarshaller());
        assertInstanceOf(UuidMarshaller.class, desc.resultMarshaller());
    }

    private static class IntMarshaller implements ByteArrayMarshaller<Integer> { }

    private static class StrMarshaller implements ByteArrayMarshaller<String> { }

    private static class UuidMarshaller implements ByteArrayMarshaller<UUID> { }

    private static class TestReceiver implements DataStreamerReceiver<Integer, String, UUID> {
        @Override
        public @Nullable CompletableFuture<List<UUID>> receive(List<Integer> page, DataStreamerReceiverContext ctx, @Nullable String arg) {
            return null;
        }

        @Override
        public @Nullable Marshaller<Integer, byte[]> payloadMarshaller() {
            return new IntMarshaller();
        }

        @Override
        public @Nullable Marshaller<String, byte[]> argumentMarshaller() {
            return new StrMarshaller();
        }

        @Override
        public @Nullable Marshaller<UUID, byte[]> resultMarshaller() {
            return new UuidMarshaller();
        }
    }
}
