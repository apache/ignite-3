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

package org.apache.ignite.internal.raft.storage.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Base64;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.junit.jupiter.api.Test;

class StorageDestructionIntentSerializerTest {
    private static final String STORAGE_DESTRUCTION_INTENT_SERIALIZED_WITH_V1 = "Ae++Qw1SQUZUX05PREVfSUQLR1JPVVBfTkFNRQE=";

    private static final StorageDestructionIntent ORIGINAL_INTENT = new StorageDestructionIntent("RAFT_NODE_ID", "GROUP_NAME", true);

    private static final StorageDestructionIntentSerializer serializer = new StorageDestructionIntentSerializer();

    @Test
    void serializationAndDeserialization() {
        byte[] bytes = VersionedSerialization.toBytes(ORIGINAL_INTENT, serializer);
        StorageDestructionIntent restoredIntent = VersionedSerialization.fromBytes(bytes, serializer);

        assertEqualsOriginalIntent(restoredIntent);
    }

    @Test
    void v1CanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode(STORAGE_DESTRUCTION_INTENT_SERIALIZED_WITH_V1);
        StorageDestructionIntent restoredIntent = VersionedSerialization.fromBytes(bytes, serializer);

        assertEqualsOriginalIntent(restoredIntent);
    }

    private static void assertEqualsOriginalIntent(StorageDestructionIntent restoredIntent) {
        assertThat(restoredIntent.nodeId(), is(ORIGINAL_INTENT.nodeId()));
        assertThat(restoredIntent.groupName(), is(ORIGINAL_INTENT.groupName()));
        assertThat(restoredIntent.isVolatile(), is(ORIGINAL_INTENT.isVolatile()));
    }

    @SuppressWarnings("unused")
    private static String v1SerializedBase64() {
        byte[] v1Bytes = VersionedSerialization.toBytes(ORIGINAL_INTENT, serializer);

        return Base64.getEncoder().encodeToString(v1Bytes);
    }
}
