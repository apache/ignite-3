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

package org.apache.ignite.internal.storage.lease;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Base64;
import java.util.UUID;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.junit.jupiter.api.Test;

class LeaseInfoSerializerTest {
    private static final String V1_SERIALIZED_BASE64 = "Ae++Q9IEAAAAAAAAAQAAAAAAAAACAAAAAAAAAAhwcmltYXJ5";

    private final LeaseInfoSerializer serializer = new LeaseInfoSerializer();

    @Test
    void serializeAndDeserialize() {
        LeaseInfo originalInfo = new LeaseInfo(1234, new UUID(1, 2), "primary");

        byte[] bytes = VersionedSerialization.toBytes(originalInfo, serializer);
        LeaseInfo restoredInfo = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredInfo.leaseStartTime(), is(1234L));
        assertThat(restoredInfo.primaryReplicaNodeId(), is(new UUID(1, 2)));
        assertThat(restoredInfo.primaryReplicaNodeName(), is("primary"));
    }

    @Test
    void v1CanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode(V1_SERIALIZED_BASE64);
        LeaseInfo restoredInfo = VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredInfo.leaseStartTime(), is(1234L));
        assertThat(restoredInfo.primaryReplicaNodeId(), is(new UUID(1, 2)));
        assertThat(restoredInfo.primaryReplicaNodeName(), is("primary"));
    }

    @SuppressWarnings("unused")
    private String v1SerializedBase64() {
        LeaseInfo originalInfo = new LeaseInfo(1234, new UUID(1, 2), "primary");
        byte[] v1Bytes = VersionedSerialization.toBytes(originalInfo, serializer);
        return Base64.getEncoder().encodeToString(v1Bytes);
    }
}
