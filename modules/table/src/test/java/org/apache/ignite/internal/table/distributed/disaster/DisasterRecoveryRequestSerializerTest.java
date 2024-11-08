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

package org.apache.ignite.internal.table.distributed.disaster;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Base64;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.junit.jupiter.api.Test;

class DisasterRecoveryRequestSerializerTest {
    private static final String GROUP_UPDATE_REQUEST_V1_BASE64 = "Ae++QwEB775D782rkHhWNBIhQ2WHCbrc/ukH0Q+5FwQMIBYB";
    private static final String MANUAL_GROUP_RESTART_REQUEST_V1_BASE64 = "Ae++QwIB775D782rkHhWNBIhQ2WHCbrc/tEPuRcEIBYMAwJiAmH///9///+AgAQ=";

    private final DisasterRecoveryRequestSerializer serializer = new DisasterRecoveryRequestSerializer();

    @Test
    void serializationAndDeserializationOfGroupUpdateRequest() {
        var originalRequest = new GroupUpdateRequest(
                new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L),
                1000,
                2000,
                3000,
                Set.of(11, 21, 31),
                true
        );

        byte[] bytes = VersionedSerialization.toBytes(originalRequest, serializer);
        GroupUpdateRequest restoredRequest = (GroupUpdateRequest) VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredRequest.operationId(), is(new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L)));
        assertThat(restoredRequest.catalogVersion(), is(1000));
        assertThat(restoredRequest.zoneId(), is(2000));
        assertThat(restoredRequest.tableId(), is(3000));
        assertThat(restoredRequest.partitionIds(), is(Set.of(11, 21, 31)));
        assertThat(restoredRequest.manualUpdate(), is(true));
    }

    @Test
    void v1OfGroupUpdateRequestCanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode(GROUP_UPDATE_REQUEST_V1_BASE64);
        GroupUpdateRequest restoredRequest = (GroupUpdateRequest) VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredRequest.operationId(), is(new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L)));
        assertThat(restoredRequest.catalogVersion(), is(1000));
        assertThat(restoredRequest.zoneId(), is(2000));
        assertThat(restoredRequest.tableId(), is(3000));
        assertThat(restoredRequest.partitionIds(), is(Set.of(11, 21, 31)));
        assertThat(restoredRequest.manualUpdate(), is(true));
    }

    @Test
    void serializationAndDeserializationOfManualGroupRestartRequest() {
        var originalRequest = new ManualGroupRestartRequest(
                new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L),
                2000,
                3000,
                Set.of(11, 21, 31),
                Set.of("a", "b"),
                HybridTimestamp.MAX_VALUE.longValue()
        );

        byte[] bytes = VersionedSerialization.toBytes(originalRequest, serializer);
        ManualGroupRestartRequest restoredRequest = (ManualGroupRestartRequest) VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredRequest.operationId(), is(new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L)));
        assertThat(restoredRequest.zoneId(), is(2000));
        assertThat(restoredRequest.tableId(), is(3000));
        assertThat(restoredRequest.partitionIds(), is(Set.of(11, 21, 31)));
        assertThat(restoredRequest.nodeNames(), is(Set.of("a", "b")));
        assertThat(restoredRequest.assignmentsTimestamp(), is(HybridTimestamp.MAX_VALUE.longValue()));
    }

    @Test
    void v1OfManualGroupRestartRequestCanBeDeserialized() {
        byte[] bytes = Base64.getDecoder().decode(MANUAL_GROUP_RESTART_REQUEST_V1_BASE64);
        ManualGroupRestartRequest restoredRequest = (ManualGroupRestartRequest) VersionedSerialization.fromBytes(bytes, serializer);

        assertThat(restoredRequest.operationId(), is(new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L)));
        assertThat(restoredRequest.zoneId(), is(2000));
        assertThat(restoredRequest.tableId(), is(3000));
        assertThat(restoredRequest.partitionIds(), is(Set.of(11, 21, 31)));
        assertThat(restoredRequest.nodeNames(), is(Set.of("a", "b")));
        assertThat(restoredRequest.assignmentsTimestamp(), is(HybridTimestamp.MAX_VALUE.longValue()));
    }

    @SuppressWarnings("unused")
    private String manualGroupRestartRequestV1Base64() {
        var originalRequest = new ManualGroupRestartRequest(
                new UUID(0x1234567890ABCDEFL, 0xFEDCBA0987654321L),
                2000,
                3000,
                Set.of(11, 21, 31),
                Set.of("a", "b"),
                HybridTimestamp.MAX_VALUE.longValue()
        );

        byte[] v1Bytes = VersionedSerialization.toBytes(originalRequest, serializer);
        return Base64.getEncoder().encodeToString(v1Bytes);
    }
}
