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

package org.apache.ignite.internal.network;

import org.apache.ignite.internal.network.annotations.MessageGroup;
import org.apache.ignite.internal.network.message.ClassDescriptorListMessage;
import org.apache.ignite.internal.network.message.ClassDescriptorMessage;
import org.apache.ignite.internal.network.message.ClusterNodeMessage;
import org.apache.ignite.internal.network.message.FieldDescriptorMessage;
import org.apache.ignite.internal.network.message.InvokeRequest;
import org.apache.ignite.internal.network.message.InvokeResponse;
import org.apache.ignite.internal.network.message.ScaleCubeMessage;
import org.apache.ignite.internal.network.message.value.BooleanValueMessage;
import org.apache.ignite.internal.network.message.value.ByteArrayValueMessage;
import org.apache.ignite.internal.network.message.value.ByteValueMessage;
import org.apache.ignite.internal.network.message.value.DoubleValueMessage;
import org.apache.ignite.internal.network.message.value.FloatValueMessage;
import org.apache.ignite.internal.network.message.value.IntValueMessage;
import org.apache.ignite.internal.network.message.value.LongValueMessage;
import org.apache.ignite.internal.network.message.value.NullValueMessage;
import org.apache.ignite.internal.network.message.value.ShortValueMessage;
import org.apache.ignite.internal.network.message.value.StringValueMessage;
import org.apache.ignite.internal.network.message.value.UuidValueMessage;
import org.apache.ignite.internal.network.recovery.message.AcknowledgementMessage;
import org.apache.ignite.internal.network.recovery.message.HandshakeFinishMessage;
import org.apache.ignite.internal.network.recovery.message.HandshakeRejectedMessage;
import org.apache.ignite.internal.network.recovery.message.HandshakeStartMessage;
import org.apache.ignite.internal.network.recovery.message.HandshakeStartResponseMessage;
import org.apache.ignite.internal.network.recovery.message.ProbeMessage;

/**
 * Message types for the network module.
 */
@MessageGroup(groupName = "NetworkMessages", groupType = 1)
public class NetworkMessageTypes {
    /**
     * Type for {@link InvokeRequest}.
     */
    public static final short INVOKE_REQUEST = 0;

    /**
     * Type for {@link InvokeResponse}.
     */
    public static final short INVOKE_RESPONSE = 1;

    /**
     * Type for {@link ScaleCubeMessage}.
     */
    public static final short SCALE_CUBE_MESSAGE = 2;

    /**
     * Type for {@link HandshakeStartMessage}.
     */
    public static final short HANDSHAKE_START = 3;

    /**
     * Type for {@link HandshakeStartResponseMessage}.
     */
    public static final short HANDSHAKE_START_RESPONSE = 4;

    /**
     * Type for {@link HandshakeFinishMessage}.
     */
    public static final short HANDSHAKE_FINISH = 5;

    /**
     * Type for {@link HandshakeRejectedMessage}.
     */
    public static final short HANDSHAKE_REJECTED = 6;

    /**
     * Type for {@link AcknowledgementMessage}.
     */
    public static final short ACKNOWLEDGEMENT = 7;

    /**
     * Type for {@link ClassDescriptorMessage}.
     */
    public static final short CLASS_DESCRIPTOR_MESSAGE = 8;

    /**
     * Type for {@link FieldDescriptorMessage}.
     */
    public static final short FIELD_DESCRIPTOR_MESSAGE = 9;

    /**
     * Type for {@link ClassDescriptorListMessage}.
     */
    public static final short CLASS_DESCRIPTOR_LIST_MESSAGE = 10;

    /**
     * Type for {@link ClusterNodeMessage}.
     */
    public static final short CLUSTER_NODE_MESSAGE = 11;

    /**
     * Type for {@link ProbeMessage}.
     */
    public static final short PROBE_MESSAGE = 12;

    /**
     * Message types that contain a single value of a certain type.
     */
    public interface SingleValueMessages {
        /**
         * Type for {@link BooleanValueMessage}.
         */
        short BOOLEAN_VALUE_MESSAGE = 101;

        /**
         * Type for {@link ByteValueMessage}.
         */
        short BYTE_VALUE_MESSAGE = 102;

        /**
         * Type for {@link ShortValueMessage}.
         */
        short SHORT_VALUE_MESSAGE = 103;

        /**
         * Type for {@link IntValueMessage}.
         */
        short INT_VALUE_MESSAGE = 104;

        /**
         * Type for {@link LongValueMessage}.
         */
        short LONG_VALUE_MESSAGE = 105;

        /**
         * Type for {@link FloatValueMessage}.
         */
        short FLOAT_VALUE_MESSAGE = 106;

        /**
         * Type for {@link DoubleValueMessage}.
         */
        short DOUBLE_VALUE_MESSAGE = 107;

        /**
         * Type for {@link UuidValueMessage}.
         */
        short UUID_VALUE_MESSAGE = 108;

        /**
         * Type for {@link StringValueMessage}.
         */
        short STRING_VALUE_MESSAGE = 109;

        /**
         * Type for {@link ByteArrayValueMessage}.
         */
        short BYTE_ARRAY_VALUE_MESSAGE = 110;

        /**
         * Type for {@link NullValueMessage}.
         */
        short NULL_VALUE_MESSAGE = 111;
    }
}
