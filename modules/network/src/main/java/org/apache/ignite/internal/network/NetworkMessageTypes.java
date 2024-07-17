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
}
