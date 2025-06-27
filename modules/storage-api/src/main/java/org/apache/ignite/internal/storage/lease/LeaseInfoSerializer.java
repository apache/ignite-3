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

import java.io.IOException;
import java.util.UUID;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerializer;

/**
 * {@link VersionedSerializer} for {@link LeaseInfo} instances.
 */
public class LeaseInfoSerializer extends VersionedSerializer<LeaseInfo> {
    /** Serializer instance. */
    public static final LeaseInfoSerializer INSTANCE = new LeaseInfoSerializer();

    @Override
    protected void writeExternalData(LeaseInfo leaseInfo, IgniteDataOutput out) throws IOException {
        out.writeLong(leaseInfo.leaseStartTime());
        out.writeUuid(leaseInfo.primaryReplicaNodeId());
        out.writeUTF(leaseInfo.primaryReplicaNodeName());
    }

    @Override
    protected LeaseInfo readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        long leaseStartTime = in.readLong();
        UUID primaryReplicaNodeId = in.readUuid();
        String primaryReplicaNodeName = in.readUTF();

        return new LeaseInfo(leaseStartTime, primaryReplicaNodeId, primaryReplicaNodeName);
    }
}
