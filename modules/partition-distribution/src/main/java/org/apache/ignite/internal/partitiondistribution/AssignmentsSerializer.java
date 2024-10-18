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

package org.apache.ignite.internal.partitiondistribution;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerializer;

/**
 * {@link VersionedSerializer} for {@link Assignments} instances.
 */
public class AssignmentsSerializer extends VersionedSerializer<Assignments> {
    /** Serializer instance. */
    public static AssignmentsSerializer INSTANCE = new AssignmentsSerializer();

    @Override
    protected void writeExternalData(Assignments assignments, IgniteDataOutput out) throws IOException {
        out.writeVarInt(assignments.nodes().size());
        for (Assignment assignment : assignments.nodes()) {
            writeAssignment(assignment, out);
        }

        out.writeBoolean(assignments.force());
        // Writing long and not varlong as the latter will take 9 bytes for timestamps.
        out.writeLong(assignments.timestamp());
    }

    private static void writeAssignment(Assignment assignment, IgniteDataOutput out) throws IOException {
        out.writeUTF(assignment.consistentId());
        out.writeBoolean(assignment.isPeer());
    }

    @Override
    protected Assignments readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        Set<Assignment> nodes = readNodes(in);
        boolean force = in.readBoolean();
        long timestamp = in.readLong();

        return force ? Assignments.forced(nodes, timestamp) : Assignments.of(nodes, timestamp);
    }

    private static Set<Assignment> readNodes(IgniteDataInput in) throws IOException {
        int length = in.readVarIntAsInt();

        Set<Assignment> nodes = new HashSet<>();
        for (int i = 0; i < length; i++) {
            nodes.add(readAssignment(in));
        }

        return nodes;
    }

    private static Assignment readAssignment(IgniteDataInput in) throws IOException {
        String consistentId = in.readUTF();
        boolean isPeer = in.readBoolean();

        return isPeer ? Assignment.forPeer(consistentId) : Assignment.forLearner(consistentId);
    }
}
