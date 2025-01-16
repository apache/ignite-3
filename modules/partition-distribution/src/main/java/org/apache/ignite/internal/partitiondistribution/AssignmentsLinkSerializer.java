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
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerializer;

/**
 * {@link VersionedSerializer} for {@link AssignmentsLink} instances.
 */
public class AssignmentsLinkSerializer extends VersionedSerializer<AssignmentsLink> {

    /** Serializer instance. */
    public static final AssignmentsLinkSerializer INSTANCE = new AssignmentsLinkSerializer();

    @Override
    protected void writeExternalData(AssignmentsLink link, IgniteDataOutput out) throws IOException {
        AssignmentsSerializer.INSTANCE.writeExternal(link.assignments(), out);

        out.writeVarInt(link.configurationTerm());
        out.writeVarInt(link.configurationIndex());
    }

    @Override
    protected AssignmentsLink readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        Assignments assignments = AssignmentsSerializer.INSTANCE.readExternal(in);

        long term = in.readVarInt();
        long index = in.readVarInt();

        return new AssignmentsLink(assignments, term, index);
    }
}
