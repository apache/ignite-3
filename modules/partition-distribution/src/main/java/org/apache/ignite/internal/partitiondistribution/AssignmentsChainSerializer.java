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
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerializer;

/**
 * {@link VersionedSerializer} for {@link AssignmentsChain} instances.
 */
public class AssignmentsChainSerializer extends VersionedSerializer<AssignmentsChain> {

    /** Serializer instance. */
    public static final AssignmentsChainSerializer INSTANCE = new AssignmentsChainSerializer();

    @Override
    protected void writeExternalData(AssignmentsChain chain, IgniteDataOutput out) throws IOException {
        out.writeVarInt(chain.size());

        for (AssignmentsLink assignment : chain) {
            AssignmentsLinkSerializer.INSTANCE.writeExternal(assignment, out);
        }
    }

    @Override
    protected AssignmentsChain readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        int length = in.readVarIntAsInt();
        List<AssignmentsLink> links = new ArrayList<>(length);

        for (int i = 0; i < length; i++) {
            links.add(AssignmentsLinkSerializer.INSTANCE.readExternal(in));
        }

        return AssignmentsChain.of(links);
    }
}
