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

package org.apache.ignite.internal.raft;

import static org.apache.ignite.internal.raft.RaftGroupConfiguration.UNKNOWN_INDEX;
import static org.apache.ignite.internal.raft.RaftGroupConfiguration.UNKNOWN_TERM;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerializer;
import org.jetbrains.annotations.Nullable;

/**
 * {@link VersionedSerializer} for {@link RaftGroupConfiguration} instances.
 */
public class RaftGroupConfigurationSerializer extends VersionedSerializer<RaftGroupConfiguration> {
    /** Serializer instance. */
    public static final RaftGroupConfigurationSerializer INSTANCE = new RaftGroupConfigurationSerializer();

    @Override
    protected byte getProtocolVersion() {
        return 3;
    }

    @Override
    protected void writeExternalData(RaftGroupConfiguration config, IgniteDataOutput out) throws IOException {
        out.writeLong(config.index());
        out.writeLong(config.term());
        writeStringList(config.peers(), out);
        writeStringList(config.learners(), out);
        writeNullableStringList(config.oldPeers(), out);
        writeNullableStringList(config.oldLearners(), out);
        out.writeVarInt(config.sequenceToken());
        out.writeVarInt(config.oldSequenceToken());
    }

    private static void writeStringList(List<String> strings, IgniteDataOutput out) throws IOException {
        out.writeVarInt(strings.size());
        for (String str : strings) {
            out.writeUTF(str);
        }
    }

    private static void writeNullableStringList(@Nullable List<String> strings, IgniteDataOutput out) throws IOException {
        if (strings == null) {
            out.writeVarInt(-1);
        } else {
            writeStringList(strings, out);
        }
    }

    @Override
    protected RaftGroupConfiguration readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        long index;
        long term;

        if (protoVer >= 2) {
            index = in.readLong();
            term = in.readLong();
        } else {
            index = UNKNOWN_INDEX;
            term = UNKNOWN_TERM;
        }

        List<String> peers = readStringList(in);
        List<String> learners = readStringList(in);
        List<String> oldPeers = readNullableStringList(in);
        List<String> oldLearners = readNullableStringList(in);

        long sequenceToken;
        long oldSequenceToken;
        if (protoVer >= 3) {
            sequenceToken = in.readVarInt();
            oldSequenceToken = in.readVarInt();
        } else {
            sequenceToken = 0;
            oldSequenceToken = 0;
        }

        return new RaftGroupConfiguration(index, term, sequenceToken, oldSequenceToken, peers, learners, oldPeers, oldLearners);
    }

    private static List<String> readStringList(IgniteDataInput in) throws IOException {
        int length = in.readVarIntAsInt();
        return readStringList(length, in);
    }

    private static List<String> readStringList(int length, IgniteDataInput in) throws IOException {
        assert length >= 0 : "Invalid length: " + length;

        var list = new ArrayList<String>();
        for (int i = 0; i < length; i++) {
            list.add(in.readUTF());
        }
        return list;
    }

    private static @Nullable List<String> readNullableStringList(IgniteDataInput in) throws IOException {
        int lengthOrMinusOne = in.readVarIntAsInt();
        if (lengthOrMinusOne == -1) {
            return null;
        }

        return readStringList(lengthOrMinusOne, in);
    }
}
