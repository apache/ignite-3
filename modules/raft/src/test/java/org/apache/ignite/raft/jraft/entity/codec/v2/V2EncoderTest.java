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

package org.apache.ignite.raft.jraft.entity.codec.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.raft.jraft.entity.EnumOutter.EntryType;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

/**
 * Tests for V2 encoder/decoder with sequence token support.
 */
class V2EncoderTest {
    @SuppressWarnings("unused")
    private static final List<LogEntry> ENTRIES = List.of(
            createDataEntry(),
            createCfgEntry(false),
            createCfgEntry(true),
            createDataEntryWithSequenceTokens(),
            createCfgEntryWithSequenceTokens()
    );

    @ParameterizedTest
    @FieldSource("ENTRIES")
    void testEncodeDecode(LogEntry logEntry) {
        byte[] bytes = V2Encoder.INSTANCE.encode(logEntry);

        LogEntry decodedEntry = V2Decoder.INSTANCE.decode(bytes);

        assertEquals(logEntry, decodedEntry);
    }

    @ParameterizedTest
    @FieldSource("ENTRIES")
    void testEncodeDecodeByteBuffer(LogEntry logEntry) {
        ByteBuffer buf = ByteBuffer.allocate(V2Encoder.INSTANCE.size(logEntry)).order(ByteOrder.LITTLE_ENDIAN);

        V2Encoder.INSTANCE.encode(buf, logEntry);

        LogEntry decodedEntry = V2Decoder.INSTANCE.decode(buf.array());

        assertEquals(logEntry, decodedEntry);
    }

    @ParameterizedTest
    @FieldSource("ENTRIES")
    void testSize(LogEntry logEntry) {
        byte[] bytes = V2Encoder.INSTANCE.encode(logEntry);

        int size = V2Encoder.INSTANCE.size(logEntry);

        assertEquals(size, bytes.length);
    }

    @ParameterizedTest
    @FieldSource("ENTRIES")
    void testDirectWrite(LogEntry logEntry) {
        byte[] bytes = V2Encoder.INSTANCE.encode(logEntry);

        int size = V2Encoder.INSTANCE.size(logEntry);
        ByteBuffer direct = ByteBuffer.allocateDirect(size);

        V2Encoder.INSTANCE.append(GridUnsafe.bufferAddress(direct), logEntry);

        assertEquals(ByteBuffer.wrap(bytes), direct);
    }

    private static LogEntry createDataEntry() {
        LogEntry logEntry = new LogEntry(EntryType.ENTRY_TYPE_DATA);

        LogId id = new LogId();
        id.setIndex(1000);
        id.setTerm(10_000);

        logEntry.setId(id);
        logEntry.setChecksum(100_000);
        logEntry.setSequenceToken(0);
        logEntry.setOldSequenceToken(0);

        byte[] bytes = new byte[4096];
        ThreadLocalRandom.current().nextBytes(bytes);
        logEntry.setData(ByteBuffer.wrap(bytes));

        return logEntry;
    }

    private static LogEntry createCfgEntry(boolean emptyLists) {
        LogEntry logEntry = new LogEntry(EntryType.ENTRY_TYPE_CONFIGURATION);

        LogId id = new LogId();
        id.setIndex(1000);
        id.setTerm(10_000);

        logEntry.setId(id);
        logEntry.setChecksum(100_000);
        logEntry.setSequenceToken(0);
        logEntry.setOldSequenceToken(0);

        if (!emptyLists) {
            logEntry.setOldLearners(List.of(new PeerId("oldLearner")));
            logEntry.setOldPeers(List.of(new PeerId("oldPeer")));

            logEntry.setLearners(List.of(new PeerId("learner")));
            logEntry.setPeers(List.of(new PeerId("peer")));
        }

        return logEntry;
    }

    private static LogEntry createDataEntryWithSequenceTokens() {
        LogEntry logEntry = new LogEntry(EntryType.ENTRY_TYPE_DATA);

        LogId id = new LogId();
        id.setIndex(2000);
        id.setTerm(20_000);

        logEntry.setId(id);
        logEntry.setChecksum(200_000);
        logEntry.setSequenceToken(12345);
        logEntry.setOldSequenceToken(12340);

        byte[] bytes = new byte[2048];
        ThreadLocalRandom.current().nextBytes(bytes);
        logEntry.setData(ByteBuffer.wrap(bytes));

        return logEntry;
    }

    private static LogEntry createCfgEntryWithSequenceTokens() {
        LogEntry logEntry = new LogEntry(EntryType.ENTRY_TYPE_CONFIGURATION);

        LogId id = new LogId();
        id.setIndex(3000);
        id.setTerm(30_000);

        logEntry.setId(id);
        logEntry.setChecksum(300_000);
        logEntry.setSequenceToken(99999);
        logEntry.setOldSequenceToken(88888);

        logEntry.setOldLearners(List.of(new PeerId("oldLearner1"), new PeerId("oldLearner2")));
        logEntry.setOldPeers(List.of(new PeerId("oldPeer1"), new PeerId("oldPeer2")));

        logEntry.setLearners(List.of(new PeerId("learner1"), new PeerId("learner2")));
        logEntry.setPeers(List.of(new PeerId("peer1"), new PeerId("peer2")));

        return logEntry;
    }
}
