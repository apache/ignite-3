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

package org.apache.ignite.raft.jraft.entity.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.raft.jraft.entity.EnumOutter.EntryType;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.entity.codec.v1.V1Decoder;
import org.apache.ignite.raft.jraft.entity.codec.v1.V1Encoder;
import org.apache.ignite.raft.jraft.entity.codec.v2.V2Decoder;
import org.apache.ignite.raft.jraft.entity.codec.v2.V2Encoder;
import org.junit.jupiter.api.Test;

/**
 * Tests backward compatibility between V1 and V2 codecs.
 */
class BackwardCompatibilityTest {

    /**
     * Test that V1-encoded entries can be decoded by AutoDetectDecoder.
     */
    @Test
    void testV1EntriesCanBeReadByAutoDetect() {
        LogEntry originalEntry = createDataEntry();

        // Encode with V1
        byte[] v1Bytes = V1Encoder.INSTANCE.encode(originalEntry);

        // Decode with AutoDetectDecoder (should use V1 decoder)
        LogEntry decodedEntry = AutoDetectDecoder.INSTANCE.decode(v1Bytes);

        assertNotNull(decodedEntry);
        assertEquals(originalEntry.getType(), decodedEntry.getType());
        assertEquals(originalEntry.getId(), decodedEntry.getId());
        assertEquals(originalEntry.getChecksum(), decodedEntry.getChecksum());

        // V1 entries should have zero sequence tokens when decoded
        assertEquals(0, decodedEntry.getSequenceToken());
        assertEquals(0, decodedEntry.getOldSequenceToken());
    }

    /**
     * Test that V2-encoded entries can be decoded by AutoDetectDecoder.
     */
    @Test
    void testV2EntriesCanBeReadByAutoDetect() {
        LogEntry originalEntry = createDataEntry();
        originalEntry.setSequenceToken(12345);
        originalEntry.setOldSequenceToken(12340);

        // Encode with V2
        byte[] v2Bytes = V2Encoder.INSTANCE.encode(originalEntry);

        // Decode with AutoDetectDecoder (should use V2 decoder internally)
        LogEntry decodedEntry = AutoDetectDecoder.INSTANCE.decode(v2Bytes);

        assertNotNull(decodedEntry);
        assertEquals(originalEntry.getType(), decodedEntry.getType());
        assertEquals(originalEntry.getId(), decodedEntry.getId());
        assertEquals(originalEntry.getChecksum(), decodedEntry.getChecksum());
        assertEquals(originalEntry.getSequenceToken(), decodedEntry.getSequenceToken());
        assertEquals(originalEntry.getOldSequenceToken(), decodedEntry.getOldSequenceToken());
    }

    /**
     * Test that V1 decoder returns null for V2-encoded entries (incompatible).
     */
    @Test
    void testV1DecoderRejectsV2Entries() {
        LogEntry originalEntry = createDataEntry();
        originalEntry.setSequenceToken(12345);
        originalEntry.setOldSequenceToken(12340);

        // Encode with V2
        byte[] v2Bytes = V2Encoder.INSTANCE.encode(originalEntry);

        // Try to decode with V1 decoder - should return null (wrong magic byte)
        LogEntry decodedEntry = V1Decoder.INSTANCE.decode(v2Bytes);

        assertEquals(null, decodedEntry);
    }

    /**
     * Test that V2 decoder returns null for V1-encoded entries (incompatible).
     */
    @Test
    void testV2DecoderRejectsV1Entries() {
        LogEntry originalEntry = createDataEntry();

        // Encode with V1
        byte[] v1Bytes = V1Encoder.INSTANCE.encode(originalEntry);

        // Try to decode with V2 decoder - should return null (wrong magic byte)
        LogEntry decodedEntry = V2Decoder.INSTANCE.decode(v1Bytes);

        assertEquals(null, decodedEntry);
    }

    /**
     * Test configuration entry with peers and learners for backward compatibility.
     */
    @Test
    void testConfigurationEntryBackwardCompatibility() {
        LogEntry originalEntry = createCfgEntry();

        // Encode with V1
        byte[] v1Bytes = V1Encoder.INSTANCE.encode(originalEntry);

        // Decode with AutoDetectDecoder
        LogEntry decodedEntry = AutoDetectDecoder.INSTANCE.decode(v1Bytes);

        assertNotNull(decodedEntry);
        assertEquals(originalEntry.getType(), decodedEntry.getType());
        assertEquals(originalEntry.getPeers(), decodedEntry.getPeers());
        assertEquals(originalEntry.getOldPeers(), decodedEntry.getOldPeers());
        assertEquals(originalEntry.getLearners(), decodedEntry.getLearners());
        assertEquals(originalEntry.getOldLearners(), decodedEntry.getOldLearners());

        // V1 entries should have zero sequence tokens when decoded
        assertEquals(0, decodedEntry.getSequenceToken());
        assertEquals(0, decodedEntry.getOldSequenceToken());
    }

    /**
     * Test that V2 entries with zero sequence tokens work correctly.
     */
    @Test
    void testV2WithZeroSequenceTokens() {
        LogEntry originalEntry = createDataEntry();
        originalEntry.setSequenceToken(0);
        originalEntry.setOldSequenceToken(0);

        // Encode with V2
        byte[] v2Bytes = V2Encoder.INSTANCE.encode(originalEntry);

        // Decode with V2
        LogEntry decodedEntry = V2Decoder.INSTANCE.decode(v2Bytes);

        assertNotNull(decodedEntry);
        assertEquals(0, decodedEntry.getSequenceToken());
        assertEquals(0, decodedEntry.getOldSequenceToken());
        assertEquals(originalEntry, decodedEntry);
    }

    /**
     * Test that DefaultLogEntryCodecFactory uses V2 for encoding and can decode both V1 and V2.
     */
    @Test
    void testDefaultFactoryUsesV2() {
        LogEntryCodecFactory factory = DefaultLogEntryCodecFactory.getInstance();

        LogEntry originalEntry = createDataEntry();
        originalEntry.setSequenceToken(55555);
        originalEntry.setOldSequenceToken(44444);

        // Encode using factory encoder (should be V2)
        byte[] bytes = factory.encoder().encode(originalEntry);

        // Decode using factory decoder (should be AutoDetectDecoder)
        LogEntry decodedEntry = factory.decoder().decode(bytes);

        assertNotNull(decodedEntry);
        assertEquals(originalEntry.getSequenceToken(), decodedEntry.getSequenceToken());
        assertEquals(originalEntry.getOldSequenceToken(), decodedEntry.getOldSequenceToken());
    }

    private static LogEntry createDataEntry() {
        LogEntry logEntry = new LogEntry(EntryType.ENTRY_TYPE_DATA);

        LogId id = new LogId();
        id.setIndex(1000);
        id.setTerm(10_000);

        logEntry.setId(id);
        logEntry.setChecksum(100_000);

        byte[] bytes = new byte[4096];
        ThreadLocalRandom.current().nextBytes(bytes);
        logEntry.setData(ByteBuffer.wrap(bytes));

        return logEntry;
    }

    private static LogEntry createCfgEntry() {
        LogEntry logEntry = new LogEntry(EntryType.ENTRY_TYPE_CONFIGURATION);

        LogId id = new LogId();
        id.setIndex(1000);
        id.setTerm(10_000);

        logEntry.setId(id);
        logEntry.setChecksum(100_000);

        logEntry.setOldLearners(List.of(new PeerId("oldLearner")));
        logEntry.setOldPeers(List.of(new PeerId("oldPeer")));

        logEntry.setLearners(List.of(new PeerId("learner")));
        logEntry.setPeers(List.of(new PeerId("peer")));

        return logEntry;
    }
}
