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

package org.apache.ignite.internal.raft.storage.segstore;

import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentFileManager.SWITCH_SEGMENT_RECORD;
import static org.apache.ignite.internal.raft.storage.segstore.SegstoreLogStorage.GROUP_ID_SIZE_BYTES;
import static org.apache.ignite.internal.raft.storage.segstore.SegstoreLogStorage.HASH_SIZE;
import static org.apache.ignite.internal.raft.storage.segstore.SegstoreLogStorage.LENGTH_SIZE_BYTES;
import static org.apache.ignite.internal.raft.storage.segstore.SegstoreLogStorage.entrySize;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.util.FastCrc;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryCodecFactory;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryDecoder;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryEncoder;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegstoreLogStorageTest extends IgniteAbstractTest {
    private static final int SEGMENT_SIZE = 1024;

    private static final long GROUP_ID = 1000;

    private SegstoreLogStorage logStorage;

    private SegmentFileManager segmentFileManager;

    @Mock
    private LogEntryEncoder encoder;

    @BeforeEach
    void setUp() throws IOException {
        segmentFileManager = new SegmentFileManager(workDir, SEGMENT_SIZE);

        logStorage = new SegstoreLogStorage(GROUP_ID, segmentFileManager);

        var opts = new LogStorageOptions();

        opts.setLogEntryCodecFactory(new LogEntryCodecFactory() {
            @Override
            public LogEntryEncoder encoder() {
                return encoder;
            }

            @Override
            public LogEntryDecoder decoder() {
                return fail("Should not be called.");
            }
        });

        segmentFileManager.start();

        logStorage.init(opts);
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAllManually(
                logStorage == null ? null : logStorage::shutdown,
                segmentFileManager
        );
    }

    @Test
    void testAppendEntry() throws IOException {
        byte[] payload = {1, 2, 3, 4, 5};

        when(encoder.encode(any())).thenReturn(payload);

        logStorage.appendEntry(new LogEntry());

        List<Path> segmentFiles = segmentFiles();

        assertThat(segmentFiles, hasSize(1));

        try (InputStream is = Files.newInputStream(segmentFiles.get(0))) {
            // Skip header.
            is.readNBytes(SegmentFileManager.HEADER_RECORD.length);

            ByteBuffer entry = ByteBuffer.wrap(is.readNBytes(entrySize(payload))).order(SegmentFile.BYTE_ORDER);

            validateEntry(entry, payload);
        }
    }

    @Test
    void testAppendEntries() throws IOException {
        int bytesToGenerate = SEGMENT_SIZE * 3;

        int maxPayloadSize = 100;

        var payloads = new ArrayList<byte[]>();

        ThreadLocalRandom random = ThreadLocalRandom.current();

        while (bytesToGenerate > 0) {
            int payloadSize = random.nextInt(maxPayloadSize);

            var payload = new byte[payloadSize];

            random.nextBytes(payload);

            payloads.add(payload);

            bytesToGenerate -= payloadSize;
        }

        Iterator<byte[]> payloadsIterator = payloads.iterator();

        when(encoder.encode(any())).thenAnswer(invocation -> payloadsIterator.next());

        assertThat(logStorage.appendEntries(nCopies(payloads.size(), new LogEntry())), is(payloads.size()));

        var entries = new ArrayList<ByteBuffer>(payloads.size());

        for (Path segmentFile : segmentFiles()) {
            try (InputStream is = Files.newInputStream(segmentFile)) {
                // Skip header.
                is.readNBytes(SegmentFileManager.HEADER_RECORD.length);

                long bytesRead = SegmentFileManager.HEADER_RECORD.length;

                while (bytesRead < SEGMENT_SIZE - SWITCH_SEGMENT_RECORD.length) {
                    long groupId = ByteBuffer.wrap(is.readNBytes(GROUP_ID_SIZE_BYTES)).order(SegmentFile.BYTE_ORDER).getLong();

                    if (groupId == 0) {
                        // EOF reached.
                        break;
                    }

                    int payloadLength = ByteBuffer.wrap(is.readNBytes(LENGTH_SIZE_BYTES)).order(SegmentFile.BYTE_ORDER).getInt();
                    byte[] remaining = is.readNBytes(payloadLength + HASH_SIZE);

                    ByteBuffer entry = ByteBuffer.allocate(GROUP_ID_SIZE_BYTES + LENGTH_SIZE_BYTES + payloadLength + HASH_SIZE)
                            .order(SegmentFile.BYTE_ORDER)
                            .putLong(groupId)
                            .putInt(payloadLength)
                            .put(remaining)
                            .flip();

                    entries.add(entry);

                    bytesRead += entry.capacity();
                }
            }
        }

        for (int i = 0; i < entries.size(); i++) {
            validateEntry(entries.get(i), payloads.get(i));
        }
    }

    private List<Path> segmentFiles() throws IOException {
        try (Stream<Path> files = Files.list(workDir)) {
            return files.sorted().collect(toList());
        }
    }

    private static void validateEntry(ByteBuffer entry, byte[] expectedPayload) {
        assertThat(entry.getLong(), is(GROUP_ID));

        assertThat(entry.getInt(), is(expectedPayload.length));

        byte[] actualPayload = new byte[expectedPayload.length];
        entry.get(actualPayload);

        assertThat(actualPayload, is(expectedPayload));

        int entrySizeWithoutCrc = entry.position();
        int actualCrc = entry.getInt();
        int expectedCrc = FastCrc.calcCrc(entry.rewind(), entrySizeWithoutCrc);

        assertThat(actualCrc, is(expectedCrc));
    }
}
