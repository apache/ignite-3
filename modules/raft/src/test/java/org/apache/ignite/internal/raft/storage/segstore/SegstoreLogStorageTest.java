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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentFileManager.SWITCH_SEGMENT_RECORD;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayload.overheadSize;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
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
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
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
        segmentFileManager = new SegmentFileManager(workDir, SEGMENT_SIZE, 1);

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

        doAnswer(invocation -> {
            ByteBuffer buffer = invocation.getArgument(0);

            buffer.put(payload);

            return null;
        }).when(encoder).encode(any(), any());

        when(encoder.size(any())).thenAnswer(invocation -> payload.length);

        logStorage.appendEntry(new LogEntry());

        List<Path> segmentFiles = segmentFiles();

        assertThat(segmentFiles, hasSize(1));

        try (InputStream is = Files.newInputStream(segmentFiles.get(0))) {
            // Skip header.
            is.readNBytes(SegmentFileManager.HEADER_RECORD.length);

            DeserializedSegmentPayload entry = DeserializedSegmentPayload.fromBytes(is.readNBytes(overheadSize() + payload.length));

            assertThat(entry.groupId(), is(GROUP_ID));
            assertThat(entry.payload(), is(payload));
        }
    }

    @Test
    void testAppendEntries() throws IOException {
        List<byte[]> payloads = generateRandomData();

        var iteratorEncoder = new LogEntryEncoder() {
            private final Iterator<byte[]> payloadsIterator = payloads.iterator();

            private byte[] nextPayload;

            @Override
            public byte[] encode(LogEntry log) {
                return fail("Should not be called.");
            }

            @Override
            public void encode(ByteBuffer buffer, LogEntry log) {
                buffer.put(nextPayload);
            }

            @Override
            public int size(LogEntry logEntry) {
                nextPayload = payloadsIterator.next();

                return nextPayload.length;
            }
        };

        doAnswer(invocation -> {
            iteratorEncoder.encode(invocation.getArgument(0), invocation.getArgument(1));

            return null;
        }).when(encoder).encode(any(), any());

        when(encoder.size(any())).thenAnswer(invocation -> iteratorEncoder.size(invocation.getArgument(0)));

        List<LogEntry> entries = IntStream.range(0, payloads.size())
                .mapToObj(i -> {
                    var entry = new LogEntry();

                    entry.setId(new LogId(i, 0));

                    return entry;
                })
                .collect(toList());

        assertThat(logStorage.appendEntries(entries), is(payloads.size()));

        var actualEntries = new ArrayList<DeserializedSegmentPayload>(payloads.size());

        for (Path segmentFile : segmentFiles()) {
            try (InputStream is = Files.newInputStream(segmentFile)) {
                // Skip header.
                is.readNBytes(SegmentFileManager.HEADER_RECORD.length);

                long bytesRead = SegmentFileManager.HEADER_RECORD.length;

                while (bytesRead < SEGMENT_SIZE - SWITCH_SEGMENT_RECORD.length) {
                    DeserializedSegmentPayload entry = DeserializedSegmentPayload.fromInputStream(is);

                    if (entry == null) {
                        // EOF reached.
                        break;
                    }

                    actualEntries.add(entry);

                    bytesRead += entry.size();
                }
            }
        }

        for (int i = 0; i < actualEntries.size(); i++) {
            assertThat(actualEntries.get(i).groupId(), is(GROUP_ID));
            assertThat(actualEntries.get(i).payload(), is(payloads.get(i)));
        }
    }

    private List<Path> segmentFiles() throws IOException {
        try (Stream<Path> files = Files.list(workDir)) {
            return files.sorted().collect(toList());
        }
    }

    private static List<byte[]> generateRandomData() {
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

        return payloads;
    }
}
