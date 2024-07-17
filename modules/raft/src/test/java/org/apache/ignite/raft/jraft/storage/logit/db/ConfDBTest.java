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

package org.apache.ignite.raft.jraft.storage.logit.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryCodecFactory;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryDecoder;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryEncoder;
import org.apache.ignite.raft.jraft.entity.codec.v1.LogEntryV1CodecFactory;
import org.apache.ignite.raft.jraft.storage.logit.BaseLogitStorageTest;
import org.apache.ignite.raft.jraft.storage.logit.storage.db.AbstractDB;
import org.apache.ignite.raft.jraft.storage.logit.storage.db.ConfDB;
import org.apache.ignite.raft.jraft.storage.logit.util.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConfDBTest extends BaseLogitStorageTest {
    private ConfDB confDB;
    private String               confStorePath;
    private LogEntryCodecFactory logEntryCodecFactory;
    private LogEntryDecoder decoder;
    private LogEntryEncoder encoder;

    private ScheduledExecutorService checkpointExecutor;

    @BeforeEach
    @Override
    public void setup() throws Exception {
        super.setup();
        checkpointExecutor = Executors.newSingleThreadScheduledExecutor();
        this.confStorePath = this.path + File.separator + "conf";
        Files.createDirectories(Path.of(confStorePath));
        this.logEntryCodecFactory = LogEntryV1CodecFactory.getInstance();
        decoder = this.logEntryCodecFactory.decoder();
        encoder = this.logEntryCodecFactory.encoder();
        this.init();
    }

    public void init() {
        this.confDB = new ConfDB(this.confStorePath, checkpointExecutor);
        this.confDB.init(this.logStoreFactory);
    }

    @AfterEach
    public void teardown() throws Exception {
        this.confDB.shutdown();
        checkpointExecutor.shutdown();
    }

    @Test
    public void testAppendConfAndIter() throws Exception {
        this.confDB.startServiceManager();
        final LogEntry confEntry1 = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        confEntry1.setId(new LogId(1, 1));
        final List<PeerId> conf1Peers = JRaftUtils.getConfiguration("localhost:8081,localhost:8082").listPeers();
        confEntry1.setPeers(conf1Peers);

        final LogEntry confEntry2 = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        confEntry2.setId(new LogId(2, 2));
        final List<PeerId> conf2Peers = JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083")
            .listPeers();
        confEntry2.setPeers(conf2Peers);
        {

            this.confDB.appendLogAsync(1, this.encoder.encode(confEntry1));
            final Pair<Integer, Long> posPair = this.confDB.appendLogAsync(2, this.encoder.encode(confEntry2));
            this.confDB.waitForFlush(posPair.getSecond(), 100);
        }
        {
            final AbstractDB.LogEntryIterator iterator = this.confDB.iterator(this.decoder);
            final LogEntry conf1 = iterator.next();
            assertEquals(toString(conf1.getPeers()), toString(conf1Peers));

            final LogEntry conf2 = iterator.next();
            assertEquals(toString(conf2.getPeers()), toString(conf2Peers));

            assertNull(iterator.next());
        }
    }

    public String toString(final List<PeerId> peers) {
        final StringBuilder sb = new StringBuilder();
        int i = 0;
        int size = peers.size();
        for (final PeerId peer : peers) {
            sb.append(peer);
            if (i < size - 1) {
                sb.append(",");
            }
            i++;
        }
        return sb.toString();
    }
}
