/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.storage.snapshot.local;

import org.apache.ignite.raft.jraft.entity.LocalFileMetaOutter;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.BaseStorageTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class LocalSnapshotWriterTest extends BaseStorageTest {
    private RaftOptions opts;
    private LocalSnapshotWriter writer;
    @Mock
    private LocalSnapshotStorage snapshotStorage;

    @BeforeEach
    public void setup() throws Exception {
        opts = new RaftOptions();
        this.writer = new LocalSnapshotWriter(path.toString(), snapshotStorage, opts);
        assertTrue(this.writer.init(null));
    }

    @Test
    public void testAddRemove() throws Exception {
        assertNull(this.writer.getFileMeta("data"));
        assertFalse(this.writer.listFiles().contains("data"));
        assertTrue(this.writer.addFile("data"));
        assertFalse(this.writer.addFile("data"));
        assertTrue(this.writer.listFiles().contains("data"));
        assertNotNull(this.writer.getFileMeta("data"));
        assertTrue(this.writer.removeFile("data"));
        assertFalse(this.writer.removeFile("data"));
        assertNull(this.writer.getFileMeta("data"));
        assertFalse(this.writer.listFiles().contains("data"));
    }

    @Test
    public void testSyncInit() throws Exception {
        LocalFileMetaOutter.LocalFileMeta meta = opts.getRaftMessagesFactory()
            .localFileMeta()
            .checksum("test")
            .source(LocalFileMetaOutter.FileSource.FILE_SOURCE_LOCAL)
            .build();
        assertTrue(this.writer.addFile("data1", meta));
        assertTrue(this.writer.addFile("data2"));

        assertEquals(meta, this.writer.getFileMeta("data1"));
        assertNull(((LocalFileMetaOutter.LocalFileMeta) this.writer.getFileMeta("data2")).checksum());
        assertFalse(((LocalFileMetaOutter.LocalFileMeta) this.writer.getFileMeta("data2")).hasUserMeta());

        this.writer.sync();
        //create a new writer
        LocalSnapshotWriter newWriter = new LocalSnapshotWriter(path.toString(), snapshotStorage, new RaftOptions());
        assertTrue(newWriter.init(null));
        assertNotSame(writer, newWriter);
        assertEquals(meta, newWriter.getFileMeta("data1"));
        assertNull(((LocalFileMetaOutter.LocalFileMeta) newWriter.getFileMeta("data2")).checksum());
        assertFalse(((LocalFileMetaOutter.LocalFileMeta) newWriter.getFileMeta("data2")).hasUserMeta());
    }

    @Test
    public void testClose() throws Exception {
        this.writer.close();
        Mockito.verify(this.snapshotStorage, Mockito.only()).close(this.writer, false);
    }
}
