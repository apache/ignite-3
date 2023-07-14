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

package org.apache.ignite.raft.jraft.storage.logit.file;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.raft.jraft.storage.logit.BaseLogitStorageTest;
import org.apache.ignite.raft.jraft.storage.logit.storage.file.AbstractFile;
import org.apache.ignite.raft.jraft.storage.logit.storage.file.FileManager;
import org.apache.ignite.raft.jraft.storage.logit.storage.file.FileType;
import org.apache.ignite.raft.jraft.storage.logit.storage.file.index.IndexFile;
import org.apache.ignite.raft.jraft.storage.logit.storage.service.AllocateFileService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Use indexFile to test fileManager
 */
public class FileManagerTest extends BaseLogitStorageTest {
    private FileManager fileManager;
    private AllocateFileService allocateService;
    private String              indexStorePath;

    @BeforeEach
    @Override
    public void setup() throws Exception {
        super.setup();
        this.indexStorePath = this.path + File.separator + "index";
        Files.createDirectories(Path.of(indexStorePath));
        this.allocateService = new AllocateFileService(FileType.FILE_INDEX, indexStorePath, this.logStoreFactory);
        this.allocateService.start();
        this.fileManager = this.logStoreFactory.newFileManager(FileType.FILE_INDEX, indexStorePath,
            this.allocateService);
    }

    @AfterEach
    public void teardown() throws Exception {
        this.fileManager.shutdown();
        this.allocateService.shutdown(true);
    }

    /**
     * When call writeDataToFirstFile and writeDataToSecondFile
     * The fileManager's file state is :
     *
     * fileId   fileFromOffset    firstLogIndex  lastLogIndex  fileLastOffset         wrotePosition
     * 0        0                 0              9             26 + 100 =216          26 + 100
     * 1        26 + 100          10             15            26 + 26 + 160 = 212    26 + 60
     */
    @Test
    public void writeDataToFirstFile() {
        // Append 10 index to first file , and come to the file end (size:130)
        {
            for (int i = 0; i < 10; i++) {
                final AbstractFile lastFile = this.fileManager.getLastFile(i, 10, true);
                assert (lastFile instanceof IndexFile);
                final IndexFile indexFile = (IndexFile) lastFile;
                indexFile.appendIndex(i, i, segmentIndex);
            }
        }
    }

    @Test
    public void writeDataToSecondFile() {
        writeDataToFirstFile();

        // Try get last file again , this file is a new blank file (from allocator)
        final AbstractFile lastFile = this.fileManager.getLastFile(10, 10, true);
        assertEquals(this.indexFileSize, lastFile.getFileFromOffset());

        // Write 5 index to second file , wrotePosition = 30 + 50
        final IndexFile indexFile = (IndexFile) lastFile;
        for (int i = 10; i <= 15; i++) {
            indexFile.appendIndex(i, i, segmentIndex);
        }
    }

    @Test
    public void testFindAbstractFileByOffset() {
        writeDataToSecondFile();
        {
            // Test find first file by offset 0
            final AbstractFile firstFile = this.fileManager.findFileByOffset(30, false);
            assertEquals(0, firstFile.getFileFromOffset());
            // Test find second file by offset 136
            final AbstractFile secondFile = this.fileManager.findFileByOffset(this.indexFileSize + 10, false);
            assertEquals(this.indexFileSize, secondFile.getFileFromOffset());
        }
    }

    @Test
    public void testFlush() {
        writeDataToSecondFile();

        {
            // First time  flush , flush position = indexFileSize (126)
            this.fileManager.flush();
            assertEquals(126, this.fileManager.getFlushedPosition());
        }

        {
            // Second time  flush , flush position = 212
            this.fileManager.flush();
            assertEquals(212, this.fileManager.getFlushedPosition());
        }
    }

    @Test
    public void testTruncateSuffix() {
        // Current flush position = 212
        testFlush();
        // Test truncate to logIndex = 10 , that means update flush position to 126 + 26 + 10 = 162
        this.fileManager.truncateSuffix(10, 0);
        assertEquals(162, this.fileManager.getFlushedPosition());
        assertEquals(10, this.fileManager.getLastLogIndex());
    }
}
