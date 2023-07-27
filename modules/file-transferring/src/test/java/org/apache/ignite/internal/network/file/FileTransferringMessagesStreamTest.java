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

package org.apache.ignite.internal.network.file;

import static org.apache.ignite.internal.network.file.FileGenerator.randomFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.network.file.messages.ChunkedFile;
import org.apache.ignite.internal.network.file.messages.FileHeader;
import org.apache.ignite.internal.network.file.messages.FileTransferInfo;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
class FileTransferringMessagesStreamTest {

    @WorkDirectory
    private Path workDir;

    @Test
    void negativeChunkSize() {
        UUID transferId = UUID.randomUUID();
        File file = randomFile(workDir, 1024).toFile();
        List<File> filesToSend = List.of(file);
        int chunkSize = -1;

        assertThrows(IllegalArgumentException.class, () -> new FileTransferringMessagesStream(transferId, filesToSend, chunkSize));
    }

    @Test
    void emptyFileList() throws IOException {
        UUID transferId = UUID.randomUUID();
        List<File> filesToSend = List.of();
        int chunkSize = 1024 * 1024; // 1 MB

        assertThrows(IllegalArgumentException.class, () -> new FileTransferringMessagesStream(transferId, filesToSend, chunkSize));
    }

    @Test
    void listOfEmptyFiles() throws IOException {
        UUID transferId = UUID.randomUUID();
        File file1 = randomFile(workDir, 0).toFile();
        File file2 = randomFile(workDir, 0).toFile();
        List<File> filesToSend = List.of(file1, file2);
        int chunkSize = 1024 * 1024; // 1 MB

        FileTransferringMessagesStream stream = new FileTransferringMessagesStream(transferId, filesToSend, chunkSize);

        try {
            // check transfer FileTransferInfo
            assertTrue(stream.hasNextMessage());
            FileTransferInfo info = (FileTransferInfo) stream.nextMessage();
            assertEquals(transferId, info.transferId());
            assertEquals(2, info.filesCount());

            // check the first FileHeader
            assertTrue(stream.hasNextMessage());
            FileHeader header1 = (FileHeader) stream.nextMessage();
            assertEquals(transferId, header1.transferId());
            assertEquals(header1.fileName(), file1.getName());
            assertEquals(file1.length(), header1.fileSize());

            // check the second FileHeader
            assertTrue(stream.hasNextMessage());
            FileHeader header2 = (FileHeader) stream.nextMessage();
            assertEquals(transferId, header2.transferId());
            assertEquals(header2.fileName(), file2.getName());
            assertEquals(file2.length(), header2.fileSize());

            // check the stream is empty
            assertFalse(stream.hasNextMessage());
        } finally {
            stream.close();
        }
    }

    @Test
    void listOfSmallFiles() throws IOException {
        UUID transferId = UUID.randomUUID();
        File file1 = randomFile(workDir, 1024).toFile();
        File file2 = randomFile(workDir, 1024).toFile();
        List<File> filesToSend = List.of(file1, file2);
        int chunkSize = 1024 * 1024; // 1 MB

        FileTransferringMessagesStream stream = new FileTransferringMessagesStream(transferId, filesToSend, chunkSize);

        try {
            // check transfer FileTransferInfo
            assertTrue(stream.hasNextMessage());
            FileTransferInfo info = (FileTransferInfo) stream.nextMessage();
            assertEquals(transferId, info.transferId());
            assertEquals(2, info.filesCount());

            // check the first FileHeader
            assertTrue(stream.hasNextMessage());
            FileHeader header1 = (FileHeader) stream.nextMessage();
            assertEquals(transferId, header1.transferId());
            assertEquals(header1.fileName(), file1.getName());
            assertEquals(file1.length(), header1.fileSize());

            // check the first ChunkedFile
            assertTrue(stream.hasNextMessage());
            ChunkedFile chunkedFile1 = (ChunkedFile) stream.nextMessage();
            assertEquals(transferId, chunkedFile1.transferId());
            assertEquals(header1.fileName(), chunkedFile1.fileName());
            assertEquals(0, chunkedFile1.offset());
            assertEquals(file1.length(), chunkedFile1.data().length);

            // check the second FileHeader
            assertTrue(stream.hasNextMessage());
            FileHeader header2 = (FileHeader) stream.nextMessage();
            assertEquals(transferId, header2.transferId());
            assertEquals(header2.fileName(), file2.getName());
            assertEquals(file2.length(), header2.fileSize());

            // check the second ChunkedFile
            assertTrue(stream.hasNextMessage());
            ChunkedFile chunkedFile2 = (ChunkedFile) stream.nextMessage();
            assertEquals(transferId, chunkedFile2.transferId());
            assertEquals(header2.fileName(), chunkedFile2.fileName());
            assertEquals(0, chunkedFile2.offset());
            assertEquals(file2.length(), chunkedFile2.data().length);

            // check the stream is empty
            assertFalse(stream.hasNextMessage());
        } finally {
            stream.close();
        }
    }

    @Test
    void listOfBigFiles() throws IOException {
        UUID transferId = UUID.randomUUID();
        File file1 = randomFile(workDir, 1948).toFile();
        File file2 = randomFile(workDir, 1724).toFile();
        List<File> filesToSend = List.of(file1, file2);
        int chunkSize = 1024; // 1 KB

        FileTransferringMessagesStream stream = new FileTransferringMessagesStream(transferId, filesToSend, chunkSize);

        try {
            // check transfer FileTransferInfo
            assertTrue(stream.hasNextMessage());
            FileTransferInfo info = (FileTransferInfo) stream.nextMessage();
            assertEquals(transferId, info.transferId());
            assertEquals(2, info.filesCount());

            // check the first FileHeader
            assertTrue(stream.hasNextMessage());
            FileHeader header1 = (FileHeader) stream.nextMessage();
            assertEquals(transferId, header1.transferId());
            assertEquals(header1.fileName(), file1.getName());
            assertEquals(file1.length(), header1.fileSize());

            // check the first ChunkedFile
            assertTrue(stream.hasNextMessage());
            ChunkedFile chunkedFile1 = (ChunkedFile) stream.nextMessage();
            assertEquals(transferId, chunkedFile1.transferId());
            assertEquals(header1.fileName(), chunkedFile1.fileName());
            assertEquals(0, chunkedFile1.offset());
            assertEquals(chunkSize, chunkedFile1.data().length);

            // check the second ChunkedFile
            assertTrue(stream.hasNextMessage());
            ChunkedFile chunkedFile2 = (ChunkedFile) stream.nextMessage();
            assertEquals(transferId, chunkedFile2.transferId());
            assertEquals(header1.fileName(), chunkedFile2.fileName());
            assertEquals(chunkSize, chunkedFile2.offset());
            assertEquals(file1.length() - chunkSize, chunkedFile2.data().length);

            // check the second FileHeader
            assertTrue(stream.hasNextMessage());
            FileHeader header2 = (FileHeader) stream.nextMessage();
            assertEquals(transferId, header2.transferId());
            assertEquals(header2.fileName(), file2.getName());
            assertEquals(file2.length(), header2.fileSize());

            // check the third ChunkedFile
            assertTrue(stream.hasNextMessage());
            ChunkedFile chunkedFile3 = (ChunkedFile) stream.nextMessage();
            assertEquals(transferId, chunkedFile3.transferId());
            assertEquals(header2.fileName(), chunkedFile3.fileName());
            assertEquals(0, chunkedFile3.offset());
            assertEquals(chunkSize, chunkedFile3.data().length);

            // check the fourth ChunkedFile
            assertTrue(stream.hasNextMessage());
            ChunkedFile chunkedFile4 = (ChunkedFile) stream.nextMessage();
            assertEquals(transferId, chunkedFile4.transferId());
            assertEquals(header2.fileName(), chunkedFile4.fileName());
            assertEquals(chunkSize, chunkedFile4.offset());
            assertEquals(file2.length() - chunkSize, chunkedFile4.data().length);

            // check the stream is empty
            assertFalse(stream.hasNextMessage());
        } finally {
            stream.close();
        }
    }

    @Test
    void listOfDifferentFiles() throws IOException {
        UUID transferId = UUID.randomUUID();
        int chunkSize = 1024 * 1024; // 1 MB
        File file1 = randomFile(workDir, chunkSize * 2).toFile();
        File file2 = randomFile(workDir, 0).toFile();
        File file3 = randomFile(workDir, chunkSize).toFile();
        List<File> filesToSend = List.of(file1, file2, file3);

        FileTransferringMessagesStream stream = new FileTransferringMessagesStream(transferId, filesToSend, chunkSize);

        try {
            // check transfer FileTransferInfo
            assertTrue(stream.hasNextMessage());
            FileTransferInfo info = (FileTransferInfo) stream.nextMessage();
            assertEquals(transferId, info.transferId());
            assertEquals(3, info.filesCount());

            // check the first FileHeader
            assertTrue(stream.hasNextMessage());
            FileHeader header1 = (FileHeader) stream.nextMessage();
            assertEquals(transferId, header1.transferId());
            assertEquals(header1.fileName(), file1.getName());
            assertEquals(file1.length(), header1.fileSize());

            // check the first ChunkedFile
            assertTrue(stream.hasNextMessage());
            ChunkedFile chunkedFile1 = (ChunkedFile) stream.nextMessage();
            assertEquals(transferId, chunkedFile1.transferId());
            assertEquals(header1.fileName(), chunkedFile1.fileName());
            assertEquals(0, chunkedFile1.offset());
            assertEquals(chunkSize, chunkedFile1.data().length);

            // check the second ChunkedFile
            assertTrue(stream.hasNextMessage());
            ChunkedFile chunkedFile2 = (ChunkedFile) stream.nextMessage();
            assertEquals(transferId, chunkedFile2.transferId());
            assertEquals(header1.fileName(), chunkedFile2.fileName());
            assertEquals(chunkSize, chunkedFile2.offset());
            assertEquals(chunkSize, chunkedFile2.data().length);

            // check the second FileHeader
            assertTrue(stream.hasNextMessage());
            FileHeader header2 = (FileHeader) stream.nextMessage();
            assertEquals(transferId, header2.transferId());
            assertEquals(header2.fileName(), file2.getName());
            assertEquals(file2.length(), header2.fileSize());

            // check the third FileHeader
            assertTrue(stream.hasNextMessage());
            FileHeader header3 = (FileHeader) stream.nextMessage();
            assertEquals(transferId, header3.transferId());
            assertEquals(header3.fileName(), file3.getName());
            assertEquals(file3.length(), header3.fileSize());

            // check the third ChunkedFile
            assertTrue(stream.hasNextMessage());
            ChunkedFile chunkedFile3 = (ChunkedFile) stream.nextMessage();
            assertEquals(transferId, chunkedFile3.transferId());
            assertEquals(header3.fileName(), chunkedFile3.fileName());
            assertEquals(0, chunkedFile3.offset());
            assertEquals(chunkSize, chunkedFile3.data().length);

            // check the stream is empty
            assertFalse(stream.hasNextMessage());
        } finally {
            stream.close();
        }
    }
}
