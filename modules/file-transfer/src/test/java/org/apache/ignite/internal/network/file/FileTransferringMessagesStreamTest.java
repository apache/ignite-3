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
import org.apache.ignite.internal.network.file.messages.FileChunk;
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


        FileTransferringMessagesStream stream = new FileTransferringMessagesStream(transferId, filesToSend, chunkSize);

        try {
            // check transfer FileTransferInfo
            assertTrue(stream.hasNextMessage());
            FileTransferInfo info = (FileTransferInfo) stream.nextMessage();
            assertEquals(transferId, info.transferId());
            assertEquals(0, info.filesCount());

            // check the stream is empty
            assertFalse(stream.hasNextMessage());
        } finally {
            stream.close();
        }
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
            FileChunk fileChunk1 = (FileChunk) stream.nextMessage();
            assertEquals(transferId, fileChunk1.transferId());
            assertEquals(header1.fileName(), fileChunk1.fileName());
            assertEquals(0, fileChunk1.offset());
            assertEquals(file1.length(), fileChunk1.data().length);

            // check the second FileHeader
            assertTrue(stream.hasNextMessage());
            FileHeader header2 = (FileHeader) stream.nextMessage();
            assertEquals(transferId, header2.transferId());
            assertEquals(header2.fileName(), file2.getName());
            assertEquals(file2.length(), header2.fileSize());

            // check the second ChunkedFile
            assertTrue(stream.hasNextMessage());
            FileChunk fileChunk2 = (FileChunk) stream.nextMessage();
            assertEquals(transferId, fileChunk2.transferId());
            assertEquals(header2.fileName(), fileChunk2.fileName());
            assertEquals(0, fileChunk2.offset());
            assertEquals(file2.length(), fileChunk2.data().length);

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
            FileChunk fileChunk1 = (FileChunk) stream.nextMessage();
            assertEquals(transferId, fileChunk1.transferId());
            assertEquals(header1.fileName(), fileChunk1.fileName());
            assertEquals(0, fileChunk1.offset());
            assertEquals(chunkSize, fileChunk1.data().length);

            // check the second ChunkedFile
            assertTrue(stream.hasNextMessage());
            FileChunk fileChunk2 = (FileChunk) stream.nextMessage();
            assertEquals(transferId, fileChunk2.transferId());
            assertEquals(header1.fileName(), fileChunk2.fileName());
            assertEquals(chunkSize, fileChunk2.offset());
            assertEquals(file1.length() - chunkSize, fileChunk2.data().length);

            // check the second FileHeader
            assertTrue(stream.hasNextMessage());
            FileHeader header2 = (FileHeader) stream.nextMessage();
            assertEquals(transferId, header2.transferId());
            assertEquals(header2.fileName(), file2.getName());
            assertEquals(file2.length(), header2.fileSize());

            // check the third ChunkedFile
            assertTrue(stream.hasNextMessage());
            FileChunk fileChunk3 = (FileChunk) stream.nextMessage();
            assertEquals(transferId, fileChunk3.transferId());
            assertEquals(header2.fileName(), fileChunk3.fileName());
            assertEquals(0, fileChunk3.offset());
            assertEquals(chunkSize, fileChunk3.data().length);

            // check the fourth ChunkedFile
            assertTrue(stream.hasNextMessage());
            FileChunk fileChunk4 = (FileChunk) stream.nextMessage();
            assertEquals(transferId, fileChunk4.transferId());
            assertEquals(header2.fileName(), fileChunk4.fileName());
            assertEquals(chunkSize, fileChunk4.offset());
            assertEquals(file2.length() - chunkSize, fileChunk4.data().length);

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
            FileChunk fileChunk1 = (FileChunk) stream.nextMessage();
            assertEquals(transferId, fileChunk1.transferId());
            assertEquals(header1.fileName(), fileChunk1.fileName());
            assertEquals(0, fileChunk1.offset());
            assertEquals(chunkSize, fileChunk1.data().length);

            // check the second ChunkedFile
            assertTrue(stream.hasNextMessage());
            FileChunk fileChunk2 = (FileChunk) stream.nextMessage();
            assertEquals(transferId, fileChunk2.transferId());
            assertEquals(header1.fileName(), fileChunk2.fileName());
            assertEquals(chunkSize, fileChunk2.offset());
            assertEquals(chunkSize, fileChunk2.data().length);

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
            FileChunk fileChunk3 = (FileChunk) stream.nextMessage();
            assertEquals(transferId, fileChunk3.transferId());
            assertEquals(header3.fileName(), fileChunk3.fileName());
            assertEquals(0, fileChunk3.offset());
            assertEquals(chunkSize, fileChunk3.data().length);

            // check the stream is empty
            assertFalse(stream.hasNextMessage());
        } finally {
            stream.close();
        }
    }
}
