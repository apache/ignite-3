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

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.network.file.messages.FileChunk;
import org.apache.ignite.internal.network.file.messages.FileHeader;
import org.apache.ignite.internal.network.file.messages.FileTransferFactory;
import org.apache.ignite.internal.network.file.messages.FileTransferInfo;
import org.apache.ignite.network.NetworkMessage;
import org.jetbrains.annotations.Nullable;

/**
 * Stream of messages to send files.
 */
public class FileTransferMessagesStream implements AutoCloseable {
    private final UUID transferId;

    private final Queue<File> filesToSend;

    private final int chunkSize;

    @Nullable
    private ChunkedFileReader currFile;

    @Nullable
    private FileTransferInfo fileTransferInfo;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final FileTransferFactory factory = new FileTransferFactory();

    /**
     * Creates a new stream of messages to send files.
     *
     * @param transferId the id of the stream.
     * @param filesToSend the files to send. Must not be empty.
     * @param chunkSize the size of the chunks to send. Must be positive.
     */
    FileTransferMessagesStream(
            UUID transferId,
            List<File> filesToSend,
            int chunkSize
    ) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("Chunk size must be positive.");
        }

        if (filesToSend.isEmpty()) {
            throw new IllegalArgumentException("Files to send must not be empty.");
        }

        this.transferId = transferId;
        this.filesToSend = new LinkedList<>(filesToSend);
        this.chunkSize = chunkSize;
        this.fileTransferInfo = fileTransferInfo();
    }

    /**
     * Returns true if there are more messages to send.
     *
     * @return true if there are more messages to send.
     */
    boolean hasNextMessage() throws IOException {
        // check that the stream is not closed.
        if (closed.get()) {
            return false;
        } else {
            // check that there are more messages to send.
            // 1. there is a file transfer info message to send.
            // 2. there are files to send.
            // 3. there is a current file to send.
            return fileTransferInfo != null || !filesToSend.isEmpty() || (currFile != null && !currFile.isFinished());
        }
    }

    /**
     * Returns the next message to send.
     *
     * @return the next message to send.
     * @throws IOException if an I/O error occurs.
     * @throws IllegalStateException if there are no more messages to send.
     */
    NetworkMessage nextMessage() throws IOException {
        if (!hasNextMessage()) {
            throw new IllegalStateException("There are no more messages to send.");
        }

        if (fileTransferInfo != null) {
            FileTransferInfo info = fileTransferInfo;
            fileTransferInfo = null;
            return info;
        } else {
            if (currFile == null || currFile.isFinished()) {
                switchToNextFile();
                return header();
            } else {
                return nextChunk();
            }
        }
    }

    private FileTransferInfo fileTransferInfo() {
        return factory.fileTransferInfo()
                .transferId(transferId)
                .filesCount(filesToSend.size())
                .build();
    }

    /**
     * Returns the header of the current file to send.
     */
    private FileHeader header() throws IOException {
        assert currFile != null : "Current file is null.";

        return factory.fileHeader()
                .transferId(transferId)
                .fileName(currFile.fileName())
                .fileSize(currFile.length())
                .build();
    }

    /**
     * Returns the next chunk of the current file. Throws an exception if the current file is finished.
     *
     * @return the next chunk of the current file.
     * @throws IOException if an I/O error occurs.
     * @throws IllegalStateException if the current file is finished.
     */
    private FileChunk nextChunk() throws IOException {
        assert currFile != null : "Current file is null.";
        assert !currFile.isFinished() : "Current file is finished.";

        return factory.fileChunk()
                .transferId(transferId)
                .fileName(currFile.fileName())
                .offset(currFile.offset())
                .data(currFile.readNextChunk())
                .build();
    }

    private void switchToNextFile() throws IOException {
        closeCurrFile();

        if (filesToSend.isEmpty()) {
            throw new IllegalStateException("There are no more files to send.");
        } else {
            currFile = ChunkedFileReader.open(filesToSend.poll(), chunkSize);
        }
    }

    private void closeCurrFile() throws IOException {
        if (currFile != null) {
            currFile.close();
            currFile = null;
        }
    }

    /**
     * Closes the stream.
     */
    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        closeCurrFile();
    }
}
