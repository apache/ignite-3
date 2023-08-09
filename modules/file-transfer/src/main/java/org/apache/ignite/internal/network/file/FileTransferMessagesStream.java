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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.ignite.internal.network.file.messages.FileChunkMessage;
import org.apache.ignite.internal.network.file.messages.FileTransferFactory;
import org.jetbrains.annotations.NotNull;

/**
 * Stream of messages to send files.
 */
public class FileTransferMessagesStream implements Iterable<FileChunkMessage>, AutoCloseable {
    private final UUID transferId;

    private final ChunkedFileReader reader;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final FileTransferFactory messageFactory = new FileTransferFactory();

    /**
     * Creates a new stream of messages to send files.
     *
     * @param transferId the id of the stream.
     * @param reader the reader of the file to send.
     */
    private FileTransferMessagesStream(
            UUID transferId,
            ChunkedFileReader reader
    ) {
        this.transferId = transferId;
        this.reader = reader;
    }

    /**
     * Creates a new stream of messages to send file.
     *
     * @param chunkSize the size of the chunks to send.
     * @param transferId the id of the transfer.
     * @param path the path of the file to send.
     * @return a new stream of messages to send files.
     * @throws IOException if an I/O error occurs.
     */
    public static FileTransferMessagesStream fromPath(
            int chunkSize,
            UUID transferId,
            Path path
    ) throws IOException {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("Chunk size must be positive");
        }

        return new FileTransferMessagesStream(transferId, ChunkedFileReader.open(path.toFile(), chunkSize));
    }

    /**
     * Returns true if there are more messages to send.
     *
     * @return true if there are more messages to send.
     */
    boolean hasNextMessage() throws IOException {
        // Check that the stream is not closed and the reader is not finished.
        return !closed.get() && !reader.isFinished();
    }

    /**
     * Returns the next message to send.
     *
     * @return the next message to send.
     * @throws IOException if an I/O error occurs.
     * @throws IllegalStateException if there are no more messages to send.
     */
    FileChunkMessage nextMessage() throws IOException {
        if (hasNextMessage()) {
            return nextChunk();
        } else {
            throw new IllegalStateException("There are no more messages to send");
        }
    }

    /**
     * Returns the next chunk of the current file. Throws an exception if the current file is finished.
     *
     * @return the next chunk of the current file.
     * @throws IOException if an I/O error occurs.
     * @throws IllegalStateException if the current file is finished.
     */
    private FileChunkMessage nextChunk() throws IOException {
        return messageFactory.fileChunkMessage()
                .transferId(transferId)
                .fileName(reader.fileName())
                .offset(reader.offset())
                .data(reader.readNextChunk())
                .build();
    }

    /**
     * Closes the stream.
     */
    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            reader.close();
        }
    }

    @NotNull
    @Override
    public Iterator<FileChunkMessage> iterator() {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                try {
                    return hasNextMessage();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public FileChunkMessage next() {
                try {
                    return nextMessage();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };
    }

    @Override
    public void forEach(Consumer<? super FileChunkMessage> action) {
        for (FileChunkMessage message : this) {
            action.accept(message);
        }
    }
}
