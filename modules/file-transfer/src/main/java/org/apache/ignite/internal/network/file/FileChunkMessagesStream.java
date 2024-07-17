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
import java.util.function.Consumer;
import org.apache.ignite.internal.network.file.messages.FileChunkMessage;
import org.apache.ignite.internal.network.file.messages.FileTransferFactory;

/**
 * A stream of {@link FileChunkMessage} to send file.
 */
public class FileChunkMessagesStream implements Iterable<FileChunkMessage>, AutoCloseable {
    private final UUID transferId;

    private final Path path;

    private final ChunkedFileReader reader;

    private boolean closed = false;

    private final FileTransferFactory messageFactory = new FileTransferFactory();


    /**
     * Creates a new stream of messages to send files.
     *
     * @param transferId the id of the stream.
     * @param reader the reader of the file to send.
     */
    private FileChunkMessagesStream(
            UUID transferId,
            Path path,
            ChunkedFileReader reader
    ) {
        this.transferId = transferId;
        this.path = path;
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
    public static FileChunkMessagesStream fromPath(
            int chunkSize,
            UUID transferId,
            Path path
    ) throws IOException {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("Chunk size must be positive");
        }

        return new FileChunkMessagesStream(transferId, path, ChunkedFileReader.open(path.toFile(), chunkSize));
    }

    /**
     * Returns true if there are more messages to send.
     *
     * @return true if there are more messages to send.
     */
    boolean hasNextMessage() throws IOException {
        // Check that the stream is not closed and the reader has more chunks.
        return !closed && reader.hasNextChunk();
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
                .fileName(path.getFileName().toString())
                .number(reader.nextChunkNumber())
                .data(reader.readNextChunk())
                .build();
    }

    public Path path() {
        return path;
    }

    /**
     * Closes the stream.
     */
    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            reader.close();
        }
    }

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
