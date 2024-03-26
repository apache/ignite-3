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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;

/**
 * Chunked file reader. Reads the file in chunks. Each chunk has a fixed size. The last chunk may be smaller than the chunk size. If the
 * file size is less than the chunk size, only one chunk will be read. The reader is not thread-safe.
 */
class ChunkedFileReader implements AutoCloseable {
    private final int chunkSize;

    private final BufferedInputStream stream;

    private int nextChunkNumber = 0;

    private boolean hasNextChunk;

    /**
     * Constructor.
     *
     * @param chunkSize Chunk size.
     * @param stream Random access file.
     */
    private ChunkedFileReader(int chunkSize, BufferedInputStream stream) throws IOException {
        this.chunkSize = chunkSize;
        this.stream = stream;
        this.hasNextChunk = stream.available() > 0;
    }

    /**
     * Opens a file for reading.
     *
     * @param file File.
     * @param chunkSize Chunk size.
     * @return Chunked file reader.
     * @throws FileNotFoundException If the file does not exist.
     */
    static ChunkedFileReader open(File file, int chunkSize) throws IOException {
        return new ChunkedFileReader(chunkSize, new BufferedInputStream(Files.newInputStream(file.toPath())));
    }

    /**
     * Returns {@code false} if there are no more chunks to read. Otherwise, returns {@code true}. Does not change the state of the reader.
     *
     * @return {@code false} if there are no more chunks to read. Otherwise, returns {@code true}.
     */
    boolean hasNextChunk() {
        return hasNextChunk;
    }

    /**
     * Reads the next chunk. If there are no more chunks to read, throws an exception. If the last chunk is read successfully, closes the
     * file.
     *
     * @return Chunk data.
     * @throws IOException If an I/O error occurs.
     */
    byte[] readNextChunk() throws IOException {
        if (!hasNextChunk()) {
            throw new IOException("No more chunks to read");
        }

        byte[] data = stream.readNBytes(chunkSize);

        nextChunkNumber++;
        hasNextChunk = stream.available() > 0;

        if (!hasNextChunk) {
            stream.close();
        }

        return data;
    }

    /**
     * Returns the number of the next chunk to read. Does not change the state of the reader.
     *
     * @return The number of the next chunk to read.
     * @throws IOException If there are no more chunks to read.
     */
    int nextChunkNumber() throws IOException {
        if (!hasNextChunk()) {
            throw new IOException("No more chunks to read");
        }

        return nextChunkNumber;
    }

    /**
     * Closes the file.
     *
     * @throws IOException If an I/O error occurs.
     */
    @Override
    public void close() throws IOException {
        stream.close();
    }
}
