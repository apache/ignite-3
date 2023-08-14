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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.ignite.internal.network.file.exception.FileValidationException;
import org.apache.ignite.internal.network.file.messages.FileChunkMessage;

/**
 * Chunked file writer. Writes chunks to a file. The writer is not thread-safe.
 */
class ChunkedFileWriter implements AutoCloseable {
    private final BufferedOutputStream stream;

    private final long expectedFileSize;

    private long bytesWritten = 0;

    private int nextChunkNumber = 0;

    private ChunkedFileWriter(BufferedOutputStream stream, long expectedFileSize) {
        this.stream = stream;
        this.expectedFileSize = expectedFileSize;
    }

    /**
     * Opens a file with known size for writing.
     *
     * @param file File path.
     * @param expectedFileSize Expected file size.
     * @return Chunked file writer.
     * @throws FileNotFoundException If the file is not found.
     */
    static ChunkedFileWriter open(File file, long expectedFileSize) throws IOException {
        return new ChunkedFileWriter(new BufferedOutputStream(new FileOutputStream(file)), expectedFileSize);
    }

    /**
     * Writes a chunk to the file. The chunk number must be equal to the next expected chunk number. Closes the file if the last chunk is
     * written.
     *
     * @param chunk Chunk to write.
     * @throws IOException If an I/O error occurs.
     * @throws FileValidationException If the chunk number or file size is invalid or expected file size is exceeded.
     */
    void write(FileChunkMessage chunk) throws IOException {
        if (chunk.number() != nextChunkNumber) {
            throw new FileValidationException("Chunk number mismatch: expected " + nextChunkNumber + ", actual " + chunk.number());
        }

        stream.write(chunk.data());
        nextChunkNumber++;
        bytesWritten += chunk.data().length;

        if (bytesWritten > expectedFileSize) {
            throw new FileValidationException("File size mismatch: expected " + expectedFileSize + ", actual " + bytesWritten);
        }

        if (bytesWritten == expectedFileSize) {
            stream.flush();
            close();
        }
    }

    /**
     * Checks if the file is complete. The file is complete if all chunks have been written to the file.
     *
     * @return {@code true} if the file is complete, {@code false} otherwise.
     * @throws IOException If an I/O error occurs.
     */
    boolean isComplete() throws IOException {
        return bytesWritten == expectedFileSize;
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
