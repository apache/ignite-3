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
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.internal.network.file.exception.FileValidationException;
import org.apache.ignite.internal.network.file.messages.FileChunkMessage;

/**
 * Chunked file writer. Writes chunks to a file. Checks that the file size and chunk numbers are valid.
 */
class ChunkedFileWriter implements AutoCloseable {
    private final BufferedOutputStream stream;

    private final long expectedFileLength;

    private long bytesWritten = 0;

    private int expectedNextChunkNumber = 0;

    /**
     * Lock to synchronize access to the file. We don't write to the file simultaneously, but the next chunk number and bytes written can be
     * updated from different threads.
     */
    private final Lock lock = new ReentrantLock();

    private ChunkedFileWriter(BufferedOutputStream stream, long expectedFileLength) {
        this.stream = stream;
        this.expectedFileLength = expectedFileLength;
    }

    /**
     * Opens a file with known size for writing.
     *
     * @param file File path.
     * @param expectedFileLength Expected file size.
     * @return Chunked file writer.
     * @throws FileNotFoundException If the file is not found.
     */
    static ChunkedFileWriter open(File file, long expectedFileLength) throws IOException {
        return new ChunkedFileWriter(new BufferedOutputStream(Files.newOutputStream(file.toPath())), expectedFileLength);
    }

    /**
     * Writes a chunk to the file. The chunk number must be equal to the next expected chunk number. Closes the file if the last chunk is
     * written. Returns true if the last chunk is written.
     *
     * @param chunk Chunk to write.
     * @return True if the last chunk is written. False otherwise.
     * @throws IOException If an I/O error occurs.
     * @throws FileValidationException If the chunk number or file size is invalid or expected file size is exceeded.
     */
    boolean write(FileChunkMessage chunk) throws IOException {
        lock.lock();
        try {
            if (chunk.number() != expectedNextChunkNumber) {
                throw new FileValidationException(
                        "Chunk number mismatch: expected " + expectedNextChunkNumber + ", actual " + chunk.number()
                );
            }

            stream.write(chunk.data());
            expectedNextChunkNumber++;
            bytesWritten += chunk.data().length;

            if (bytesWritten > expectedFileLength) {
                throw new FileValidationException("File size mismatch: expected " + expectedFileLength + ", actual " + bytesWritten);
            }

            if (bytesWritten == expectedFileLength) {
                close();

                return true;
            } else {
                return false;
            }
        } finally {
            lock.unlock();
        }
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
