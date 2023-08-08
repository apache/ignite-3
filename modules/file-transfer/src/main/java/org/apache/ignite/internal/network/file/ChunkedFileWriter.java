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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.PriorityQueue;
import java.util.Queue;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.network.file.messages.FileChunkMessage;

/**
 * Chunked file writer. Writes chunks in order. If a chunk is not in order, it is stored in a queue. When the next chunk is written, the
 * queues are checked for the next chunk. If the next chunk is found, it is written to the file and removed from the queue. If the next
 * chunk is not found, the file is not written to. The writer is not thread-safe
 */
class ChunkedFileWriter implements ManuallyCloseable {
    private final RandomAccessFile raf;

    private long fileSize;

    private final Queue<FileChunkMessage> chunks = new PriorityQueue<>(FileChunkMessage.COMPARATOR);

    private ChunkedFileWriter(RandomAccessFile raf, long fileSize) {
        this.raf = raf;
        this.fileSize = fileSize;
    }

    /**
     * Opens a file for writing.
     *
     * @param path File path.
     * @param fileSize File size.
     * @return Chunked file writer.
     * @throws FileNotFoundException If the file is not found.
     */
    static ChunkedFileWriter open(Path path, long fileSize) throws FileNotFoundException {
        return new ChunkedFileWriter(new RandomAccessFile(path.toFile(), "rw"), fileSize);
    }

    /**
     * Writes a chunk to the file.
     *
     * @param chunk Chunk.
     * @throws IOException If an I/O error occurs.
     */
    void write(FileChunkMessage chunk) throws IOException {
        chunks.add(chunk);
        while (!chunks.isEmpty() && chunks.peek().offset() == raf.getFilePointer()) {
            raf.write(chunks.poll().data());
        }

        if (fileSize > -1 && raf.getFilePointer() > fileSize) {
            throw new IOException("File size exceeded");
        }
    }

    /**
     * Checks if the file is finished.
     *
     * @return {@code True} if the file is finished.
     * @throws IOException If an I/O error occurs.
     */
    boolean isFinished() throws IOException {
        return raf.length() == fileSize;
    }

    /**
     * Sets the file size.
     *
     * @param fileSize File size.
     */
    void fileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    /**
     * Closes the file.
     *
     * @throws IOException If an I/O error occurs.
     */
    @Override
    public void close() throws IOException {
        raf.close();
    }
}
