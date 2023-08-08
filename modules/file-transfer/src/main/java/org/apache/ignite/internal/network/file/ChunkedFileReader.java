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
import java.io.RandomAccessFile;
import org.apache.ignite.internal.close.ManuallyCloseable;

/**
 * Chunked file reader. Reads the file in chunks. Each chunk has a fixed size. The last chunk may be smaller than the chunk size. If the
 * file size is less than the chunk size, only one chunk will be read. The reader is not thread-safe.
 */
class ChunkedFileReader implements ManuallyCloseable {
    private final String fileName;
    private final RandomAccessFile raf;
    private final int chunkSize;

    /**
     * Constructor.
     *
     * @param fileName File name.
     * @param raf Random access file.
     * @param chunkSize Chunk size.
     */
    private ChunkedFileReader(String fileName, RandomAccessFile raf, int chunkSize) {
        this.fileName = fileName;
        this.raf = raf;
        this.chunkSize = chunkSize;
    }

    /**
     * Opens a file for reading.
     *
     * @param file File.
     * @param chunkSize Chunk size.
     * @return Chunked file reader.
     * @throws IOException If an I/O error occurs.
     */
    static ChunkedFileReader open(File file, int chunkSize) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        return new ChunkedFileReader(file.getName(), raf, chunkSize);
    }

    /**
     * Reads the next chunk.
     *
     * @return Chunk data.
     * @throws IOException If an I/O error occurs.
     */
    byte[] readNextChunk() throws IOException {
        int toRead = (int) Math.min(chunkSize, length() - offset());
        byte[] data = new byte[toRead];
        raf.read(data);
        return data;
    }

    /**
     * Returns the file name.
     *
     * @return File name.
     */
    String fileName() {
        return fileName;
    }

    /**
     * Returns the current chunk offset.
     *
     * @return Chunk size.
     */
    long offset() throws IOException {
        return raf.getFilePointer();
    }

    /**
     * Returns the file length.
     *
     * @return File length.
     */
    long length() throws IOException {
        return raf.length();
    }

    /**
     * Returns {@code true} if the file has been read completely.
     *
     * @return {@code true} if the file has been read completely.
     */
    boolean isFinished() throws IOException {
        return raf.getFilePointer() == raf.length();
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
