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

package org.apache.ignite.internal.fileio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * Interface to perform file I/O operations.
 */
public interface FileIo extends AutoCloseable {
    /**
     * Returns current file position.
     *
     * @return Current file position, a non-negative integer counting the number of bytes from the beginning of the file to the current
     *      position.
     * @throws IOException If some I/O error occurs.
     */
    long position() throws IOException;

    /**
     * Sets new current file position.
     *
     * @param newPosition The new position, a non-negative integer counting the number of bytes from the beginning of the file.
     * @throws IOException If some I/O error occurs.
     */
    void position(long newPosition) throws IOException;

    /**
     * Reads a sequence of bytes from this file into the {@code destBuf}.
     *
     * @param destBuf Destination byte buffer.
     * @return Number of read bytes, or {@code -1} if the given position is greater than or equal to the file's current size.
     * @throws IOException If some I/O error occurs.
     */
    int read(ByteBuffer destBuf) throws IOException;

    /**
     * Reads a sequence of bytes from this file into the {@code destBuf} starting from specified file {@code position}.
     *
     * @param destBuf Destination byte buffer.
     * @param position Starting position of file.
     * @return Number of read bytes, possibly zero, or {@code -1} if the given position is greater than or equal to the file's current size.
     * @throws IOException If some I/O error occurs.
     */
    int read(ByteBuffer destBuf, long position) throws IOException;

    /**
     * Reads an up to {@code len} bytes from this file into the {@code buf}.
     *
     * @param buf Destination byte array.
     * @param off The start offset in array {@code buf} at which the data is written.
     * @param len Maximum number of bytes read.
     * @return Number of read bytes.
     * @throws IOException If some I/O error occurs.
     */
    int read(byte[] buf, int off, int len) throws IOException;

    /**
     * Reads a sequence of bytes from this file into the {@code destBuf}.
     *
     * <p>Tries to read either until the {@code destBuf} is full or until the end of the file.
     *
     * @param destBuf Destination byte buffer.
     * @return Number of read bytes.
     * @throws IOException If some I/O error occurs.
     */
    int readFully(ByteBuffer destBuf) throws IOException;

    /**
     * Reads a sequence of bytes from this file into the {@code destBuf} starting from specified file {@code position}.
     *
     * <p>Tries to read either until the {@code destBuf} is full or until the end of the file.
     *
     * @param destBuf Destination byte buffer.
     * @param position Starting position of file.
     * @return Number of read bytes.
     * @throws IOException If some I/O error occurs.
     */
    int readFully(ByteBuffer destBuf, long position) throws IOException;

    /**
     * Reads an up to {@code len} bytes from this file into the {@code buf}.
     *
     * <p>Tries to read either until the {@code len} or until the end of the file.
     *
     * @param buf Destination byte array.
     * @param off The start offset in array {@code buff} at which the data is written.
     * @param len Number of bytes read.
     * @return Number of read bytes.
     * @throws IOException If some I/O error occurs.
     */
    int readFully(byte[] buf, int off, int len) throws IOException;

    /**
     * Writes a sequence of bytes to this file from the {@code srcBuf}.
     *
     * @param srcBuf Source buffer.
     * @return Number of written bytes.
     * @throws IOException If some I/O error occurs.
     */
    int write(ByteBuffer srcBuf) throws IOException;

    /**
     * Writes a sequence of bytes to this file from the {@code srcBuf} starting from specified file {@code position}.
     *
     * @param srcBuf Source buffer.
     * @param position Starting file position.
     * @return Number of written bytes.
     * @throws IOException If some I/O error occurs.
     */
    int write(ByteBuffer srcBuf, long position) throws IOException;

    /**
     * Writes {@code len} bytes from the {@code buf} starting at offset {@code off} to this file.
     *
     * @param buf Source byte array.
     * @param off Start offset in the {@code buf}.
     * @param len Number of bytes to write.
     * @return Number of written bytes.
     * @throws IOException If some I/O error occurs.
     */
    int write(byte[] buf, int off, int len) throws IOException;

    /**
     * Writes a sequence of bytes to this file from the {@code srcBuf}.
     *
     * <p>Tries to write either the entire {@code srcBuf} or to the end of the file.
     *
     * @param srcBuf Source buffer.
     * @return Number of written bytes.
     * @throws IOException If some I/O error occurs.
     */
    int writeFully(ByteBuffer srcBuf) throws IOException;

    /**
     * Writes a sequence of bytes to this file from the {@code srcBuf} starting from specified file {@code position}.
     *
     * <p>Tries to write either the entire {@code srcBuf} or to the end of the file.
     *
     * @param srcBuf Source buffer.
     * @param position Starting file position.
     * @return Number of written bytes.
     * @throws IOException If some I/O error occurs.
     */
    int writeFully(ByteBuffer srcBuf, long position) throws IOException;

    /**
     * Writes {@code len} bytes from the {@code buf} starting at offset {@code off} to this file.
     *
     * <p>Tries to write either to the {@code len} or to the end of the file.
     *
     * @param buf Source byte array.
     * @param off Start offset in the {@code buffer}.
     * @param len Number of bytes to write.
     * @return Number of written bytes.
     * @throws IOException If some I/O error occurs.
     */
    int writeFully(byte[] buf, int off, int len) throws IOException;

    /**
     * Allocates memory mapped buffer for this file with given size.
     *
     * @param sizeBytes Size of buffer.
     * @return Instance of mapped byte buffer.
     * @throws IOException If some I/O error occurs.
     */
    MappedByteBuffer map(int sizeBytes) throws IOException;

    /**
     * Forces any updates of this file to be written to the storage device that contains it.
     *
     * @throws IOException If some I/O error occurs.
     */
    void force() throws IOException;

    /**
     * Forces any updates of this file to be written to the storage device that contains it.
     *
     * @param withMetadata If {@code true} force also file metadata.
     * @throws IOException If some I/O error occurs.
     */
    void force(boolean withMetadata) throws IOException;

    /**
     * Returns current file size in bytes.
     *
     * @throws IOException If some I/O error occurs.
     */
    long size() throws IOException;

    /**
     * Truncates current file to zero length and resets current file position to zero.
     *
     * @throws IOException If some I/O error occurs.
     */
    void clear() throws IOException;

    /**
     * Closes current file.
     *
     * @throws IOException If some I/O error occurs.
     */
    @Override
    void close() throws IOException;
}
