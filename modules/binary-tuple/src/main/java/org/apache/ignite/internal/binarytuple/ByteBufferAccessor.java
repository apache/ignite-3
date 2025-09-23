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

package org.apache.ignite.internal.binarytuple;

/**
 * An interface for accessing a byte buffer with methods to read various data types
 * at specific positions. This interface abstracts the underlying implementation
 * for handling data in either direct or heap-based byte buffers.
 */
public interface ByteBufferAccessor {
    /**
     * Retrieves the byte value from the underlying byte buffer at the specified index.
     *
     * @param p the index in the underlying byte buffer to retrieve the byte from.
     * @return the byte value located at the specified index in the byte buffer.
     */
    byte get(int p);

    /**
     * Reads a 32-bit integer value from the underlying byte buffer at the specified index.
     *
     * @param p the index in the underlying byte buffer to start reading the 32-bit integer value from.
     * @return the 32-bit integer value located at the specified index in the byte buffer.
     */
    int getInt(int p);

    /**
     * Reads a 64-bit long value from the underlying byte buffer at the specified index.
     *
     * @param p the index in the underlying byte buffer to start reading the 64-bit long value from.
     * @return the 64-bit long value located at the specified index in the byte buffer.
     */
    long getLong(int p);

    /**
     * Reads a 16-bit short value from the underlying byte buffer at the specified index.
     *
     * @param p the index in the underlying byte buffer to start reading the 16-bit short value from.
     * @return the 16-bit short value located at the specified index in the byte buffer.
     */
    short getShort(int p);

    /**
     * Reads a 32-bit floating-point value from the underlying byte buffer at the specified index.
     *
     * @param p the index in the underlying byte buffer to start reading the 32-bit floating-point value from.
     * @return the 32-bit floating-point value located at the specified index in the byte buffer.
     */
    float getFloat(int p);

    /**
     * Reads a 64-bit double-precision floating-point value from the underlying
     * byte buffer at the specified index.
     *
     * @param p the index in the underlying byte buffer to start reading the
     *          64-bit double-precision floating-point value from.
     * @return the 64-bit double-precision floating-point value located at
     *         the specified index in the byte buffer.
     */
    double getDouble(int p);

    /**
     * Returns the capacity of the underlying byte buffer, representing the total number of bytes it can hold.
     *
     * @return the total capacity of the byte buffer.
     */
    int capacity();
}
