/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.schema.marshaller.schema;

/**
 *  Extended byte buffer interface.
 */
public interface ExtendedByteBuffer {
    /**
     * Appends byte value to buffer.
     *
     * @param val Byte value.
     */
    void put(byte val);

    /**
     * Appends byte array to buffer.
     *
     * @param values Byte array.
     */
    void put(byte[] values);

    /**
     * Appends short value to buffer.
     *
     * @param val Short value.
     */
    void putShort(short val);

    /**
     * Appends int value to buffer.
     *
     * @param val Integer value.
     */
    void putInt(int val);

    /**
     * Appends string value to buffer.
     *
     * @param val String value.
     */
    void putString(String val);

    /**
     * Reads the byte at this buffer's current position.
     *
     * @return The byte value.
     */
    byte get();

    /**
     * Reads the short at this buffer's current position.
     *
     * @return The short value.
     */
    short getShort();

    /**
     * Reads the int at this buffer's current position.
     *
     * @return The int value.
     */
    int getInt();

    /**
     * Reads the string header and string byte representation at this buffer's current position.
     *
     * @return The string value.
     */
    String getString();

    /**
     * Returns the byte array that backs this buffer.
     *
     * @return The byte array.
     */
    byte[] array();

    /**
     * Returns the size of byte array that backs this buffer.
     *
     * @return The byte array size.
     */
    int size();
}
