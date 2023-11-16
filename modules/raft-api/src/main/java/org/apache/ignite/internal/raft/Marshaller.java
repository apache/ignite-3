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

package org.apache.ignite.internal.raft;

import java.nio.ByteBuffer;

/**
 * Marshaller interface, for instances that convert objects to {@code byte[]} and back.
 */
public interface Marshaller {
    /**
     * Converts an object into a byte array.
     *
     * @param o Object to serialize.
     * @return Byte buffer with a serialized object.
     */
    byte[] marshall(Object o);

    /**
     * Converts byte buffer back to an object.
     *
     * @param raw Byte buffer with a serialized object.
     * @param <T> Generic type for avoiding explicit cast of the result instance.
     * @return Deserialized object.
     */
    <T> T unmarshall(ByteBuffer raw);

    /**
     * Overloaded {@link #unmarshall(ByteBuffer)}, which can be used to avoid manual {@link ByteBuffer#wrap(byte[])} calls.
     *
     * @param bytes Array with a serialized object.
     * @param <T> Generic type for avoiding explicit cast of the result instance.
     * @return Deserialized object.
     */
    default <T> T unmarshall(byte[] bytes) {
        return unmarshall(ByteBuffer.wrap(bytes));
    }
}
