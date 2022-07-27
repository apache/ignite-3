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

package org.apache.ignite.internal.storage;

import java.nio.ByteBuffer;

/**
 * Utility class for storages.
 */
public class StorageUtils {
    /**
     * Returns byte buffer hash that matches corresponding array hash.
     *
     * @param buf Byte buffer.
     */
    public static int hashCode(ByteBuffer buf) {
        int result = 1;
        for (int i = buf.position(); i < buf.limit(); i++) {
            result = 31 * result + buf.get(i);
        }
        return result;
    }

    /**
     * Converts to an array of bytes.
     *
     * @param buf Byte buffer.
     */
    // TODO: IGNITE-16350 Get rid of copying byte arrays.
    public static byte[] toByteArray(ByteBuffer buf) {
        byte[] arr = new byte[buf.limit()];

        buf.get(arr);

        return arr;
    }
}
