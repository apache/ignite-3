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

package org.apache.ignite.internal.storage.pagememory;

import java.nio.ByteBuffer;

/**
 * Helper class for reading an array of bytes in fragments.
 *
 * <p>Structure: array length(int) + byte array(array length).
 */
class FragmentedByteArray {
    private int arrLen = -1;

    private byte[] arr = null;

    private int off;

    /**
     * Reads data from the buffer.
     *
     * @param buf Byte buffer from which to read.
     */
    void readData(ByteBuffer buf) {
        if (buf.remaining() == 0) {
            return;
        }

        if (arrLen == -1) {
            if (buf.remaining() >= 4) {
                arrLen = buf.getInt();
            } else {
                if (arr == null) {
                    arr = new byte[4];
                }

                int len = Math.min(buf.remaining(), 4 - off);

                buf.get(arr, off, len);
                off += len;

                if (off == 4) {
                    ByteBuffer tmpBuf = ByteBuffer.wrap(arr);

                    tmpBuf.order(buf.order());

                    arrLen = tmpBuf.getInt();
                    arr = null;
                    off = 0;
                }
            }
        }

        if (arrLen != -1) {
            if (arr == null) {
                arr = new byte[arrLen];
            }

            int len = Math.min(buf.remaining(), arrLen - off);

            buf.get(arr, off, len);
            off += len;
        }
    }

    /**
     * Returns true if the array has been read completely.
     */
    boolean ready() {
        return arrLen != -1 && off == arrLen;
    }

    /**
     * Returns byte array.
     */
    byte[] array() {
        return arr;
    }
}
