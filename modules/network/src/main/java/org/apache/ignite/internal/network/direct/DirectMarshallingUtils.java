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

package org.apache.ignite.internal.network.direct;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.network.direct.stream.DirectByteBufferStream;

/**
 * Direct marshalling utils.
 */
public class DirectMarshallingUtils {
    /**
     * Reads a {@code short} from a byte buffer in an order defined by the {@link DirectByteBufferStream} implementation.
     *
     * @param buffer Byte buffer.
     * @return Direct message type.
     */
    public static short getShort(ByteBuffer buffer) {
        int primShift = 0;
        int prim = 0;

        short val = 0;

        while (buffer.hasRemaining()) {
            byte b = buffer.get();

            prim |= ((int) b & 0x7F) << (7 * primShift);

            if ((b & 0x80) == 0) {
                val = (short) prim;

                if (val == Short.MIN_VALUE) {
                    val = Short.MAX_VALUE;
                } else {
                    val--;
                }

                return val;
            } else {
                primShift++;
            }
        }

        throw new RuntimeException();
    }
}
