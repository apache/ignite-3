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

package org.apache.ignite.internal.client.proto.tx;

import java.util.EnumSet;

/**
 * Client internal transaction options.
 */
public enum ClientInternalTxOptions {
    READ_ONLY(1),
    LOW_PRIORITY(1 << 1);

    private final int mask;

    ClientInternalTxOptions(int mask) {
        this.mask = mask;
    }

    public int mask() {
        return mask;
    }

    /**
     * Unpack options.
     *
     * @param mask Packed mask.
     * @return Set of options.
     */
    public static EnumSet<ClientInternalTxOptions> unpack(int mask) {
        EnumSet<ClientInternalTxOptions> result = EnumSet.noneOf(ClientInternalTxOptions.class);
        for (ClientInternalTxOptions flag : values()) {
            if ((mask & flag.mask()) != 0) {
                result.add(flag);
            }
        }
        return result;
    }
}
