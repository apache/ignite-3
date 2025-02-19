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

package org.apache.ignite.client.handler;

import java.util.UUID;

/**
 * Generates client query IDs.
 * TODO more details
 */
public class ClientQueryIdGenerator {
    private final long mostSigBits;
    private final long connectionId;

    ClientQueryIdGenerator(String nodeName, long connectionId) {
        this.mostSigBits = 1L << 32 | nodeName.hashCode();
        this.connectionId = toIntWithOverflow(connectionId);
    }

    public UUID generate(int requestNumber) {
        return new UUID(mostSigBits, (long) requestNumber << 32 | connectionId);
    }

    private static long toIntWithOverflow(long value) {
        assert value >= 0;

        if (value > Integer.MAX_VALUE) {
            return value % Integer.MAX_VALUE;
        }

        return value;
    }
}
