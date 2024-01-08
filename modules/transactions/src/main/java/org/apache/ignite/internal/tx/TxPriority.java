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

package org.apache.ignite.internal.tx;

import org.apache.ignite.internal.tx.impl.WaitDieDeadlockPreventionPolicy;

/**
 * Transaction priority. The priority is used in {@link WaitDieDeadlockPreventionPolicy} to determine if a waiter should wait or abort.
 */
public enum TxPriority {
    LOW((byte) 0),
    NORMAL((byte) 1);
    private final byte byteValue;

    TxPriority(byte byteValue) {
        this.byteValue = byteValue;
    }

    public byte byteValue() {
        return byteValue;
    }

    /**
     * Returns the priority from the byte value.
     *
     * @param byteValue the byte value.
     * @return the priority.
     */
    static TxPriority fromPriority(byte byteValue) {
        switch (byteValue) {
            case 0:
                return LOW;
            case 1:
                return NORMAL;
            default:
                throw new IllegalArgumentException("Unknown priority: " + byteValue);
        }
    }
}
