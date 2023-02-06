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

package org.apache.ignite.tx;

/**
 * Ignite transaction options.
 */
public class TransactionOptions {
    /** Transaction timeout. */
    private long timeoutMillis = 0;

    /** Read-only transaction. */
    private boolean readOnly = false;

    /**
     * @return Transaction timeout, in milliseconds.
     */
    public long timeoutMillis() {
        return timeoutMillis;
    }

    /**
     * @param timeoutMillis Transaction timeout, in milliseconds.
     */
    public void timeoutMillis(long timeoutMillis) {
        if (readOnly && timeoutMillis != 0) {
            throw illegalStateException();
        }

        this.timeoutMillis = timeoutMillis;
    }

    /**
     * @return Whether a read-only transaction should be used.
     *
     * <p>Read-only transactions provide a snapshot view of data at a certain point in time.
     * They are lock-free and perform better than normal transactions, but do not permit data modifications.
     */
    public boolean readOnly() {
        return readOnly;
    }

    /**
     * @param readOnly Whether a read-only transaction should be used.
     *
     * <p>Read-only transactions provide a snapshot view of data at a certain point in time.
     * They are lock-free and perform better than normal transactions, but do not permit data modifications.
     */
    public void readOnly(boolean readOnly) {
        if (readOnly && timeoutMillis != 0) {
            throw illegalStateException();
        }

        this.readOnly = readOnly;
    }

    private static IllegalStateException illegalStateException() {
        return new IllegalStateException("Read-only transactions cannot have a timeout");
    }
}
