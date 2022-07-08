/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.raft.jraft.storage.impl;

import org.apache.ignite.lang.IgniteException;

/**
 * Thrown by {@link org.apache.ignite.raft.jraft.storage.LogStorage} methods to indicate that some entries (a suffix of the
 * collection of entries that was tried to be saved) were not saved due to an overflow.
 */
public class LogStorageOverflowException extends IgniteException {
    private final int overflowedEntries;

    /**
     * Constructs new instance.
     *
     * @param overflowedEntries Length of the suffix that was not saved.
     */
    public LogStorageOverflowException(int overflowedEntries) {
        super(overflowedEntries + " entries were not saved due to log storage overflow");

        this.overflowedEntries = overflowedEntries;
    }

    /**
     * Returns number of entries that were not saved in the current attempt to append entries.
     *
     * @return Number of entries that were not saved in the current attempt to append entries.
     */
    public int overflowedEntries() {
        return overflowedEntries;
    }
}
