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

package org.apache.ignite.internal.tx;

import org.jetbrains.annotations.NotNull;

/**
 * The timestamp.
 */
public class Timestamp implements Comparable<Timestamp> {
    /** */
    private final long timestamp;

    /**
     * @param timestamp The timestamp.
     */
    Timestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * @param other Other version.
     * @return Comparison result.
     */
    @Override public int compareTo(@NotNull Timestamp other) {
        return Long.compare(timestamp, other.timestamp);
    }

    @Override public boolean equals(Object o) {
        if (!(o instanceof Timestamp)) return false;

        return compareTo((Timestamp) o) == 0;
    }

    @Override public int hashCode() {
        return (int) (timestamp ^ (timestamp >>> 32));
    }

    /**
     * @return Next timestamp (monotonically increasing)
     */
    public static Timestamp nextVersion() {
        return new Timestamp(System.nanoTime());
    }
}
