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

package org.apache.ignite.internal.raft.storage.segstore;

import org.apache.ignite.internal.tostring.S;

/**
 * Represents properties common for some file types (namely Segment and Index files) used by the log storage.
 */
class FileProperties {
    /** File ordinal. Incremented each time a new file is created. */
    private final int ordinal;

    /** File generation. Incremented each time an existing file is compressed by the Raft Log Garbage Collector. */
    private final int generation;

    FileProperties(int ordinal) {
        this(ordinal, 0);
    }

    FileProperties(int ordinal, int generation) {
        if (ordinal < 0) {
            throw new IllegalArgumentException("Invalid file ordinal: " + ordinal);
        }

        if (generation < 0) {
            throw new IllegalArgumentException("Invalid file generation: " + generation);
        }

        this.ordinal = ordinal;
        this.generation = generation;
    }

    int ordinal() {
        return ordinal;
    }

    int generation() {
        return generation;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FileProperties that = (FileProperties) o;
        return ordinal == that.ordinal && generation == that.generation;
    }

    @Override
    public int hashCode() {
        int result = ordinal;
        result = 31 * result + generation;
        return result;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
