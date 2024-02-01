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

package org.apache.ignite.internal.sql.engine.exec.mapping;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.Objects;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/** Resolves assignment according to incoming data. */
public class AssignmentExtractorImpl {
    private final int partitions;
    private final int keys;

    private int hash;
    private boolean calculated;
    private int processedIdx;

    public AssignmentExtractorImpl(int partitions, int keys) {
        this.partitions = partitions;
        this.keys = keys;
    }

    /**
     * Append object assignment to be calculated for.
     *
     * @param value The object for which assignment will be calculated.
     */
    public void append(@Nullable Object value) {
        if (calculated) {
            reset();
        }
        assert processedIdx < keys : "extra keys supplied";
        processedIdx++;
        hash = 31 * hash + Objects.hashCode(value);
    }

    /**
     * Calculate assignment based on appending objects.
     *
     * @return Resolved assignment.
     */
    public int getAssignment() {
        complete();
        return IgniteUtils.safeAbs(hash % partitions);
    }

    private void complete() {
        assert processedIdx == keys :
                format("partially initialized: keys supplied={}, keys expected={}", processedIdx, keys);
        calculated = true;
    }

    private void reset() {
        hash = 0;
        processedIdx = 0;
        calculated = false;
    }
}
