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

/** Resolves assignments according to incoming data. */
public class AssignmentResolverImpl<RowT> implements AssignmentsResolver<RowT> {
    private final int partitions;
    private final int colocationKeysSize;

    private int hash;
    private boolean calculated;
    private int curColIdx;

    public AssignmentResolverImpl(int partitions, int colocationKeysSize) {
        this.partitions = partitions;
        this.colocationKeysSize = colocationKeysSize;
    }

    /** {@inheritDoc} */
    @Override
    public void append(@Nullable Object value) {
        if (calculated) {
            reset();
        }
        assert curColIdx < colocationKeysSize : "extra keys supplied";
        curColIdx++;
        hash = 31 * hash + Objects.hashCode(value);
    }

    /** {@inheritDoc} */
    @Override
    public int getPartition() {
        complete();
        return IgniteUtils.safeAbs(hash % partitions);
    }

    private void complete() {
        assert curColIdx == colocationKeysSize :
                format("partially initialized: keys supplied={}, keys expected={}", curColIdx, colocationKeysSize);
        calculated = true;
    }

    private void reset() {
        hash = 0;
        curColIdx = 0;
        calculated = false;
    }
}
