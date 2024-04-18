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

package org.apache.ignite.internal.sql.engine.schema;

import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.util.ColocationUtils;
import org.apache.ignite.internal.util.HashCalculator;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/** Extract partition based on supplied row and types info. */
public class PartitionCalculator {
    private final HashCalculator hashCalculator = new HashCalculator();
    private final NativeType[] types;
    private final int partitionCount;

    private int currentField = 0;

    /** Constructor. */
    public PartitionCalculator(int partitionCount, NativeType[] types) {
        this.partitionCount = partitionCount;
        this.types = types;
    }

    /**
     * Append object partition to be calculated for.
     *
     * @param value The object for which partition will be calculated.
     */
    public void append(@Nullable Object value) {
        assert currentField < types.length;

        ColocationUtils.append(hashCalculator, value, types[currentField++]);
    }

    /**
     * Calculate partition based on appending objects.
     *
     * @return Resolved partition.
     */
    public int partition() {
        assert currentField == types.length;

        try {
            return IgniteUtils.safeAbs(hashCalculator.hash()) % partitionCount;
        } finally {
            hashCalculator.reset();
            currentField = 0;
        }
    }
}
