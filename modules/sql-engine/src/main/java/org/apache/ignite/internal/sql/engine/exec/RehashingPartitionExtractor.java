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

package org.apache.ignite.internal.sql.engine.exec;

import static org.apache.ignite.internal.util.ArrayUtils.nullOrEmpty;

import org.apache.ignite.internal.util.IgniteUtils;

/** Extract assignment based on incoming row. */
public class RehashingPartitionExtractor<RowT> implements RowPartitionExtractor<RowT> {
    private final int targetCount;
    private final int[] fields;
    private final RowHandler<RowT> rowHandler;

    /** Constructor. */
    public RehashingPartitionExtractor(
            int targetCount,
            int[] fields,
            RowHandler<RowT> rowHandler
    ) {
        assert !nullOrEmpty(fields);

        this.targetCount = targetCount;
        this.fields = fields;
        this.rowHandler = rowHandler;
    }

    /** {@inheritDoc} */
    @Override
    public int partition(RowT row) {
        int hash = 0;
        for (int columnId : fields) {
            Object value = rowHandler.get(columnId, row);

            hash = 31 * hash + (value == null ? 0 : value.hashCode());
        }

        return IgniteUtils.safeAbs(hash) % targetCount;
    }
}
