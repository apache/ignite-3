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

package org.apache.ignite.internal.sql.engine.schema;

import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.idx.InternalSortedIndex;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.IndexScan;
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;

/**
 * Ignite scannable index.
 */
public class IgniteIndex {
    private final RelCollation collation;

    private final InternalSortedIndex idx;

    private final InternalIgniteTable tbl;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public IgniteIndex(RelCollation collation, InternalSortedIndex idx, InternalIgniteTable tbl) {
        this.collation = Objects.requireNonNull(collation);
        this.idx = Objects.requireNonNull(idx);
        this.tbl = Objects.requireNonNull(tbl);
    }

    public RelCollation collation() {
        return collation;
    }

    public String name() {
        return idx.name();
    }

    public InternalIgniteTable table() {
        return tbl;
    }

    public InternalSortedIndex index() {
        return idx;
    }

    public UUID id() {
        return idx.id();
    }

    /**
     * Scan index.
     */
    public <RowT> Iterable<RowT> scan(
            ExecutionContext<RowT> ectx,
            ColocationGroup colocationGrp,
            Predicate<RowT> filters,
            Supplier<RowT> lower,
            Supplier<RowT> upper,
            Function<RowT, RowT> rowTransformer,
            ImmutableBitSet requiredColumns
    ) {
        return new IndexScan<>(
                this,
                ectx,
                colocationGrp,
                filters,
                lower,
                upper,
                rowTransformer,
                requiredColumns
        );
    }
}
