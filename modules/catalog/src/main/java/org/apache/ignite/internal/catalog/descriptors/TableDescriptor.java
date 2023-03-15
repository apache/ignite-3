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

package org.apache.ignite.internal.catalog.descriptors;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Table descriptor.
 */
public class TableDescriptor extends ObjectDescriptor {
    private static final long serialVersionUID = -2021394971104316570L;

    private final int zoneId = 0;
    private final int engineId = 0;

    private final TableColumnDescriptor[] columns;
    private final String[] primaryKeyColumns;
    private final String[] colocationColumns;

    @IgniteToStringExclude
    private transient Map<String, TableColumnDescriptor> columnsMap;

    public TableDescriptor(int id, String name, TableColumnDescriptor[] columns, String[] pkCols, @Nullable String[] colocationCols) {
        super(id, Type.TABLE, name);

        this.columns = Objects.requireNonNull(columns);
        primaryKeyColumns = Objects.requireNonNull(pkCols, "No primary key columns.");
        colocationColumns = Objects.requireNonNullElse(colocationCols, primaryKeyColumns);

        this.columnsMap = Arrays.stream(columns).collect(Collectors.toMap(TableColumnDescriptor::name, Function.identity()));

        // TODO: IGNITE-18535 Throw proper exceptions.
        assert !columnsMap.isEmpty() : "No columns.";
        assert primaryKeyColumns.length > 0 : "No primary key columns.";
        assert colocationColumns.length > 0 : "No colocation columns.";

        assert Arrays.stream(primaryKeyColumns).noneMatch(c -> Objects.requireNonNull(columnsMap.get(c), c).nullable());
        //noinspection ArrayEquality
        assert primaryKeyColumns == colocationColumns || Set.of(primaryKeyColumns).containsAll(List.of(colocationColumns));
    }

    public int zoneId() {
        return zoneId;
    }

    public int engineId() {
        return engineId;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.columnsMap = Arrays.stream(columns).collect(Collectors.toMap(TableColumnDescriptor::name, Function.identity()));
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }
}
