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

package org.apache.ignite.internal.table;

import java.util.function.Supplier;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.table.QualifiedName;
import org.jetbrains.annotations.Nullable;

/**
 * Caches a {@link TupleMarshaller}.
 */
class TupleMarshallerCache {
    private final SchemaRegistry schemaRegistry;

    private volatile @Nullable TupleMarshaller cachedMarshaller;

    TupleMarshallerCache(SchemaRegistry schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    /**
     * Returns a {@link TupleMarshaller} for the given schema version.
     *
     * @param tableNameSupplier Supplier of table name.
     * @param schemaVersion Version for which to return a marshaller.
     */
    TupleMarshaller marshaller(Supplier<QualifiedName> tableNameSupplier, int schemaVersion) {
        TupleMarshaller marshaller = cachedMarshaller;

        if (marshaller != null && marshaller.schemaVersion() == schemaVersion) {
            return marshaller;
        }

        marshaller = new TupleMarshallerImpl(tableNameSupplier, schemaRegistry.schema(schemaVersion));

        cachedMarshaller = marshaller;

        return marshaller;
    }
}
