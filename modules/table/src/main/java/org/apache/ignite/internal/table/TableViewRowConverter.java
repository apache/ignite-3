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

import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.registry.SchemaRegistryException;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.lang.MarshallerException;

/**
 * Converts {@link BinaryRow binary rows} to {@link Row rows} using {@link SchemaRegistry}.
 */
final class TableViewRowConverter {

    private final SchemaRegistry schemaReg;

    TableViewRowConverter(SchemaRegistry schemaReg) {
        this.schemaReg = schemaReg;
    }

    SchemaRegistry registry() {
        return schemaReg;
    }

    Row resolveRow(BinaryRow binaryRow, int targetSchemaVersion) throws MarshallerException {
        try {
            return schemaReg.resolve(binaryRow, targetSchemaVersion);
        } catch (SchemaRegistryException e) {
            throw new MarshallerException("Failed find a serialization schema for the binary row.", e);
        }
    }

    List<Row> resolveKeys(Collection<BinaryRow> rows, int targetSchemaVersion) throws MarshallerException {
        try {
            return schemaReg.resolveKeys(rows, targetSchemaVersion);
        } catch (SchemaRegistryException e) {
            throw new MarshallerException("Failed find a serialization schema for the binary row.", e);
        }
    }

    List<Row> resolveRows(Collection<BinaryRow> rows, int targetSchemaVersion) throws MarshallerException {
        try {
            return schemaReg.resolve(rows, targetSchemaVersion);
        } catch (SchemaRegistryException e) {
            throw new MarshallerException("Failed find a serialization schema for the binary row.", e);
        }
    }
}
