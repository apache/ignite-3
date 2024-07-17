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

package org.apache.ignite.internal.schema.row;

import java.math.BigDecimal;
import org.apache.ignite.internal.binarytuple.BinaryTupleContainer;
import org.apache.ignite.internal.lang.InternalTuple;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.schema.SchemaDescriptor;

/**
 * Schema-aware row interface.
 */
public interface Row extends SchemaAware, BinaryRowEx, InternalTuple, BinaryTupleContainer {
    /**
     * Creates a row from a given {@code BinaryRow}.
     *
     * @param schema Schema.
     * @param binaryRow Binary row.
     */
    static Row wrapBinaryRow(SchemaDescriptor schema, BinaryRow binaryRow) {
        return new RowImpl(false, schema, BinaryTupleSchema.createRowSchema(schema), binaryRow);
    }

    /**
     * Creates a row from a given {@code BinaryRow} that only contains the key component.
     *
     * @param schema Schema.
     * @param binaryRow Binary row.
     */
    static Row wrapKeyOnlyBinaryRow(SchemaDescriptor schema, BinaryRow binaryRow) {
        return new RowImpl(true, schema, BinaryTupleSchema.createKeySchema(schema), binaryRow);
    }

    /** Short-cut method that reads decimal value with a scale from the schema. */
    BigDecimal decimalValue(int col);

    /**
     * Reads value for specified column.
     *
     * @param colIdx Column index.
     * @return Column value.
     */
    Object value(int colIdx);

    /**
     * Gets a value indicating whether the row contains only key columns.
     *
     * @return {@code true} if the row contains only key columns.
     */
    boolean keyOnly();
}
