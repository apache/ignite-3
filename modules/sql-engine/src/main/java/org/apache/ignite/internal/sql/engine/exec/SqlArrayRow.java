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

import static org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactoryImpl.UNSPECIFIED_VALUE_PLACEHOLDER;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;

/**
 * TODO Sql array row.
 */
public class SqlArrayRow implements SqlRowWrapper {
    private final Object[] row;
    private final RowSchema rowSchema;

    SqlArrayRow(Object[] row, RowSchema rowSchema) {
        this.row = row;
        this.rowSchema = rowSchema;
    }

    @Override
    public int columnsCount() {
        return row.length;
    }

    @Override
    public Object get(int i) {
        return row[i];
    }

    @Override
    public void set(int i, Object v) {
        row[i] = v;
    }

    @Override
    public ByteBuffer toByteBuffer() {
        BinaryTupleBuilder tupleBuilder = new BinaryTupleBuilder(row.length);

        for (int i = 0; i < row.length; i++) {
            Object val = row[i];

            // TODO
            if (val == UNSPECIFIED_VALUE_PLACEHOLDER) {
                break; // No more columns in prefix.
            }

            SqlRowSchemaConverterUtils.appendRow(tupleBuilder, rowSchema, i, val);
        }

        return tupleBuilder.build();
    }
}
