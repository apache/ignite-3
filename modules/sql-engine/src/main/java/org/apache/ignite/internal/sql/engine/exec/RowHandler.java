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

import org.apache.ignite.internal.lang.InternalTuple;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.jetbrains.annotations.Nullable;

/**
 * Universal accessor for rows. It also has factory methods.
 */
public interface RowHandler<RowT> {
    /**
     * Extract appropriate field.
     *
     * @param field Field position to be processed.
     * @param row Object to be extracted from.
     */
    @Nullable Object get(int field, RowT row);

    /** Concatenate two rows. */
    RowT concat(RowT left, RowT right);

    /**
     * Creates a new row containing only the fields specified in the provided mapping.
     *
     * <p>For example:
     * <pre>
     *    source row [5, 6, 7, 8] apply mapping [1, 3]
     *    result row will be [6, 8]
     * </pre>
     *
     * @param row Source row.
     * @param mapping Target field indexes.
     * @return A new row with fields from the specified mapping.
     */
    RowT map(RowT row, int[] mapping);

    /** Return column count contained in the incoming row. */
    int columnCount(RowT row);

    /**
     * Assembly row representation as BinaryTuple.
     *
     * @param row Incoming data to be processed.
     * @return {@link BinaryTuple} representation.
     */
    BinaryTuple toBinaryTuple(RowT row);

    /** String representation. */
    String toString(RowT row);

    /** Creates a factory that produces rows with fields defined by the given schema. */
    RowFactory<RowT> factory(RowSchema rowSchema);

    /**
     * Provide methods for inner row assembly.
     */
    @SuppressWarnings("PublicInnerClass")
    interface RowFactory<RowT> {
        /** Return row accessor and mutator implementation. */
        RowHandler<RowT> handler();

        /** Creates a {@link RowBuilder row builder}. */
        RowBuilder<RowT> rowBuilder();

        /** Create empty row. */
        RowT create();

        /**
         * Create row using incoming objects.
         *
         * @param fields Sequential objects definitions output row will be created from.
         * @return Instantiation defined representation.
         */
        RowT create(Object... fields);

        /**
         * Create row using incoming binary tuple.
         *
         * @param tuple {@link InternalTuple} representation.
         * @return Instantiation defined representation.
         */
        RowT create(InternalTuple tuple);
    }

    /**
     * A builder to create rows. It uses the schema provided by an instance of row factory that created it.
     *
     * <pre>
     *     // Create a row builder.
     *     var rowBuilder = rowFactory.rowBuilder();
     *     ...
     *     // Call build() after all fields have been set.
     *     var row1 = rowBuilder.build();
     *     // Call reset() to cleanup builder's state.
     *     rowBuilder.reset();
     * </pre>
     */
    interface RowBuilder<RowT> {

        /**
         * Adds a field to the current row.
         *
         * @param value Field value.
         * @return this.
         */
        RowBuilder<RowT> addField(@Nullable Object value);

        /** Creates a new row from a previously added fields. */
        RowT build();

        /**
         * Resets the state of this builder.
         */
        void reset();

        /**
         * Creates a new row and resets the state of this builder. This is a shorthand for:
         * <pre>
         *     Row row = builder.build();
         *     builder.reset();
         *     return row;
         * </pre>
         */
        default RowT buildAndReset() {
            RowT row = build();
            reset();
            return row;
        }
    }
}
