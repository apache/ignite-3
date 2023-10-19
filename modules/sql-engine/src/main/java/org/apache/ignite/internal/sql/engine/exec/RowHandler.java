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

import java.nio.ByteBuffer;
import org.apache.ignite.internal.schema.row.InternalTuple;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.jetbrains.annotations.Nullable;

/**
 * Universal accessor and mutator for rows. It also has factory methods.
 */
public interface RowHandler<RowT> {
    /**
     * Extract appropriate field.
     *
     * @param field Field position to be processed.
     * @param row Object to be extracted from.
     */
    @Nullable Object get(int field, RowT row);

    /** Set incoming row field.
     *
     * @param field Field position to be processed.
     * @param row Row which field need to be changed.
     * @param val Value which should be set.
     */
    void set(int field, RowT row, @Nullable Object val);

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
     * Assembly row representation as ByteBuffer.
     *
     * @param row Incoming data to be processed.
     * @return {@link ByteBuffer} representation.
     */
    ByteBuffer toByteBuffer(RowT row);

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
         * Create row using incoming {@link ByteBuffer}.
         *
         * @param raw {@link ByteBuffer} representation.
         * @return Instantiation defined representation.
         */
        RowT create(ByteBuffer raw);

        /**
         * Create row using incoming binary tuple.
         *
         * @param tuple {@link InternalTuple} representation.
         * @return Instantiation defined representation.
         */
        RowT create(InternalTuple tuple);
    }
}
