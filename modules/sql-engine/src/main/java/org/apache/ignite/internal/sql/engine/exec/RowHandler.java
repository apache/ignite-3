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
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.jetbrains.annotations.Nullable;

/**
 * Universal accessor and mutator for rows. It also has factory methods.
 */
public interface RowHandler<RowT> {
    @Nullable Object get(int field, RowT row);

    void set(int field, RowT row, @Nullable Object val);

    RowT concat(RowT left, RowT right);

    int columnCount(RowT row);

    ByteBuffer toByteBuffer(RowT row);

    String toString(RowT row);

    /** Creates a factory that produces rows with fields defined by the given schema. */
    RowFactory<RowT> factory(RowSchema rowSchema);

    /**
     * RowFactory interface.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @SuppressWarnings("PublicInnerClass")
    interface RowFactory<RowT> {
        RowHandler<RowT> handler();

        RowT create();

        RowT create(Object... fields);

        RowT create(ByteBuffer raw);
    }
}
