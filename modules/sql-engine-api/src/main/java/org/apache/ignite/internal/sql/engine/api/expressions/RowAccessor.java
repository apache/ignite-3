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

package org.apache.ignite.internal.sql.engine.api.expressions;

import org.jetbrains.annotations.Nullable;

/**
 * Class representing a reader for the input row.
 *
 * @param <RowT> Type of the supported row.
 */
public interface RowAccessor<RowT> {
    /** Reads the specified field of the given row. */
    @Nullable Object get(int field, RowT row);

    /** Checks whether the given field is {@code null}. */
    boolean isNull(int field, RowT row);

    /** Return columns count contained in the incoming row. */
    int columnsCount(RowT row);
}
