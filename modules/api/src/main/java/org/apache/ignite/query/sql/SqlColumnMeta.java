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

package org.apache.ignite.query.sql;

import org.apache.ignite.schema.ColumnType;

/**
 * Column metadata.
 */
public interface SqlColumnMeta {
    /**
     * Return column name in resultset.
     *
     * Note: If row column doess not represents any table column, then generated name will be used.
     *
     * @return Column name.
     */
    String name();
    //TODO: do we expect a CanonicalName here?
    //TODO: do we want a Table name for real column? Is Calcite supports this?

    /**
     * Returns column type.
     *
     * @return Column type.
     */
    ColumnType type();
    //TODO: do ever we want to expose ColumnType (NativeType) here? Is it useful or drop this?

    /**
     * Returns column value type.
     *
     * @return Value type.
     */
    Class<?> valueClass();
    //TODO: this maybe more useful in contraty to the previous one.

    /**
     * Row column nullability.
     *
     * @return {@code true} if column is nullable, {@code false} otherwise.
     */
    boolean nullable();
    //TODO: AFAIK, Calcite can derive column type for us.
}
