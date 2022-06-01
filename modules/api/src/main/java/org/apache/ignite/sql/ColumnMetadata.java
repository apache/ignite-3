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

package org.apache.ignite.sql;

/**
 * Interface that provides methods for accessing column metadata.
 */
public interface ColumnMetadata {
    /**
     * Return column name in the result set.
     *
     * <p>Note: If row column does not represent any table column, then generated name will be
     * used.
     *
     * @return Column name.
     */
    String name();

    /**
     * Returns a class of column values.
     *
     * @return Value class.
     */
    Class<?> valueClass();

    /**
     * Returns SQL column type.
     * TODO: IGNITE-16962 replace return type regarding the SQL type system.
     *
     * @return Value type.
     */
    Object type();

    /**
     * Returns row column nullability flag.
     *
     * @return {@code true} if column is nullable, {@code false} otherwise.
     */
    boolean nullable();
}
