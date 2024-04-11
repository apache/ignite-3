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

package org.apache.ignite.catalog;

/**
 * Sort order.
 */
public enum SortOrder {
    DEFAULT(""),
    ASC("asc"),
    ASC_NULLS_FIRST("asc nulls first"),
    ASC_NULLS_LAST("asc nulls last"),
    DESC("desc"),
    DESC_NULLS_FIRST("desc nulls first"),
    DESC_NULLS_LAST("desc nulls last"),
    NULLS_FIRST("nulls first"),
    NULLS_LAST("nulls last");

    private final String sql;

    SortOrder(String sql) {
        this.sql = sql;
    }

    /**
     * Returns SQL string describing this sort order.
     *
     * @return SQL string.
     */
    public String sql() {
        return sql;
    }
}
