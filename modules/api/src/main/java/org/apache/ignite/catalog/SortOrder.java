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
    ASC("ASC"),
    ASC_NULLS_FIRST("ASC NULLS FIRST"),
    ASC_NULLS_LAST("ASC NULLS LAST"),
    DESC("DESC"),
    DESC_NULLS_FIRST("DESC NULLS FIRST"),
    DESC_NULLS_LAST("DESC NULLS LAST"),
    NULLS_FIRST("NULLS FIRST"),
    NULLS_LAST("NULLS LAST");

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
