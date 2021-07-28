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

import java.util.List;

/**
 * ResultSet metadata.
 */
public interface SqlResultSetMeta {
    /**
     * Returns number of column that every row in a ResulSet contains.
     *
     * @return Number of columns.
     */
    int columnsCount();

    /**
     * Returns metadata with description for every column in resultset.
     *
     * @return Columns metadata.
     */
    List<SqlColumnMeta> columns();

    /**
     * Returns metadata for the requested column.
     *
     * @param columnId Column Id.
     * @return Column metadata.
     */
    SqlColumnMeta column(int columnId);

    /**
     * Returns metadata for the requested column.
     *
     * @param columnName Column name.
     * @return Column metadata.
     */
    SqlColumnMeta column(String columnName);

    /**
     * Returns column id in ResultSet by column name.
     *
     * @param columnName Columns name which id is resoliving.
     * @return Column Id for requested column.
     */
    int indexOf(String columnName);
}
