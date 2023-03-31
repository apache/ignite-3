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

package org.apache.ignite.sql;

import java.util.List;

/**
 * ResultSet metadata.
 */
public interface ResultSetMetadata {
    /**
     * Returns metadata with a description for every column in the result set.
     *
     * @return Column metadata.
     */
    List<ColumnMetadata> columns();

    /**
     * Returns a column index for the column with a given name.
     *
     * @param columnName Name of the column whose index is being resolved.
     * @return Column index or {@code -1} if the specified column is not found.
     */
    int indexOf(String columnName);
}
