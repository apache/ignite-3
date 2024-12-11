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

package org.apache.ignite.internal.sql.engine.util;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.KeyValueView;

/** A table from TPC suite. */
public interface TpcTable {
    /** Returns name of the table. */
    String tableName();

    /** Returns number of column in the table. */
    int columnsCount();

    /** Returns name of the column with given 0-based index. */
    String columnName(int idx);

    /** Returns definition of a table including necessary indexes. */
    String ddlScript();

    /**
     * Returns DML string representing single-row INSERT statement with dynamic parameters placeholders.
     *
     * <p>The order of columns matches the order specified in TPC-H specification
     * (see table declarations or output of {@link #ddlScript()}).
     *
     * <p>The statement returned is tolerant to columns' type mismatch, implying you
     * can use any value while there is cast from provided value to required type.
     */
    String insertPrepareStatement();

    /**
     * Returns iterator returning rows of the corresponding table.
     *
     * <p>May be used to fill the table via {@link KeyValueView KV API} or {@link IgniteSql SQL API}.
     *
     * @param pathToDataset A path to a directory with CSV file containing data for the table.
     * @return Iterator over data of the table.
     * @throws IOException In case of error.
     */
    Iterator<Object[]> dataProvider(Path pathToDataset) throws IOException;
}
