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

package org.apache.ignite.client.internal.table;

import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;

public class ClientSchema {
    /** Schema version. Incremented on each schema modification. */
    private final int ver;

    /** Columns. */
    private final ClientColumn[] columns;

    private final Map<String, ClientColumn> keyColumns = new HashMap<>();

    private final Map<String, ClientColumn> valColumns = new HashMap<>();

    public ClientSchema(int ver, ClientColumn[] columns) {
        assert ver >= 0;
        assert columns != null;

        this.ver = ver;
        this.columns = columns;

        for (var col : columns) {
            if (col.key())
                keyColumns.put(col.name(), col);
            else
                valColumns.put(col.name(), col);
        }
    }

    public int version() {
        return ver;
    }

    public @NotNull ClientColumn[] columns() {
        return columns;
    }

    public @NotNull Map<String, ClientColumn> keyColumns() {
        return keyColumns;
    }

    public @NotNull Map<String, ClientColumn> valColumns() {
        return valColumns;
    }
}
