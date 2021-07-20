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

import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

public class ClientSchema {
    /** Schema version. Incremented on each schema modification. */
    private final int ver;

    /** Key columns count. */
    private final int keyColumnCount;

    /** Columns. */
    private final ClientColumn[] columns;

    private final Map<String, ClientColumn> map = new HashMap<>();

    public ClientSchema(int ver, ClientColumn[] columns) {
        assert ver >= 0;
        assert columns != null;

        this.ver = ver;
        this.columns = columns;

        var keyCnt = 0;

        for (var col : columns) {
            if (col.key())
                keyCnt++;

            map.put(col.name(), col);
        }

        keyColumnCount = keyCnt;
    }

    public int version() {
        return ver;
    }

    public @NotNull ClientColumn[] columns() {
        return columns;
    }

    public @Nullable ClientColumn column(String name) {
        return map.get(name);
    }

    public @NotNull ClientColumn requiredColumn(String name) {
        var column = map.get(name);

        if (column == null)
            throw new IgniteException("Column is not present in schema: " + name);

        return column;
    }

    public int keyColumnCount() {
        return keyColumnCount;
    }
}
