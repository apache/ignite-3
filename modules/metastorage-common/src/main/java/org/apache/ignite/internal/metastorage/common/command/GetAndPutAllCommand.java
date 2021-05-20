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

package org.apache.ignite.internal.metastorage.common.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.raft.client.WriteCommand;
import org.jetbrains.annotations.NotNull;

/**
 * Get and put all command for MetaStorageCommandListener that inserts or updates entries
 * with given keys and given values and retrieves a previous entries for given keys.
 */
public final class GetAndPutAllCommand implements WriteCommand {
    /** Keys. */
    @NotNull private final List<byte[]> keys;

    /** Values. */
    @NotNull private final List<byte[]> vals;

    /**
     * @param vals Values.
     */
    public GetAndPutAllCommand(@NotNull Map<ByteArray, byte[]> vals) {
        int size = vals.size();

        this.keys = new ArrayList<>(size);
        this.vals = new ArrayList<>(size);

        for (Map.Entry<ByteArray, byte[]> e : vals.entrySet()) {
            this.keys.add(e.getKey().bytes());

            this.vals.add(e.getValue());
        }
    }

    /**
     * @return Keys.
     */
    public @NotNull List<byte[]> keys() {
        return keys;
    }

    /**
     * @return Values.
     */
    public @NotNull List<byte[]> vals() {
        return vals;
    }
}
