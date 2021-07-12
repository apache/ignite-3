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

package org.apache.ignite.client.internal;

import org.apache.ignite.client.ClientOp;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.manager.IgniteTables;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 *
 */
public class IgniteClientTables implements IgniteTables {
    /** */
    private final ReliableChannel ch;

    public IgniteClientTables(ReliableChannel ch) {
        this.ch = ch;
    }

    /** {@inheritDoc} */
    @Override public Table createTable(String name, Consumer<TableChange> tableInitChange) {
        // TODO
        return null;
    }

    /** {@inheritDoc} */
    @Override public void dropTable(String name) {
        ch.request(ClientOp.TABLE_DROP, w -> w.out().packString(name));
    }

    /** {@inheritDoc} */
    @Override public List<Table> tables() {
        return ch.service(ClientOp.TABLES_GET, r -> {
            var in = r.in();
            var cnt = in.unpackMapHeader();
            var res = new ArrayList<Table>(cnt);

            for (int i = 0; i < cnt; i++)
                res.add(new ClientTable(ch, in.unpackUuid(), in.unpackString()));

            return res;
        });
    }

    /** {@inheritDoc} */
    @Override public Table table(String name) {
        return ch.service(ClientOp.TABLE_GET, w -> w.out().packString(name),
                r -> new ClientTable(ch, r.in().unpackUuid(), name));
    }
}
