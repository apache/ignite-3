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

package org.apache.ignite.internal.client.table;

import static org.apache.ignite.internal.client.ClientUtils.sync;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.TopologyCache;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.marshaller.MarshallersProvider;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.manager.IgniteTables;

/**
 * Client tables API implementation.
 */
public class ClientTables implements IgniteTables {
    private final ReliableChannel ch;

    private final MarshallersProvider marshallers;

    private final TopologyCache topologyCache;

    /**
     * Constructor.
     *
     * @param ch Channel.
     * @param marshallers Marshallers provider.
     * @param topologyCache Server topology cache.
     */
    public ClientTables(ReliableChannel ch, MarshallersProvider marshallers, TopologyCache topologyCache) {
        this.ch = ch;
        this.marshallers = marshallers;
        this.topologyCache = topologyCache;
    }

    /** {@inheritDoc} */
    @Override
    public List<Table> tables() {
        return sync(tablesAsync());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<Table>> tablesAsync() {
        return ch.serviceAsync(ClientOp.TABLES_GET, r -> {
            var in = r.in();
            var cnt = in.unpackInt();
            var res = new ArrayList<Table>(cnt);

            for (int i = 0; i < cnt; i++) {
                res.add(new ClientTable(ch, marshallers, topologyCache, in.unpackInt(), in.unpackString()));
            }

            return res;
        });
    }

    /** {@inheritDoc} */
    @Override
    public Table table(String name) {
        return sync(tableAsync(name));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Table> tableAsync(String name) {
        Objects.requireNonNull(name);

        return ch.serviceAsync(ClientOp.TABLE_GET, w -> w.out().packString(name),
                r -> r.in().tryUnpackNil() ? null : new ClientTable(ch, marshallers, topologyCache, r.in().unpackInt(), name));
    }
}
