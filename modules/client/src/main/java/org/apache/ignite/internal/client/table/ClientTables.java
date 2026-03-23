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

import static org.apache.ignite.internal.util.ViewUtils.sync;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.ClientChannel;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature;
import org.apache.ignite.internal.marshaller.MarshallersProvider;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.Table;

/**
 * Client tables API implementation.
 */
public class ClientTables implements IgniteTables {
    private final ReliableChannel ch;

    private final MarshallersProvider marshallers;
    private final int sqlPartitionAwarenessMetadataCacheSize;

    /**
     * Constructor.
     *
     * @param ch Channel.
     * @param marshallers Marshallers provider.
     * @param sqlPartitionAwarenessMetadataCacheSize Size of the cache to store partition awareness metadata.
     */
    public ClientTables(ReliableChannel ch, MarshallersProvider marshallers, int sqlPartitionAwarenessMetadataCacheSize) {
        this.ch = ch;
        this.marshallers = marshallers;
        this.sqlPartitionAwarenessMetadataCacheSize = sqlPartitionAwarenessMetadataCacheSize;
    }

    /** {@inheritDoc} */
    @Override
    public List<Table> tables() {
        return sync(tablesAsync());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<Table>> tablesAsync() {
        return ch.serviceAsync((ch) -> useQualifiedNames(ch) ? ClientOp.TABLES_GET_QUALIFIED : ClientOp.TABLES_GET,
                ClientOp.TABLES_GET,
                null, r -> {
                    var in = r.in();
                    var cnt = in.unpackInt();
                    var res = new ArrayList<Table>(cnt);
                    boolean unpackQualifiedNames = useQualifiedNames(r.clientChannel());

                    for (int i = 0; i < cnt; i++) {
                        int tableId = in.unpackInt();
                        QualifiedName name = unpackQualifiedNames ? in.unpackQualifiedName() : QualifiedName.parse(in.unpackString());

                        res.add(new ClientTable(ch, marshallers, tableId, name, sqlPartitionAwarenessMetadataCacheSize));
                    }

                    return res;
                });
    }

    /** {@inheritDoc} */
    @Override
    public Table table(QualifiedName name) {
        return sync(tableAsync(name));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Table> tableAsync(QualifiedName name) {
        Objects.requireNonNull(name);

        return ch.serviceAsync((ch) -> useQualifiedNames(ch) ? ClientOp.TABLE_GET_QUALIFIED : ClientOp.TABLE_GET,
                ClientOp.TABLE_GET,
                w -> {
                    if (useQualifiedNames(w.clientChannel())) {
                        w.out().packQualifiedName(name);
                    } else {
                        w.out().packString(name.objectName());
                    }
                }, r -> {
                    if (r.in().tryUnpackNil()) {
                        return null;
                    }

                    int tableId = r.in().unpackInt();
                    boolean unpackQualifiedNames = useQualifiedNames(r.clientChannel());
                    QualifiedName qname = unpackQualifiedNames ? r.in().unpackQualifiedName() : QualifiedName.parse(r.in().unpackString());

                    return new ClientTable(ch, marshallers, tableId, qname, sqlPartitionAwarenessMetadataCacheSize);
                });
    }

    private static boolean useQualifiedNames(ClientChannel ch) {
        return ch.protocolContext().isFeatureSupported(ProtocolBitmaskFeature.TABLE_GET_REQS_USE_QUALIFIED_NAME);
    }
}
