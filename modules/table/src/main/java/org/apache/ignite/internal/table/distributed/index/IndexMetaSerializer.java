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

package org.apache.ignite.internal.table.distributed.index;

import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerializer;

/**
 * {@link VersionedSerializer} for {@link IndexMeta} instances.
 */
class IndexMetaSerializer extends VersionedSerializer<IndexMeta> {
    static final IndexMetaSerializer INSTANCE = new IndexMetaSerializer();

    @Override
    protected void writeExternalData(IndexMeta meta, IgniteDataOutput out) throws IOException {
        out.writeVarInt(meta.catalogVersion());
        out.writeVarInt(meta.indexId());
        out.writeVarInt(meta.tableId());
        out.writeVarInt(meta.tableVersion());
        out.writeUTF(meta.indexName());
        out.writeVarInt(meta.status().code());

        out.writeVarInt(meta.statusChanges().size());
        for (Entry<MetaIndexStatus, MetaIndexStatusChange> entry : meta.statusChanges().entrySet()) {
            out.writeVarInt(entry.getKey().code());

            MetaIndexStatusChange change = entry.getValue();
            out.writeVarInt(change.catalogVersion());
            hybridTimestamp(change.activationTimestamp()).writeTo(out);
        }
    }

    @Override
    protected IndexMeta readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        int catalogVersion = in.readVarIntAsInt();
        int indexId = in.readVarIntAsInt();
        int tableId = in.readVarIntAsInt();
        int tableVersion = in.readVarIntAsInt();
        String indexName = in.readUTF();
        MetaIndexStatus status = MetaIndexStatus.findByCode(in.readVarIntAsInt());
        Map<MetaIndexStatus, MetaIndexStatusChange> statusChanges = readStatusChanges(in);

        return new IndexMeta(
                catalogVersion,
                indexId,
                tableId,
                tableVersion,
                indexName,
                status,
                statusChanges
        );
    }

    private static Map<MetaIndexStatus, MetaIndexStatusChange> readStatusChanges(IgniteDataInput in) throws IOException {
        int length = in.readVarIntAsInt();

        Map<MetaIndexStatus, MetaIndexStatusChange> map = new EnumMap<>(MetaIndexStatus.class);
        for (int i = 0; i < length; i++) {
            MetaIndexStatus status = MetaIndexStatus.findByCode(in.readVarIntAsInt());

            int catalogVersion = in.readVarIntAsInt();
            HybridTimestamp activationTimestamp = HybridTimestamp.readFrom(in);
            MetaIndexStatusChange change = new MetaIndexStatusChange(catalogVersion, activationTimestamp.longValue());

            map.put(status, change);
        }

        return map;
    }
}
