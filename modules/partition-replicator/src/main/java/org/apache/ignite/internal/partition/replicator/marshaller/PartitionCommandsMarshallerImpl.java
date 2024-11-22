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

package org.apache.ignite.internal.partition.replicator.marshaller;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.partition.replicator.network.command.CatalogVersionAware;
import org.apache.ignite.internal.raft.util.OptimizedMarshaller;
import org.apache.ignite.internal.replicator.command.SafeTimePropagatingCommand;

/**
 * Default {@link PartitionCommandsMarshaller} implementation.
 */
public class PartitionCommandsMarshallerImpl extends OptimizedMarshaller implements PartitionCommandsMarshaller {
    public PartitionCommandsMarshallerImpl(MessageSerializationRegistry serializationRegistry, ByteBuffersPool cache) {
        super(serializationRegistry, cache);
    }

    @Override
    protected void beforeWriteMessage(Object o, ByteBuffer buffer) {
        int requiredCatalogVersion = o instanceof CatalogVersionAware
                ? ((CatalogVersionAware) o).requiredCatalogVersion()
                : PartitionCommandsMarshaller.NO_VERSION_REQUIRED;

        stream.setBuffer(buffer);
        stream.writeFixedInt(requiredCatalogVersion);
        stream.writeFixedLong(0); // TODO FIXME optimize double write.
    }

    @Override
    public <T> T unmarshall(ByteBuffer raw) {
        raw = raw.duplicate();

        int requiredCatalogVersion = readRequiredCatalogVersion(raw);
        long safeTs = readSafeTimestamp(raw);

        T res = super.unmarshall(raw);

        if (res instanceof CatalogVersionAware) {
            ((CatalogVersionAware) res).requiredCatalogVersion(requiredCatalogVersion);
        }

        // Apply patched value.
        if (res instanceof SafeTimePropagatingCommand && safeTs != 0) {
            ((SafeTimePropagatingCommand) res).safeTime(HybridTimestamp.hybridTimestamp(safeTs));
        }

        return res;
    }

    @Override
    public int readRequiredCatalogVersion(ByteBuffer raw) {
        return raw.getInt();
    }

    @Override
    public long readSafeTimestamp(ByteBuffer raw) {
        return raw.getLong();
    }
}
