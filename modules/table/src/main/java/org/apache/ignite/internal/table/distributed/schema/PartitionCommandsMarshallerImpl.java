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

package org.apache.ignite.internal.table.distributed.schema;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.raft.util.OptimizedMarshaller;
import org.apache.ignite.internal.table.distributed.command.CatalogVersionAware;
import org.apache.ignite.internal.util.VarIntUtils;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;

/**
 * Default {@link PartitionCommandsMarshaller} implementation.
 */
public class PartitionCommandsMarshallerImpl extends OptimizedMarshaller implements PartitionCommandsMarshaller {
    public PartitionCommandsMarshallerImpl(MessageSerializationRegistry serializationRegistry) {
        super(serializationRegistry);
    }

    @Override
    public byte[] marshall(Object o) {
        int requiredCatalogVersion = o instanceof CatalogVersionAware ? ((CatalogVersionAware) o).requiredCatalogVersion() : 0;

        stream.setBuffer(buffer);
        stream.writeInt(requiredCatalogVersion);

        return super.marshall(o);
    }

    @Override
    public <T> T unmarshall(ByteBuffer raw) {
        skipRequiredCatalogVersion(raw);

        return super.unmarshall(raw);
    }

    private void skipRequiredCatalogVersion(ByteBuffer raw) {
        readRequiredCatalogVersion(raw);
    }

    /**
     * Reads required catalog version from the provided buffer.
     *
     * @param raw Buffer to read from.
     * @return Catalog version.
     */
    @Override
    public int readRequiredCatalogVersion(ByteBuffer raw) {
        return VarIntUtils.readVarInt(raw);
    }
}
