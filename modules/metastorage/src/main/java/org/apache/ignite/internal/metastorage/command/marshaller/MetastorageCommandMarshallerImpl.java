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

package org.apache.ignite.internal.metastorage.command.marshaller;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.raft.util.OptimizedMarshaller;
import org.apache.ignite.internal.replicator.command.SafeTimePropagatingCommand;

public class MetastorageCommandMarshallerImpl extends OptimizedMarshaller implements MetastorageCommandMarshaller {
    public MetastorageCommandMarshallerImpl(MessageSerializationRegistry serializationRegistry, ByteBuffersPool cache) {
        super(serializationRegistry, cache);
    }

    @Override
    public void patch(ByteBuffer raw, HybridTimestamp safeTs) {
        raw.putLong(0, safeTs.longValue());
    }

    @Override
    protected void beforeWriteMessage(Object o, ByteBuffer buffer) {
        stream.setBuffer(buffer);
        stream.writeFixedLong(0);
    }

    @Override
    public <T> T unmarshall(ByteBuffer raw) {
        raw = raw.duplicate();

        long safeTs = readSafeTimestamp(raw);

        System.out.println("safeTs:" + safeTs);

        T res = super.unmarshall(raw);

        System.out.println("cmd:" + res.getClass().getName());

        if (res instanceof SafeTimePropagatingCommand && safeTs != 0) {
            ((SafeTimePropagatingCommand) res).safeTime(HybridTimestamp.hybridTimestamp(safeTs));
        }

        return res;
    }

    @Override
    public long readSafeTimestamp(ByteBuffer raw) {
        return raw.getLong();
    }
}
