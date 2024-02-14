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

package org.apache.ignite.internal.raft.util;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.util.OptimizedMarshaller.ByteBuffersPool;

/**
 * Thread-safe variant of {@link OptimizedMarshaller}.
 */
public class ThreadLocalOptimizedMarshaller implements Marshaller {
    /** Thread-local optimized marshaller holder. Not static, because it depends on serialization registry. */
    private final ThreadLocal<Marshaller> marshaller;

    /** Shared pool of byte buffers for all thread-local instances. */
    private final ByteBuffersPool pool = new DefaultByteBuffersPool(Runtime.getRuntime().availableProcessors());

    /**
     * Constructor.
     *
     * @param serializationRegistry Serialization registry.
     */
    public ThreadLocalOptimizedMarshaller(MessageSerializationRegistry serializationRegistry) {
        marshaller = ThreadLocal.withInitial(() -> new OptimizedMarshaller(serializationRegistry, pool));
    }

    @Override
    public byte[] marshall(Object o) {
        return marshaller.get().marshall(o);
    }

    @Override
    public <T> T unmarshall(ByteBuffer bytes) {
        return marshaller.get().unmarshall(bytes);
    }
}
