package org.apache.ignite.internal.raft.util;

import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.raft.jraft.util.Marshaller;

/**
 * Thread-safe variant of {@link OptimizedMarshaller}.
 */
public class ThreadLocalOptimizedMarshaller implements Marshaller {
    /** Thread-local optimized marshaller holder. Not static, because it depends on serialization registry. */
    private final ThreadLocal<Marshaller> marshaller;

    /**
     * Constructor.
     *
     * @param serializationRegistry Serialization registry.
     */
    public ThreadLocalOptimizedMarshaller(MessageSerializationRegistry serializationRegistry) {
        marshaller = ThreadLocal.withInitial(() -> new OptimizedMarshaller(serializationRegistry));
    }

    @Override
    public byte[] marshall(Object o) {
        return marshaller.get().marshall(o);
    }

    @Override
    public <T> T unmarshall(byte[] bytes) {
        return marshaller.get().unmarshall(bytes);
    }
}
