package org.apache.ignite.raft.jraft.util;

public interface Marshaller {
    public static Marshaller DEFAULT = new JDKMarshaller();

    byte[] marshall(Object o);

    <T> T unmarshall(byte[] raw);
}
