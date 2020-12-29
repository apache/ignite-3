package com.alipay.sofa.jraft.util;

import java.io.IOException;

public interface Marshaller {
    public static Marshaller DEFAULT = new JDKMarshaller();

    byte[] marshall(Object o) throws IOException;

    <T> T unmarshall(byte[] raw) throws IOException;
}
