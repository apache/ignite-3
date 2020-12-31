package com.alipay.sofa.jraft.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 *
 */
public class JDKMarshaller implements Marshaller {
    /**
     * {@inheritDoc}
     */
    @Override public byte[] marshall(Object o) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(o);
            oos.close();
            return baos.toByteArray();
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override public <T> T unmarshall(byte[] raw) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(raw);
            ObjectInputStream oos = new ObjectInputStream(bais);
            return (T) oos.readObject();
        } catch (Exception e) {
            throw new Error(e);
        }
    }
}
