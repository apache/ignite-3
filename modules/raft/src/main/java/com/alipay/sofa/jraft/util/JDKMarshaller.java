package com.alipay.sofa.jraft.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/** */
public class JDKMarshaller implements Marshaller {
    /** {@inheritDoc} */
    @Override public byte[] marshall(Object o) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(o);
        oos.close();

        return baos.toByteArray();
    }

    /** {@inheritDoc} */
    @Override public Object unmarshall(byte[] raw) throws IOException{
        ByteArrayInputStream bais = new ByteArrayInputStream(raw);
        ObjectInputStream oos = new ObjectInputStream(bais);

        try {
            return oos.readObject();
        } catch (ClassNotFoundException e) {
            throw new Error(e);
        }
    }
}
