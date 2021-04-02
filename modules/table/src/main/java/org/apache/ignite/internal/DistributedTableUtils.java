package org.apache.ignite.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedTableUtils {
    private static Logger log = LoggerFactory.getLogger(DistributedTableUtils.class);

    public static byte[] toBytes(Object obj) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            try (ObjectOutputStream out = new ObjectOutputStream(bos)) {

                out.writeObject(obj);

                out.flush();

                return bos.toByteArray();
            }
        }
        catch (Exception e) {
            log.warn("Could not serialize a class [cls=" + obj.getClass().getName() + "]", e);

            return null;
        }
    }

    public static Object fromBytes(byte[] bytes) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
            try (ObjectInputStream in = new ObjectInputStream(bis)) {
                return in.readObject();
            }
        }
        catch (Exception e) {
            log.warn("Could not deserialize an object", e);

            return null;
        }
    }
}
