package org.apache.ignite.client.proto.query.event;

import org.apache.ignite.client.proto.ClientMessagePacker;
import org.apache.ignite.client.proto.ClientMessageUnpacker;

public class ClientMessageUtils {
    public static void writeStringNullable(ClientMessagePacker packer, String str) {
        if (str == null)
            packer.packNil();
        else
            packer.packString(str);
    }

    public static String readStringNullable(ClientMessageUnpacker unpacker) {
        if (!unpacker.tryUnpackNil())
            return unpacker.unpackString();

        return null;
    }
}
