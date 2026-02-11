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

package org.apache.ignite.internal.client.proto;

import java.util.BitSet;
import java.util.EnumMap;
import java.util.Map;

/**
 * Utils for client protocol handshake handling.
 */
public class HandshakeUtils {
    /** Client type: general. */
    public static final int CLIENT_TYPE_GENERAL = 2;

    /**
     * Packs extensions.
     *
     * @param packer Packer.
     * @param extensions Extensions.
     */
    public static void packExtensions(ClientMessagePacker packer, Map<HandshakeExtension, Object> extensions) {
        packer.packInt(extensions.size());

        for (Map.Entry<HandshakeExtension, Object> entry : extensions.entrySet()) {
            packer.packString(entry.getKey().key());
            packer.packString((String) entry.getValue());
        }
    }

    /**
     * Unpacks extensions.
     *
     * @param unpacker Unpacker.
     * @return Extensions.
     */
    public static Map<HandshakeExtension, Object> unpackExtensions(ClientMessageUnpacker unpacker) {
        EnumMap<HandshakeExtension, Object> extensions = new EnumMap<>(HandshakeExtension.class);
        int mapSize = unpacker.unpackInt();

        for (int i = 0; i < mapSize; i++) {
            String key = unpacker.unpackString();
            HandshakeExtension handshakeExtension = HandshakeExtension.fromKey(key);

            if (handshakeExtension != null) {
                extensions.put(handshakeExtension, unpackExtensionValue(handshakeExtension, unpacker));
            } else {
                // Unknown extension, skip it.
                unpacker.skipValues(1);
            }
        }

        return extensions;
    }

    /**
     * Packs features.
     *
     * @param packer Packer.
     * @param features Features bit set.
     */
    public static void packFeatures(ClientMessagePacker packer, BitSet features) {
        packer.packBinary(features.toByteArray());
    }

    /**
     * Unpacks features.
     *
     * @param unpacker Unpacker.
     * @return Features.
     */
    public static BitSet unpackFeatures(ClientMessageUnpacker unpacker) {
        // BitSet.valueOf is always little-endian.
        return BitSet.valueOf(unpacker.readBinary());
    }

    private static Object unpackExtensionValue(HandshakeExtension handshakeExtension, ClientMessageUnpacker unpacker) {
        Class<?> type = handshakeExtension.valueType();
        if (type == String.class) {
            return unpacker.unpackString();
        } else {
            throw new IllegalArgumentException("Unsupported extension type: " + type.getName());
        }
    }

    /** Returns a bit that includes only supported features. */
    public static BitSet supportedFeatures(BitSet supportedFeatures, BitSet requestedFeatures) {
        BitSet result = (BitSet) supportedFeatures.clone();
        result.and(requestedFeatures);
        return result;
    }
}
