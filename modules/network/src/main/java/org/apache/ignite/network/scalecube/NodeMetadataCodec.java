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

package org.apache.ignite.network.scalecube;

import io.scalecube.cluster.metadata.MetadataCodec;
import java.nio.ByteBuffer;
import org.apache.ignite.network.NodeMetadata;

/**
 * Codec for serialization and deserialization of {@link NodeMetadata}.
 */
public class NodeMetadataCodec implements MetadataCodec {

    public static final NodeMetadataCodec INSTANCE = new NodeMetadataCodec();

    /**
     * Version of the codec.
     */
    private int version = 1;

    /**
     * Default constructor.
     */
    private NodeMetadataCodec() {
    }

    /**
     * Constructor.
     *
     * @param version .
     */
    NodeMetadataCodec(int version) {
        this.version = version;
    }

    /**
     * Deserializes {@link ByteBuffer} to {@link NodeMetadata}.
     *
     * @param byteBuffer {@link ByteBuffer} to deserialize.
     * @return {@link NodeMetadata} or null if something goes wrong.
     */
    @Override
    public NodeMetadata deserialize(ByteBuffer byteBuffer) {
        try {
            int version = readInt(byteBuffer, 0);
            if (version == this.version) {
                int port = readInt(byteBuffer, 4);
                return new NodeMetadata(port);
            } else {
                return null;
            }
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Serializes {@link NodeMetadata} to {@link ByteBuffer}.
     *
     * @param o {@link NodeMetadata} to serialize.
     * @return {@link ByteBuffer}.
     */
    @Override
    public ByteBuffer serialize(Object o) {
        if (o instanceof NodeMetadata) {
            NodeMetadata metadata = (NodeMetadata) o;
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putInt(version);
            buffer.putInt(metadata.restPort());
            return buffer;
        } else {
            return ByteBuffer.wrap(new byte[0]);
        }
    }

    /**
     * Reads a specific integer byte value (4 bytes) from the input byte buffer at the given offset.
     *
     * @param buf input byte buffer
     * @param pos offset into the byte buffer to read
     * @return the int value read
     */
    private static int readInt(ByteBuffer buf, int pos) {
        return (((buf.get(pos) & 0xff) << 24)
                | ((buf.get(pos + 1) & 0xff) << 16)
                | ((buf.get(pos + 2) & 0xff) << 8) | (buf.get(pos + 3) & 0xff));
    }
}
