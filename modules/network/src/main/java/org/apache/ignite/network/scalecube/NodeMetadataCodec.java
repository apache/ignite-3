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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.scalecube.cluster.metadata.MetadataCodec;
import java.nio.ByteBuffer;
import org.apache.ignite.network.NodeMetadata;

/**
 * Codec for serialization and deserialization of {@link NodeMetadata}.
 */
public class NodeMetadataCodec implements MetadataCodec {

    public static final NodeMetadataCodec INSTANCE = new NodeMetadataCodec();
    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Default constructor.
     */
    private NodeMetadataCodec() {
    }

    /**
     * Deserializes {@link ByteBuffer} to {@link NodeMetadata}.
     *
     * @param byteBuffer {@link ByteBuffer} to deserialize.
     * @return {@link NodeMetadata} or null if something goes wrong.
     */
    @Override
    public NodeMetadata deserialize(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        }
        try {
            return mapper.readValue(byteBuffer.array(), NodeMetadata.class);
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
        try {
            return ByteBuffer.wrap(mapper.writeValueAsBytes(o));
        } catch (JsonProcessingException e) {
            return ByteBuffer.wrap(new byte[0]);
        }
    }
}
