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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.scalecube.cluster.metadata.JdkMetadataCodec;
import java.nio.ByteBuffer;
import org.apache.ignite.network.NodeMetadata;
import org.junit.jupiter.api.Test;

class MetadataSerDeTest {

    private static final NodeMetadataDeserializer METADATA_DESERIALIZER = new NodeMetadataDeserializer();
    private static final JdkMetadataCodec JDK_METADATA_CODEC = new JdkMetadataCodec();

    @Test
    void serializeAndDeserialize() {
        NodeMetadata metadata = new NodeMetadata("localhost", 10000);
        ByteBuffer buffer = JDK_METADATA_CODEC.serialize(metadata);
        NodeMetadata fromByteBuffer = METADATA_DESERIALIZER.deserialize(buffer);
        assertEquals(metadata, fromByteBuffer);
    }

    @Test
    void deserializeNull() {
        assertNull(METADATA_DESERIALIZER.deserialize(null));
    }

    @Test
    void deserializeEmptyByteBuffer() {
        assertNull(METADATA_DESERIALIZER.deserialize(ByteBuffer.wrap(new byte[0])));
    }
}
