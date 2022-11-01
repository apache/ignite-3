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

import java.nio.ByteBuffer;
import org.apache.ignite.network.NodeMetadata;
import org.junit.jupiter.api.Test;

class NodeMetadataCodecTest {

    private static final NodeMetadataCodec METADATA_CODEC = NodeMetadataCodec.INSTANCE;

    @Test
    void serializeAndDeserialize() {
        NodeMetadata metadata = new NodeMetadata("localhost", 10000);
        ByteBuffer buffer = METADATA_CODEC.serialize(metadata);
        NodeMetadata fromByteBuffer = METADATA_CODEC.deserialize(buffer);
        assertEquals(metadata, fromByteBuffer);
    }

    @Test
    void serializeNull() {
        assertNull(METADATA_CODEC.serialize(null));
    }

    @Test
    void deserializeNull() {
        assertNull(METADATA_CODEC.deserialize(null));
    }

    @Test
    void fromByteBufferWithWrongContent() {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        NodeMetadata fromByteBuffer = METADATA_CODEC.deserialize(buffer);
        assertNull(fromByteBuffer);
    }
}
