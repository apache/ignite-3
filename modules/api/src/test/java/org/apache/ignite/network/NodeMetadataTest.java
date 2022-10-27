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

package org.apache.ignite.network;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class NodeMetadataTest {

    @Test
    void testToByteBufferAndFromByteBuffer() {
        NodeMetadata metadata = new NodeMetadata(10000);
        ByteBuffer buffer = metadata.toByteBuffer();
        NodeMetadata fromByteBuffer = NodeMetadata.fromByteBuffer(buffer);
        assertEquals(metadata, fromByteBuffer);
    }

    @Test
    void testFromByteBufferWithWrongContent() {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        NodeMetadata fromByteBuffer = NodeMetadata.fromByteBuffer(buffer);
        assertNull(fromByteBuffer);
    }
}