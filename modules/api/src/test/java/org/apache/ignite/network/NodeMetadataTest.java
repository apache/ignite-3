package org.apache.ignite.network;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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
        ByteBuffer buffer = ByteBuffer.allocate(1);
        NodeMetadata fromByteBuffer = NodeMetadata.fromByteBuffer(buffer);
        assertNull(fromByteBuffer);
    }
}