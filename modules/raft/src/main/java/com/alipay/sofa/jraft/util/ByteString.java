package com.alipay.sofa.jraft.util;

import java.nio.ByteBuffer;

public class ByteString {
    public static final ByteString EMPTY = new ByteString(ByteBuffer.wrap(new byte[0]));

    private ByteBuffer buf;

    public ByteString(ByteBuffer buf) {
        this.buf = buf;
    }

    public ByteString(byte[] bytes) {
        this.buf = ByteBuffer.wrap(bytes);
    }

    public int size() {
        return buf.capacity();
    }

    public ByteBuffer asReadOnlyByteBuffer() {
        return buf.asReadOnlyBuffer();
    }
}
