package com.alipay.sofa.jraft.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

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

    public byte byteAt(int pos) {
        return buf.get(pos);
    }

    public void writeTo(OutputStream outputStream) throws IOException {
        WritableByteChannel channel = Channels.newChannel(outputStream);

        channel.write(buf);
    }
}
