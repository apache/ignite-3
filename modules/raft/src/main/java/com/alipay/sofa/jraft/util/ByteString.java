package com.alipay.sofa.jraft.util;

import java.io.ByteArrayOutputStream;
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

    public byte[] toByteArray() {
        if (buf.hasArray())
            return buf.array();

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        WritableByteChannel channel = Channels.newChannel(bos);
        try {
            channel.write(buf);
        } catch (IOException e) {
            throw new Error(e);
        }
        return bos.toByteArray();
    }

    public ByteString copy() {
        return this == EMPTY ? EMPTY : new ByteString(toByteArray());
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ByteString that = (ByteString) o;

        return buf.equals(that.buf);
    }

    @Override public int hashCode() {
        return buf.hashCode();
    }
}
