package org.apache.ignite.raft.jraft.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

// TODO asch get rid
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
        byte[] arr = new byte[buf.remaining()];
        buf.get(arr);
        buf.flip();
        return arr;
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
