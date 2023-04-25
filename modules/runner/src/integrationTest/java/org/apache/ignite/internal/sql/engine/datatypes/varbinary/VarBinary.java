package org.apache.ignite.internal.sql.engine.datatypes.varbinary;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.sql.engine.util.NativeTypeWrapper;
import org.jetbrains.annotations.NotNull;

/**
 * A wrapper for {@link NativeTypes#BYTES}.
 */
public final class VarBinary implements NativeTypeWrapper<VarBinary> {

    private final byte[] bytes;

    /** Constructor */
    private VarBinary(byte[] bytes) {
        this.bytes = bytes;
    }

    /** Creates a var binary object from the given bytes array*/
    public static VarBinary fromBytes(byte[] bytes) {
        return new VarBinary(bytes);
    }

    /** Creates a var binary object from UTF-8 bytes obtained from the given string. */
    public static VarBinary fromUtf8String(String string) {
        return new VarBinary(string.getBytes(StandardCharsets.UTF_8));
    }

    /** Converts this var binary object ot a java string of the given {@link Charset}. */
    public String toString(Charset charset) {
        return new String(bytes, charset);
    }

    /** {@inheritDoc} */
    @Override
    public byte[] get() {
        return bytes;
    }

    /** {@inheritDoc} */
    @Override
    public int compareTo(@NotNull VarBinary o) {
        return Arrays.compare(bytes, o.bytes);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VarBinary varBinary = (VarBinary) o;
        return Arrays.equals(bytes, varBinary.bytes);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toString(StandardCharsets.UTF_8);
    }
}
