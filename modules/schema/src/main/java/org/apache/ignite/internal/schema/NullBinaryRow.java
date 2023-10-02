package org.apache.ignite.internal.schema;

import java.nio.ByteBuffer;
import org.jetbrains.annotations.Nullable;

public class NullBinaryRow implements BinaryRow {
    @Override
    public final int schemaVersion() {
        return 0;
    }

    @Override
    public final int tupleSliceLength() {
        return 0;
    }

    @Override
    public final @Nullable ByteBuffer tupleSlice() {
        return null;
    }
}
