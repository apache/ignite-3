package org.apache.ignite.internal.table.distributed.command;

import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.tx.Lockable;
import org.jetbrains.annotations.NotNull;

public interface LockableCommand extends Lockable {
    /**
     * @param row The row.
     * @return Bytes.
     */
    default byte[] extractAndWrapKey(@NotNull BinaryRow row) {
        byte[] key = new byte[row.keySlice().capacity()];
        row.keySlice().get(key);

        return key;
    }
}
