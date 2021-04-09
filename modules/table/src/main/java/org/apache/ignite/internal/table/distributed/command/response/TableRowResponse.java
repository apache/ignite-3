package org.apache.ignite.internal.table.distributed.command.response;

import org.apache.ignite.internal.schema.BinaryRow;

public class TableRowResponse {
    private BinaryRow row;

    public TableRowResponse(BinaryRow row) {
        this.row = row;
    }

    public BinaryRow getValue() {
        return row;
    }
}
