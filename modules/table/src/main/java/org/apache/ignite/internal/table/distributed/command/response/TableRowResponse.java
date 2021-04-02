package org.apache.ignite.internal.table.distributed.command.response;

import org.apache.ignite.internal.table.TableRow;

public class TableRowResponse {
    private TableRow row;

    public TableRowResponse(TableRow row) {
        this.row = row;
    }

    public TableRow getValue() {
        return row;
    }
}
