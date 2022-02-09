package org.apache.ignite.internal.metastorage.common;

import java.io.Serializable;

public class StatementResultInfo implements Serializable {
    private final byte[] res;

    public StatementResultInfo(byte[] res) {
        this.res = res;
    }

    public byte[] result() {
        return res;
    }

}
