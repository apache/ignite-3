package org.apache.ignite.internal.metastorage.common;

import java.io.Serializable;
import org.apache.ignite.internal.metastorage.common.command.IfInfo;

public class StatementInfo implements Serializable {

    @SuppressWarnings("MemberName")
    private final IfInfo iif;
    private final UpdateInfo update;

    public StatementInfo(IfInfo iif) {
        this.iif = iif;
        this.update = null;
    }

    public StatementInfo(UpdateInfo update) {
        this.update = update;
        this.iif = null;
    }

    public boolean isTerminal() {
        return update != null;
    }

    public IfInfo iif() {
        return iif;
    }

    public UpdateInfo update() {
        return update;
    }
}
