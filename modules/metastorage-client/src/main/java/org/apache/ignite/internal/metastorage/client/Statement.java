package org.apache.ignite.internal.metastorage.client;

public class Statement {
    private final If iif;
    private final Update update;

    public Statement(If iif) {
        this.iif = iif;
        this.update = null;
    }

    public Statement(Update update) {
        this.update = update;
        this.iif = null;
    }

    public boolean isTerminal() {
        return update != null;
    }

    public If iif() {
        return iif;
    }

    public Update update() {
        return update;
    }
}
