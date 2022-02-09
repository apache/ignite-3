package org.apache.ignite.internal.metastorage.client;

public class Statement {
    private final If _if;
    private final Update update;
    
    public Statement(If _if) {
        this._if = _if;
        this.update = null;
    }
    
    public Statement(Update update) {
        this.update = update;
        this._if = null;
    }
    
    public boolean isTerminal() {
        return update != null;
    }
    
    public If _if() {
        return _if;
    }
    
    public Update update() {
        return update;
    }
}
