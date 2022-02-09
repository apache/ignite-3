package org.apache.ignite.internal.metastorage.client;

public class If {

    private final Condition condition;
    private final Statement andThen;
    private final Statement orElse;

    public If(Condition condition, Statement andThen, Statement orElse) {
        this.condition = condition;
        this.andThen = andThen;
        this.orElse = orElse;
    }

    public Condition condition() {
        return condition;
    }

    public Statement andThen() {
        return andThen;
    }

    public Statement orElse() {
        return orElse;
    }

    public static If iif(Condition condition, If andThen, If orElse) {
        return new If(condition, new Statement(andThen), new Statement(orElse));
    }

    public static If iif(Condition condition, If andThen, Update orElse) {
        return new If(condition, new Statement(andThen), new Statement(orElse));
    }

    public static If iif(Condition condition, Update andThen, If orElse) {
        return new If(condition, new Statement(andThen), new Statement(orElse));
    }

    public static If iif(Condition condition, Update andThen, Update orElse) {
        return new If(condition, new Statement(andThen), new Statement(orElse));
    }

}

