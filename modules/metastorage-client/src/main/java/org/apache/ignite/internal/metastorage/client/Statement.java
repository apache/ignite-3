package org.apache.ignite.internal.metastorage.client;

/**
 * Simple Either-like wrapper to hold one of the statement type: {@link If} or {@link Update}.
 * Needed to construct and simple deconstruction of nested {@link If},
 * instead of empty interface and instanceof-based unwrap.
 *
 * @see If
 */
public class Statement {

    /** If value holder. */
    private final If iif;

    /** Update value holder. */
    private final Update update;

    /**
     * Constructs new {@link If} statement.
     *
     * @param iif If
     */
    public Statement(If iif) {
        this.iif = iif;
        this.update = null;
    }

    /**
     * Constructs new {@link Update} terminal statement.
     *
     * @param update Update
     */
    public Statement(Update update) {
        this.update = update;
        this.iif = null;
    }

    /**
     * Returns true, if statement has no nested statement (i.e. it is {@link Update} statement).
     *
     * @return true, if statement has no nested statement (i.e. it is {@link Update} statement).
     */
    public boolean isTerminal() {
        return update != null;
    }

    /**
     * Returns If or {@code null}, if no If value.
     *
     * <p>Note: check which field is filled by {@link #isTerminal()}</p>
     *
     * @return If or {@code null}, if no If value.
     */
    public If iif() {
        return iif;
    }

    /**
     * Returns Update or {@code null}, if no update value.
     * Note: check which field is filled by {@link #isTerminal()}
     *
     * @return Update or {@code null}, if no update value.
     */
    public Update update() {
        return update;
    }
}
