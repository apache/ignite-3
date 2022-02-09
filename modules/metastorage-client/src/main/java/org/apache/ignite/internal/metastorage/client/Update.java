package org.apache.ignite.internal.metastorage.client;

import java.util.Collection;

/**
 * Simple operations + result wrapper to describe the terminal branch of {@link If} execution.
 */
public class Update {

    /** Operations. */
    private final Collection<Operation> ops;

    /** Result. */
    private final StatementResult result;

    /**
     * Constructs new update object.
     *
     * @param ops operations
     * @param result result
     */
    public Update(Collection<Operation> ops, StatementResult result) {
        this.ops = ops;
        this.result = result;
    }

    /**
     * Returns operations.
     *
     * @return operations.
     */
    public Collection<Operation> operations() {
        return ops;
    }

    /**
     * Returns result.
     *
     * @return result.
     */
    public StatementResult result() {
        return result;
    }
}
