package org.apache.ignite.raft.client.service;

import org.apache.ignite.raft.client.Command;

/**
 * A closure to notify abbout command processing outcome.
 * @param <R> Command type.
 */
public interface CommandClosure<R extends Command> {
    /**
     * @return The command.
     */
    R command();

    /**
     * Success outcome.
     * @param res The result.
     */
    void success(Object res);

    /**
     * Failure outcome.
     * @param err The Error.
     */
    void failure(Throwable err);
}
