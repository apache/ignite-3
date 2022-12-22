package org.apache.ignite.internal.cli.core.repl;

public interface SessionEventListener {

    /** Implementation must be async. */
    void onConnect(Session session);

    /** Implementation must be async. */
    void onDisconnect();
}
