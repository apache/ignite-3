package org.apache.ignite.internal.cli.core.repl;

public interface SessionEventListener {
    void onConnect(Session session);

    void onDisconnect();
}
