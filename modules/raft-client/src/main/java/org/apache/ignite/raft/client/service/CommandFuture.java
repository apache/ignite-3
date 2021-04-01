package org.apache.ignite.raft.client.service;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.raft.client.Command;

public interface CommandFuture<R extends Command> {
    R command();

    CompletableFuture future();
}
