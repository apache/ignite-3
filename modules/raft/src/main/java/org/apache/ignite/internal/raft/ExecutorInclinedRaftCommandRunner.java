package org.apache.ignite.internal.raft;

import static java.util.function.Function.identity;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;

public class ExecutorInclinedRaftCommandRunner implements RaftCommandRunner {
    private final RaftCommandRunner commandRunner;

    private final Executor completionExecutor;

    public ExecutorInclinedRaftCommandRunner(RaftCommandRunner commandRunner, Executor completionExecutor) {
        this.commandRunner = commandRunner;
        this.completionExecutor = completionExecutor;
    }

    @Override
    public <R> CompletableFuture<R> run(Command cmd) {
        return commandRunner.<R>run(cmd)
                .thenApplyAsync(identity(), completionExecutor);
    }
}
