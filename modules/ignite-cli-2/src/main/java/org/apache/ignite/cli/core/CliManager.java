package org.apache.ignite.cli.core;

import io.micronaut.configuration.picocli.MicronautFactory;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.ignite.cli.core.repl.Repl;
import org.apache.ignite.cli.core.repl.executor.ReplExecutor;
import org.jline.terminal.Terminal;
import picocli.shell.jline3.PicocliCommands.PicocliCommandsFactory;

@Singleton
public class CliManager {
    private final Deque<Repl> replQueue = new ArrayDeque<>();
    private ReplExecutor replExecutor;
    private Repl currentRepl;

    @Inject
    private Terminal terminal;

    public void init(MicronautFactory micronautFactory) {
        replExecutor = new ReplExecutor(new PicocliCommandsFactory(micronautFactory), terminal);
    }

    public void enableRepl(Repl repl) {
        if (currentRepl != null) {
            currentRepl.onSleep();
            replQueue.add(currentRepl);
        }
        currentRepl = repl;
        currentRepl.onEnable();
        replExecutor.execute(currentRepl);
    }

    public void exitCurrentRepl() {
        if (currentRepl != null) {
            currentRepl.dispose();
        }
        currentRepl = replQueue.pop();
        if (currentRepl != null) {
            currentRepl.onWakeUp();
        } else {
            exit();
        }
    }


    private void exit() {
        System.exit(0);
    }
}
