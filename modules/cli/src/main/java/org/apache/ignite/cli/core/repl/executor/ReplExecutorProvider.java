package org.apache.ignite.cli.core.repl.executor;

import io.micronaut.configuration.picocli.MicronautFactory;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jline.terminal.Terminal;
import picocli.shell.jline3.PicocliCommands.PicocliCommandsFactory;

/**
 * Provider of {@link ReplExecutor}.
 */
@Singleton
public class ReplExecutorProvider {
    private PicocliCommandsFactory factory;

    @Inject
    private Terminal terminal;

    public ReplExecutor get() {
        return new ReplExecutor(factory, terminal);
    }

    public void injectFactory(MicronautFactory micronautFactory) {
        this.factory = new PicocliCommandsFactory(micronautFactory);
        factory.setTerminal(terminal);
    }
}
