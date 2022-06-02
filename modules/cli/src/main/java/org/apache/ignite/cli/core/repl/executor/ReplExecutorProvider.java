package org.apache.ignite.cli.core.repl.executor;

import io.micronaut.configuration.picocli.MicronautFactory;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.config.Config;
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
    @Inject
    private Config config;


    public ReplExecutor get() {
        return new ReplExecutor(factory, terminal, config);
    }


    public void injectFactory(MicronautFactory micronautFactory) {
        this.factory = new PicocliCommandsFactory(micronautFactory);
        factory.setTerminal(terminal);
    }
}
