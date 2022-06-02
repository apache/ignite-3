package org.apache.ignite.cli.core.repl.terminal;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import java.io.IOException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

/**
 * Factory for producing JLine {@link Terminal} instances.
 */
@Factory
public class TerminalFactory {
    /**
     * Produce terminal instances.
     *
     * <p>Important: It's always must be a singleton bean. JLine has an issues with building more than 1 terminal instance per process.
     *
     * @return Terminal instance.
     * @throws IOException if an error occurs.
     */
    @Bean(preDestroy = "close")
    @Singleton
    public Terminal terminal() throws IOException {
        return TerminalBuilder.terminal();
    }
}
