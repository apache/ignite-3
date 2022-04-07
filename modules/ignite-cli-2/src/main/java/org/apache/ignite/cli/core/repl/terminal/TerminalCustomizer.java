package org.apache.ignite.cli.core.repl.terminal;

import org.jline.terminal.Terminal;

/**
 * Interface for customize provided instance of {@link Terminal}
 */
public interface TerminalCustomizer {
    /**
     * Customize method
     * @param terminal provided terminal instance
     */
    void customize(Terminal terminal);
}
