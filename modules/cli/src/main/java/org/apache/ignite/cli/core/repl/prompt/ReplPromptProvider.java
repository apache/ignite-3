package org.apache.ignite.cli.core.repl.prompt;

import jakarta.inject.Singleton;
import org.apache.ignite.cli.core.repl.Session;

/**
 * Provider for prompt in REPL.
 */
@Singleton
public class ReplPromptProvider implements PromptProvider {
    private final Session session;

    public ReplPromptProvider(Session session) {
        this.session = session;
    }

    /**
     * Return prompt.
     */
    @Override
    public String getPrompt() {
        return session.isConnectedToNode()
                ? "@|fg(10) [" + session.getNodeUrl() + "]|@> "
                : "@|fg(9) [disconnected]|@> ";
    }
}
