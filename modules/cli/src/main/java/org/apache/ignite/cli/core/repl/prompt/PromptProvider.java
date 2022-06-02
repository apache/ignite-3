package org.apache.ignite.cli.core.repl.prompt;

/**
 * Interface for prompt provider.
 */
public interface PromptProvider {
    /**
     * Returns prompt string.
     */
    String getPrompt();
}
