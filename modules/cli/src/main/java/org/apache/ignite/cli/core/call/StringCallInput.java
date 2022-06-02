package org.apache.ignite.cli.core.call;

/**
 * Input for executing commands with {@code String} arguments.
 */
public class StringCallInput implements CallInput {
    private final String string;

    public StringCallInput(String string) {
        this.string = string;
    }

    /**
     * Argument getter.
     *
     * @return {@code String} argument.
     */
    public String getString() {
        return string;
    }
}
