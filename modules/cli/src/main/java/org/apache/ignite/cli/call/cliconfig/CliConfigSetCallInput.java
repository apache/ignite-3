package org.apache.ignite.cli.call.cliconfig;

import java.util.Map;
import org.apache.ignite.cli.core.call.CallInput;

/**
 * Input for {@link CliConfigSetCall}.
 */
public class CliConfigSetCallInput implements CallInput {
    private final Map<String, String> parameters;

    public CliConfigSetCallInput(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }
}
