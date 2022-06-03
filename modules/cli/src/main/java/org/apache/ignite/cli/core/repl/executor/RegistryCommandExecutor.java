package org.apache.ignite.cli.core.repl.executor;


import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.core.call.StringCallInput;
import org.jline.console.SystemRegistry;

/**
 * Command executor based on {@link SystemRegistry}.
 */
public class RegistryCommandExecutor implements Call<StringCallInput, Object> {
    private final SystemRegistry systemRegistry;

    /**
     * Constructor.
     *
     * @param systemRegistry {@link SystemRegistry} instance.
     */
    public RegistryCommandExecutor(SystemRegistry systemRegistry) {
        this.systemRegistry = systemRegistry;
    }

    /**
     * Executor method.
     *
     * @param input processed command line.
     * @return Command output.
     */
    @Override
    public CallOutput<Object> execute(StringCallInput input) {
        try {
            Object executionResult = systemRegistry.execute(input.getString());
            if (executionResult == null) {
                return DefaultCallOutput.empty();
            }

            return DefaultCallOutput.success(executionResult);
        } catch (Exception e) {
            return DefaultCallOutput.failure(e);
        }
    }

    /**
     * Clean up {@link SystemRegistry}.
     */
    public void cleanUp() {
        systemRegistry.cleanUp();
    }
}
