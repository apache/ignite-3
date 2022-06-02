package org.apache.ignite.cli.core.repl.executor;


import java.util.function.Function;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.core.call.StringCallInput;
import org.jline.console.CmdDesc;
import org.jline.console.CmdLine;
import org.jline.console.SystemRegistry;
import org.jline.reader.LineReader;
import org.jline.widget.TailTipWidgets;
import picocli.shell.jline3.PicocliCommands;

/**
 * Command executor based on {@link SystemRegistry}.
 */
public class RegistryCommandExecutor implements Call<StringCallInput, Object> {
    private final SystemRegistry systemRegistry;

    /**
     * Constructor.
     *
     * @param systemRegistry {@link SystemRegistry} instance.
     * @param picocliCommands {@link PicocliCommands} instance.
     * @param reader {@link LineReader} instance.
     */
    public RegistryCommandExecutor(SystemRegistry systemRegistry,
                                   PicocliCommands picocliCommands,
                                   LineReader reader) {
        this(systemRegistry, picocliCommands, reader, systemRegistry::commandDescription);
    }

    /**
     * Constructor.
     *
     * @param systemRegistry {@link SystemRegistry} instance.
     * @param picocliCommands {@link PicocliCommands} instance.
     * @param reader {@link LineReader} instance.
     * @param descFunc function that returns command description.
     */
    public RegistryCommandExecutor(SystemRegistry systemRegistry,
                                   PicocliCommands picocliCommands,
                                   LineReader reader,
                                   Function<CmdLine, CmdDesc> descFunc) {
        this.systemRegistry = systemRegistry;
        systemRegistry.register("help", picocliCommands);
        if (descFunc != null) {
            TailTipWidgets widgets = new TailTipWidgets(reader, descFunc, 5,
                    TailTipWidgets.TipType.COMPLETER);
            widgets.enable();
        }
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

    /**
     * Trace exception.
     *
     * @param e exception instance.
     */
    public void trace(Exception e) {
        systemRegistry.trace(e);
    }
}
