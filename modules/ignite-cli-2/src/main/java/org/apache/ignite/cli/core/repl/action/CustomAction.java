package org.apache.ignite.cli.core.repl.action;

import java.util.concurrent.Callable;
import org.apache.ignite.cli.commands.decorators.core.CommandOutput;
import picocli.CommandLine.Command;
import picocli.CommandLine.Unmatched;

@Command
public class CustomAction implements Callable<CommandOutput> {
    @Unmatched
    private String[] s;
    
    private final ReplFunc<String, ? extends CommandOutput> func;
    
    public CustomAction(ReplFunc<String, ? extends CommandOutput> func) {
        this.func = func;
    }
    
    
    @Override
    public CommandOutput call() throws Exception {
        return func.apply(s[0]);
    }
    
    public interface ReplFunc<T, R> {
        R apply(T t) throws Exception;
    }
}
