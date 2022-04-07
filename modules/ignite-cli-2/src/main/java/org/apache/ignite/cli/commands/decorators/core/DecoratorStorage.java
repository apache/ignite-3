package org.apache.ignite.cli.commands.decorators.core;

import jakarta.inject.Singleton;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
public class DecoratorStorage {
    private final Map<Class<?>, Decorator<? extends CommandOutput, ? extends TerminalOutput>> commandDecorators = new ConcurrentHashMap<>();
    
    public void register(Class<?> commandClass, Decorator<? extends CommandOutput, ? extends TerminalOutput> decorator) {
        commandDecorators.put(commandClass, decorator);
    }
    
    public Decorator<? extends CommandOutput, ? extends TerminalOutput> getDecorator(Class<?> commandClass) {
        return commandDecorators.getOrDefault(commandClass, date -> date::get);
    }
}
