package org.apache.ignite.cli.commands.decorators;

import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.ignite.cli.commands.decorators.core.Decorator;
import org.apache.ignite.cli.commands.decorators.core.TerminalOutput;
import org.apache.ignite.cli.config.Config;

/**
 * Decorator for printing {@link Config}.
 */
public class ConfigDecorator implements Decorator<Config, TerminalOutput> {
    @Override
    public TerminalOutput decorate(Config data) {
        StringBuilder builder = new StringBuilder();
        for (Iterator<Entry<Object, Object>> iterator = data.getProperties().entrySet().iterator(); iterator.hasNext(); ) {
            Entry<Object, Object> entry = iterator.next();
            builder.append(entry.getKey()).append("=").append(entry.getValue());
            if (iterator.hasNext()) {
                builder.append(System.lineSeparator());
            }
        }
        return builder::toString;
    }
}
