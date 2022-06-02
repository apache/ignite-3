package org.apache.ignite.cli.commands.decorators;

import org.apache.ignite.cli.commands.decorators.core.Decorator;
import org.apache.ignite.cli.commands.decorators.core.TerminalOutput;

/**
 * Default decorator that calls toString method.
 *
 * @param <I> Input type.
 */
public class DefaultDecorator<I> implements Decorator<I, TerminalOutput> {

    /** {@inheritDoc} */
    @Override
    public TerminalOutput decorate(I data) {
        return data::toString;
    }
}
