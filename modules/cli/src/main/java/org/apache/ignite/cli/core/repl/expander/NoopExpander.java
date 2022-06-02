package org.apache.ignite.cli.core.repl.expander;

import org.jline.reader.Expander;
import org.jline.reader.History;

/**
 * Noop implementation of {@link Expander}.
 */
public class NoopExpander implements Expander {
    /** {@inheritDoc} */
    @Override
    public String expandHistory(History history, String line) {
        return line;
    }

    /** {@inheritDoc} */
    @Override
    public String expandVar(String word) {
        return word;
    }
}
