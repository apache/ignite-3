package org.apache.ignite.cli.commands.decorators;

import com.jakewharton.fliptables.FlipTableConverters;
import org.apache.ignite.cli.commands.decorators.core.Decorator;
import org.apache.ignite.cli.commands.decorators.core.TerminalOutput;
import org.apache.ignite.cli.sql.table.Table;

/**
 * Implementation of {@link Decorator} for {@link Table}.
 */
public class TableDecorator implements Decorator<Table<String>, TerminalOutput> {

    /**
     * Transform {@link Table} to {@link TerminalOutput}.
     *
     * @param table incoming {@link Table}.
     * @return User friendly interpretation of {@link Table} in {@link TerminalOutput}.
     */
    @Override
    public TerminalOutput decorate(Table<String> table) {
        return () -> FlipTableConverters.fromObjects(table.header(), table.content());
    }
}
