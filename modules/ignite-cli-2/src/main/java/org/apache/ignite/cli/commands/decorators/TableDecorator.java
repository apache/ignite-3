package org.apache.ignite.cli.commands.decorators;

import com.jakewharton.fliptables.FlipTableConverters;
import org.apache.ignite.cli.commands.decorators.core.Decorator;
import org.apache.ignite.cli.commands.decorators.core.TerminalOutput;
import org.apache.ignite.cli.sql.table.Table;

public class TableDecorator implements Decorator<Table<String>, TerminalOutput> {
    
    @Override
    public TerminalOutput decorate(Table<String> table) {
        return () -> FlipTableConverters.fromIterable(table, String.class);
    }
}
