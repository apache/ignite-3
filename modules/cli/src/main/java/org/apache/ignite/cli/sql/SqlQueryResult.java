package org.apache.ignite.cli.sql;

import org.apache.ignite.cli.commands.decorators.TableDecorator;
import org.apache.ignite.cli.commands.decorators.core.Decorator;
import org.apache.ignite.cli.commands.decorators.core.TerminalOutput;
import org.apache.ignite.cli.sql.table.Table;

/**
 * Composite object of sql query result.
 */
public class SqlQueryResult {
    private final Table<String> table;
    private final String message;

    /**
     * Constructor.
     *
     * @param table non null result table.
     */
    public SqlQueryResult(Table<String> table) {
        this(table, null);
    }

    /**
     * Constructor.
     *
     * @param message non null result message.
     */
    public SqlQueryResult(String message) {
        this(null, message);
    }

    private SqlQueryResult(Table<String> table, String message) {
        this.table = table;
        this.message = message;
    }

    /**
     * SQL query result provider.
     *
     * @param tableDecorator instance of {@link TableDecorator}.
     * @param messageDecorator decorator of message.
     * @return terminal output of non-null field of class.
     */
    public TerminalOutput getResult(TableDecorator tableDecorator,
                                    Decorator<String, TerminalOutput> messageDecorator) {
        if (table != null) {
            return tableDecorator.decorate(table);
        }
        return messageDecorator.decorate(message);
    }
}
