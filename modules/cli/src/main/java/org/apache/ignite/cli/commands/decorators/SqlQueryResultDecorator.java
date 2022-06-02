package org.apache.ignite.cli.commands.decorators;

import org.apache.ignite.cli.commands.decorators.core.Decorator;
import org.apache.ignite.cli.commands.decorators.core.TerminalOutput;
import org.apache.ignite.cli.sql.SqlQueryResult;

/**
 * Composite decorator for {@link SqlQueryResult}.
 */
public class SqlQueryResultDecorator implements Decorator<SqlQueryResult, TerminalOutput> {
    private final TableDecorator tableDecorator = new TableDecorator();
    private final DefaultDecorator<String> messageDecorator = new DefaultDecorator<>();

    @Override
    public TerminalOutput decorate(SqlQueryResult data) {
        return data.getResult(tableDecorator, messageDecorator);
    }
}
