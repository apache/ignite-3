package org.apache.ignite.cli.commands.sql;

import jakarta.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.Callable;
import org.apache.ignite.cli.commands.decorators.core.DecoratorStorage;
import org.apache.ignite.cli.core.CliManager;
import org.apache.ignite.cli.core.repl.Repl;
import org.apache.ignite.cli.core.repl.executor.SqlReplCommandExecutor;
import org.apache.ignite.cli.sql.SqlManager;
import org.apache.ignite.cli.sql.table.Table;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "sql")
public class SqlReplCommand implements Callable<Table<String>> {
    @Inject
    private CliManager cliManager;
    @Inject
    private DecoratorStorage storage;
    
    @Option(names = {"--jdbc-url"}, required = true)
    private String jdbc;
    @Option(names = {"-execute", "--execute"})
    private String command;
    @Option(names = {"--script-file"})
    private File file;
    
    @Override
    public Table<String> call() throws Exception {
        try (SqlManager sqlManager = new SqlManager(jdbc)) {
            if (command == null && file == null) {
                cliManager.enableRepl(Repl.builder()
                        .withName("sql-cli")
                        .withCustomAction(sqlManager::executeSql)
                        .withCommandExecutorProvider(SqlReplCommandExecutor::new)
                        .build());
            } else {
                String executeCommand = file != null ? extract(file) : command;
                return sqlManager.executeSql(executeCommand);
            }
            return null;
        }
    }
    
    private static String extract(File file) throws IOException {
        return String.join("\n", Files.readAllLines(file.toPath(), StandardCharsets.UTF_8));
    }
}
