package org.apache.ignite.cli.commands.sql;

import jakarta.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.SQLException;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.commands.decorators.SqlQueryResultDecorator;
import org.apache.ignite.cli.core.CallExecutionPipelineProvider;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.call.StringCallInput;
import org.apache.ignite.cli.core.exception.CommandExecutionException;
import org.apache.ignite.cli.core.exception.ExceptionHandlers;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.apache.ignite.cli.core.exception.handler.DefaultExceptionHandlers;
import org.apache.ignite.cli.core.exception.handler.SqlExceptionHandler;
import org.apache.ignite.cli.core.repl.Repl;
import org.apache.ignite.cli.core.repl.executor.RegistryCommandExecutor;
import org.apache.ignite.cli.core.repl.executor.ReplExecutorProvider;
import org.apache.ignite.cli.core.repl.executor.SqlQueryCall;
import org.apache.ignite.cli.sql.SqlManager;
import org.apache.ignite.cli.sql.SqlSchemaProvider;
import org.jetbrains.annotations.NotNull;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command for sql execution.
 */
@Command(name = "sql", description = "Executes SQL query.")
public class SqlCommand extends BaseCommand {
    private static final String INTERNAL_COMMAND_PREFIX = "!";

    @Option(names = {"-u", "--jdbc-url"}, required = true,
            descriptionKey = "ignite.jdbc-url", description = "JDBC url to ignite cluster")
    private String jdbc;
    @Option(names = {"-e", "--execute", "--exec"}) //todo: can be passed as parameter, not option (see IEP-88)
    private String command;
    @Option(names = {"-f", "--script-file"})
    private File file;

    @Inject
    private ReplExecutorProvider replExecutorProvider;

    private static String extract(File file) {
        try {
            return String.join("\n", Files.readAllLines(file.toPath(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new CommandExecutionException("sql", "File with command not found.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        try (SqlManager sqlManager = new SqlManager(jdbc)) {
            if (command == null && file == null) {
                replExecutorProvider.get().execute(Repl.builder()
                        .withPromptProvider(() -> "sql-cli> ")
                        .withCompleter(new SqlCompleter(new SqlSchemaProvider(sqlManager::getMetadata)))
                        .withCommandClass(SqlReplTopLevelCliCommand.class)
                        .withCallExecutionPipelineProvider(provider(sqlManager))
                        .withHistoryFileName("sqlhistory")
                        .build());
            } else {
                String executeCommand = file != null ? extract(file) : command;
                createSqlExecPipeline(sqlManager, executeCommand).runPipeline();
            }
        } catch (SQLException e) {
            new SqlExceptionHandler().handle(ExceptionWriter.fromPrintWriter(spec.commandLine().getErr()), e);
        }
    }

    @NotNull
    private CallExecutionPipelineProvider provider(SqlManager sqlManager) {
        return (call, exceptionHandlers, line) -> line.startsWith(INTERNAL_COMMAND_PREFIX)
                ? createInternalCommandPipeline(call, exceptionHandlers, line)
                : createSqlExecPipeline(sqlManager, line);
    }

    private CallExecutionPipeline<?, ?> createSqlExecPipeline(SqlManager sqlManager, String line) {
        return CallExecutionPipeline.builder(new SqlQueryCall(sqlManager))
                .inputProvider(() -> new StringCallInput(line))
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .exceptionHandlers(new DefaultExceptionHandlers())
                .decorator(new SqlQueryResultDecorator())
                .build();
    }

    private CallExecutionPipeline<?, ?> createInternalCommandPipeline(RegistryCommandExecutor call,
            ExceptionHandlers exceptionHandlers,
            String line) {
        return CallExecutionPipeline.builder(call)
                .inputProvider(() -> new StringCallInput(line.substring(INTERNAL_COMMAND_PREFIX.length())))
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .exceptionHandlers(new DefaultExceptionHandlers())
                .exceptionHandlers(exceptionHandlers)
                .build();
    }
}
