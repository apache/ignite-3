package org.apache.ignite.cli.core.exception.handler;

import org.apache.ignite.cli.core.exception.ExceptionHandlers;
import picocli.CommandLine;
import picocli.CommandLine.IExecutionExceptionHandler;
import picocli.CommandLine.ParseResult;

/**
 * Implementation of {@link IExecutionExceptionHandler} based on {@link ExceptionHandlers}.
 */
public class PicocliExecutionExceptionHandler implements IExecutionExceptionHandler {
    private final ExceptionHandlers exceptionHandlers = new DefaultExceptionHandlers();

    @Override
    public int handleExecutionException(Exception ex, CommandLine commandLine, ParseResult parseResult) {
        exceptionHandlers.handleException(System.err::println, ex);
        return 1;
    }
}
