package org.apache.ignite.cli.core;

import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.exception.ExceptionHandlers;
import org.apache.ignite.cli.core.repl.executor.RegistryCommandExecutor;

/**
 * Provider of {@link CallExecutionPipeline}.
 */
public interface CallExecutionPipelineProvider {
    /**
     * Pipeline getter.
     *
     * @param executor default executor.
     * @param exceptionHandlers exception handlers.
     * @param line call input.
     * @return new pipeline {@link CallExecutionPipeline}
     */
    CallExecutionPipeline<?, ?> get(RegistryCommandExecutor executor, ExceptionHandlers exceptionHandlers, String line);
}
