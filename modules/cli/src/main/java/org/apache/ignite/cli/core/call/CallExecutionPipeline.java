package org.apache.ignite.cli.core.call;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.function.Supplier;
import org.apache.ignite.cli.commands.decorators.DefaultDecorator;
import org.apache.ignite.cli.commands.decorators.core.Decorator;
import org.apache.ignite.cli.commands.decorators.core.TerminalOutput;
import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionHandlers;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.apache.ignite.cli.core.exception.handler.DefaultExceptionHandlers;

/**
 * Call execution pipeline.
 *
 * @param <I> Call input type.
 * @param <T> Call output's body type.
 */
public class CallExecutionPipeline<I extends CallInput, T> {
    /**
     * Call to execute.
     */
    private final Call<I, T> call;
    /**
     * Writer for execution output.
     */
    private final PrintWriter output;
    /**
     * Writer for error execution output.
     */
    private final PrintWriter errOutput;
    /**
     * Decorator that decorates call's output.
     */
    private final Decorator<T, TerminalOutput> decorator;
    /**
     * Handlers for any exceptions.
     */
    private final ExceptionHandlers exceptionHandlers;
    /**
     * Provider for call's input.
     */
    private final Supplier<I> inputProvider;

    private CallExecutionPipeline(Call<I, T> call,
            PrintWriter output,
            PrintWriter errOutput,
            ExceptionHandlers exceptionHandlers,
            Decorator<T, TerminalOutput> decorator,
            Supplier<I> inputProvider) {
        this.call = call;
        this.output = output;
        this.exceptionHandlers = exceptionHandlers;
        this.errOutput = errOutput;
        this.decorator = decorator;
        this.inputProvider = inputProvider;
    }

    /**
     * Builder helper method.
     *
     * @return builder for {@link CallExecutionPipeline}.
     */
    public static <I extends CallInput, T> CallExecutionPipelineBuilder<I, T> builder(
            Call<I, T> call) {
        return new CallExecutionPipelineBuilder<>(call);
    }

    /** {@inheritDoc} */
    public void runPipeline() {
        I callInput = inputProvider.get();

        CallOutput<T> callOutput = call.execute(callInput);

        if (callOutput.hasError()) {
            exceptionHandlers.handleException(ExceptionWriter.fromPrintWriter(errOutput), callOutput.errorCause());
            return;
        }

        if (callOutput.isEmpty()) {
            return;
        }

        TerminalOutput decoratedOutput = decorator.decorate(callOutput.body());

        output.println(decoratedOutput.toTerminalString());
    }

    /** Builder for {@link CallExecutionPipeline}. */
    public static class CallExecutionPipelineBuilder<I extends CallInput, T> {

        private final Call<I, T> call;
        private final ExceptionHandlers exceptionHandlers = new DefaultExceptionHandlers();
        private Supplier<I> inputProvider;
        private PrintWriter output = wrapOutputStream(System.out);
        private PrintWriter errOutput = wrapOutputStream(System.err);
        private Decorator<T, TerminalOutput> decorator = new DefaultDecorator<>();

        public CallExecutionPipelineBuilder(Call<I, T> call) {
            this.call = call;
        }

        public CallExecutionPipelineBuilder<I, T> inputProvider(Supplier<I> inputProvider) {
            this.inputProvider = inputProvider;
            return this;
        }

        public CallExecutionPipelineBuilder<I, T> output(PrintWriter output) {
            this.output = output;
            return this;
        }

        public CallExecutionPipelineBuilder<I, T> output(OutputStream output) {
            return output(wrapOutputStream(output));
        }

        public CallExecutionPipelineBuilder<I, T> errOutput(PrintWriter errOutput) {
            this.errOutput = errOutput;
            return this;
        }

        public CallExecutionPipelineBuilder<I, T> errOutput(OutputStream output) {
            return errOutput(wrapOutputStream(output));
        }

        public CallExecutionPipelineBuilder<I, T> exceptionHandler(ExceptionHandler<?> exceptionHandler) {
            exceptionHandlers.addExceptionHandler(exceptionHandler);
            return this;
        }

        public CallExecutionPipelineBuilder<I, T> exceptionHandlers(ExceptionHandlers exceptionHandlers) {
            this.exceptionHandlers.addExceptionHandlers(exceptionHandlers);
            return this;
        }

        public CallExecutionPipelineBuilder<I, T> decorator(Decorator<T, TerminalOutput> decorator) {
            this.decorator = decorator;
            return this;
        }

        public CallExecutionPipeline<I, T> build() {
            return new CallExecutionPipeline<>(call, output, errOutput, exceptionHandlers, decorator, inputProvider);
        }

        private static PrintWriter wrapOutputStream(OutputStream output) {
            return new PrintWriter(output, true, getStdoutEncoding());
        }

        private static Charset getStdoutEncoding() {
            String encoding = System.getProperty("sun.stdout.encoding");
            return encoding != null ? Charset.forName(encoding) : Charset.defaultCharset();
        }
    }
}
