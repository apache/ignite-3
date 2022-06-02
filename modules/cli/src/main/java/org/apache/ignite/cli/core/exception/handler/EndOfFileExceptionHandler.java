package org.apache.ignite.cli.core.exception.handler;

import java.util.function.Consumer;
import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;

/**
 * Exception handler for {@link EndOfFileException}.
 * From {@link EndOfFileException}
 * <p>
 *     This exception is thrown by {@link LineReader#readLine} when user the user types ctrl-D)
 * </p>
 * This handler call {@param endAction} to stop some process.
 */
public class EndOfFileExceptionHandler implements ExceptionHandler<EndOfFileException> {
    private final Consumer<Boolean> endAction;

    /**
     * Constructor.
     *
     * @param endAction handle action.
     */
    public EndOfFileExceptionHandler(Consumer<Boolean> endAction) {
        this.endAction = endAction;
    }

    @Override
    public void handle(ExceptionWriter err, EndOfFileException e) {
        endAction.accept(true);
    }

    @Override
    public Class<EndOfFileException> applicableException() {
        return EndOfFileException.class;
    }
}
