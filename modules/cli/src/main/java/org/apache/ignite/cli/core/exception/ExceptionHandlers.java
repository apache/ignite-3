package org.apache.ignite.cli.core.exception;

import java.util.HashMap;
import java.util.Map;

/**
 * Exception handlers collector class.
 */
public class ExceptionHandlers {
    private final Map<Class<? extends Throwable>, ExceptionHandler<? extends Throwable>> map = new HashMap<>();
    private final ExceptionHandler<Throwable> defaultHandler;

    public ExceptionHandlers() {
        this(ExceptionHandler.DEFAULT);
    }

    public ExceptionHandlers(ExceptionHandler<Throwable> defaultHandler) {
        this.defaultHandler = defaultHandler;
    }

    /**
     * Appends exception handler to collection.
     * If collection already contains handler for type {@param <T>} it will be replaced by {@param exceptionHandler}.
     *
     * @param exceptionHandler handler instance.
     * @param <T> exception type.
     */
    public <T extends Throwable> void addExceptionHandler(ExceptionHandler<T> exceptionHandler) {
        map.put(exceptionHandler.applicableException(), exceptionHandler);
    }

    /**
     * Append all exception handlers.
     *
     * @param exceptionHandlers handlers to append.
     */
    public void addExceptionHandlers(ExceptionHandlers exceptionHandlers) {
        map.putAll(exceptionHandlers.map);
    }

    /**
     * Handle method.
     *
     * @param errOutput error output.
     * @param e exception instance.
     * @param <T> exception type.
     */
    public <T extends Throwable> void handleException(ExceptionWriter errOutput, T e) {
        processException(errOutput, e instanceof WrappedException ? e.getCause() : e);
    }

    @SuppressWarnings("unchecked")
    private <T extends Throwable> void processException(ExceptionWriter errOutput, T e) {
        ExceptionHandler<T> exceptionHandler = (ExceptionHandler<T>) map.get(e.getClass());
        if (exceptionHandler != null) {
            exceptionHandler.handle(errOutput, e);
        } else {
            defaultHandler.handle(errOutput, e);
        }
    }

}
