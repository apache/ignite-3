package org.apache.ignite.internal.logger;

import java.util.Objects;
import org.apache.ignite.lang.LoggerFactory;

/**
 * This class contains different static factory methods to create an instance of logger.
 */
public final class Loggers {
    /**
     * Creates logger for given class with system logger as a backend.
     *
     * <p>Note: this method should be used to create a server-side logger since user might provide a custom backend for
     * every particular client instance.
     *
     * @param cls The class for a logger.
     * @return Ignite logger.
     */
    public static IgniteLogger forClass(Class<?> cls) {
        return forName(Objects.requireNonNull(cls, "cls").getName());
    }

    /**
     * Creates logger for given class with backend created by the given loggerFactory.
     *
     * @param cls The class for a logger.
     * @param loggerFactory Logger factory to create backend for the logger.
     * @return Ignite logger.
     */
    public static IgniteLogger forClass(Class<?> cls, LoggerFactory loggerFactory) {
        return forName(Objects.requireNonNull(cls, "cls").getName(), loggerFactory);
    }

    /**
     * Creates logger for given class with system logger as a backend.
     *
     * <p>Note: this method should be used to create a server-side logger since user might provide a custom backend for
     * every particular client instance.
     *
     * @param name The name for a logger.
     * @return Ignite logger.
     */
    public static IgniteLogger forName(String name) {
        var delegate = System.getLogger(name);

        return new IgniteLogger(delegate);
    }

    /**
     * Creates logger for given class with backend created by the given loggerFactory.
     *
     * @param name The name for a logger.
     * @param loggerFactory Logger factory to create backend for the logger.
     * @return Ignite logger.
     */
    public static IgniteLogger forName(String name, LoggerFactory loggerFactory) {
        var delegate = Objects.requireNonNull(loggerFactory, "loggerFactory").forName(name);

        return new IgniteLogger(delegate);
    }

    /**
     * Creates the logger which outputs nothing.
     *
     * @return Void logger.
     */
    public static IgniteLogger voidLogger() {
        return new VoidLogger();
    }
}
