package org.apache.ignite.lang;

/**
 * An interface describing a factory to create a logger instance.
 */
@FunctionalInterface
public interface LoggerFactory {
    /**
     * Creates a logger instance with a given name.
     *
     * @param name Name to create logger with.
     * @return Logger instance.
     */
    System.Logger forName(String name);
}
