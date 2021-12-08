package org.apache.ignite.lang;

/**
 * Exception is thrown when appropriate index is not found.
 */
public class IndexNotFoundException extends IgniteException {
    /**
     * Create a new exception with given index name.
     *
     * @param name Column name.
     * @param fullName Schema and table name.
     */
    public IndexNotFoundException(String name, String fullName) {
        super(LoggerMessageHelper.format("Index '{}' does not exist in table '{}'", name));
    }
}
