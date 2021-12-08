package org.apache.ignite.lang;

/**
 * Exception is thrown when appropriate column is not found.
 */
public class ColumnNotFoundException extends IgniteException {
    /**
     * Create a new exception with given column name.
     *
     * @param name Column name.
     * @param fullName Table canonical name.
     */
    public ColumnNotFoundException(String name, String fullName) {
        super(LoggerMessageHelper.format("Column '{}' does not exist in table '{}'", name, fullName));
    }
}
