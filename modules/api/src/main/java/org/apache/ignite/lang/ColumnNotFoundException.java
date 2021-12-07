package org.apache.ignite.lang;

/**
 * Exception is thrown when appropriate column is not found.
 */
public class ColumnNotFoundException extends IgniteException {
    /**
     * Create a new exception with given column name.
     *
     * @param name Column name.
     */
    public ColumnNotFoundException(String name) {
        super(LoggerMessageHelper.format("Column not found [name={}]", name));
    }
}
