package org.apache.ignite.lang;

/**
 * Exception is thrown when appropriate index is not found.
 */
public class IndexNotFoundException extends IgniteException {
    /**
     * Create a new exception with given index name.
     *
     * @param name Column name.
     */
    public IndexNotFoundException(String name) {
        super(LoggerMessageHelper.format("Index not found [name={}]", name));
    }
}
