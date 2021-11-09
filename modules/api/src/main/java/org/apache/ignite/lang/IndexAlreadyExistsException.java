package org.apache.ignite.lang;

/**
 * This exception is thrown when a new index failed to be created, because another index with the same name already exists.
 */
public class IndexAlreadyExistsException extends IgniteException {
    /**
     * Create a new exception with given index name.
     *
     * @param name Table name.
     */
    public IndexAlreadyExistsException(String name) {
        super(LoggerMessageHelper.format("Index already exists [name={}]", name));
    }
}
