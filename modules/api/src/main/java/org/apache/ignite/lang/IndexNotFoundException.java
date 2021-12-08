package org.apache.ignite.lang;

/**
 * Exception is thrown when appropriate index is not found.
 */
public class IndexNotFoundException extends IgniteException {
    /**
     * Create a new exception with given index name.
     *
     * @param indexName Index name.
     */
    public IndexNotFoundException(String indexName) {
        super(LoggerMessageHelper.format("Index '{}' does not exist.", indexName));
    }
}
