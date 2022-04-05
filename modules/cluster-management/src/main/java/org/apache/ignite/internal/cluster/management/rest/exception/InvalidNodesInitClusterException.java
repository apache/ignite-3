package org.apache.ignite.internal.cluster.management.rest.exception;

/**
 * Exception that is thrown when the wrong node names is passed to the init cluster method.
 */
public class InvalidNodesInitClusterException extends RuntimeException {
    public InvalidNodesInitClusterException(Throwable cause) {
        super(cause);
    }
}
