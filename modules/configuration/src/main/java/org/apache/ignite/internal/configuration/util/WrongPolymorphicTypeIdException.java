package org.apache.ignite.internal.configuration.util;

/**
 * Thrown when the wrong (unknown) type of polymorphic configuration is used.
 */
public class WrongPolymorphicTypeIdException extends RuntimeException {
    /**
     * Constructor.
     *
     * @param message Error message.
     */
    public WrongPolymorphicTypeIdException(String message) {
        super(message);
    }
}
