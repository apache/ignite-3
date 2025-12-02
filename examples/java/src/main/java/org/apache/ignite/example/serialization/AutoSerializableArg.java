package org.apache.ignite.example.serialization;

/**
 * Argument object used in auto-serialization examples.
 */
public class AutoSerializableArg {

    /** Word to process. */
    String word;

    /** Whether the word should be converted to upper case. */
    boolean isUpperCase;

    /** Default constructor. */
    public AutoSerializableArg() {
    }

    /**
     * Creates a new argument object.
     *
     * @param word Word value.
     * @param isUpperCase Flag indicating upper-case conversion.
     */
    AutoSerializableArg(String word, boolean isUpperCase) {
        this.word = word;
        this.isUpperCase = isUpperCase;
    }
}
