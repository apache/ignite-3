package org.apache.ignite.internal.metastorage.client;

import java.nio.ByteBuffer;

/**
 * Simple result of statement execution, backed by byte[] array.
 * Provides some shortcut methods to represent the values of some primitive types.
 */
public class StatementResult {

    /** Result data. */
    private final byte[] result;

    /**
     * Constructs result from the byte array.
     *
     * @param result byte array.
     */
    public StatementResult(byte[] result) {
        this.result = result;
    }

    /**
     * Constructs result from the boolean value.
     *
     * @param result boolean.
     */
    public StatementResult(boolean result) {
        this.result = new byte[] {(byte) (result ? 1 : 0)};
    }

    /**
     * Constructs result from the int value.
     *
     * @param result int.
     */
    public StatementResult(int result) {
        this.result = ByteBuffer.allocate(4).putInt(result).array();
    }

    /**
     * Returns result value as a boolean.
     *
     * @return boolean result.
     */
    public boolean getAsBoolean() {
        return result[0] != 0;
    }

    /**
     * Returns result as an int.
     *
     * @return int result.
     */
    public Integer getAsInt() {
        return ByteBuffer.wrap(result).getInt();
    }

    /**
     * Returns backed byte array.
     *
     * @return backed byte array.
     */
    public byte[] bytes() {
        return result;
    }
}
