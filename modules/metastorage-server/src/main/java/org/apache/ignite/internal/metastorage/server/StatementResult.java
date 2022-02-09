package org.apache.ignite.internal.metastorage.server;

import java.nio.ByteBuffer;

public class StatementResult {
    
    private final byte[] result;
    
    public StatementResult(boolean result) {
        this.result = new byte[] {(byte) (result ? 1 : 0)};
    }
    
    public StatementResult(int result) {
        this.result = ByteBuffer.allocate(4).putInt(result).array();
    }
    
    public boolean getAsBoolean() {
        return result[0] != 0;
    }
    
    public Integer getAsInt() {
        return ByteBuffer.wrap(result).getInt();
    }
    
    public byte[] getAsBytes() {
        return result;
    }
    
    public static StatementResult res(boolean r) {
        return new StatementResult(r);
    }
}
