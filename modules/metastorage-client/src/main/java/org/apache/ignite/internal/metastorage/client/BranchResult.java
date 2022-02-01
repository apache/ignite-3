package org.apache.ignite.internal.metastorage.client;

import java.nio.ByteBuffer;

public class BranchResult {
    
    private final byte[] result;
    
    public BranchResult(boolean result) {
        this.result = new byte[] {(byte) (result ? 1 : 0)};
    }
    
    public BranchResult(int result) {
        this.result = ByteBuffer.allocate(4).putInt(result).array();
    }
    
    public boolean getAsBoolean() {
        return result[0] != 0;
    }
    
    public Integer getAsInt() {
        return ByteBuffer.wrap(result).getInt();
    }
    
    public static BranchResult res(boolean r) {
        return new BranchResult(r);
    }
}
