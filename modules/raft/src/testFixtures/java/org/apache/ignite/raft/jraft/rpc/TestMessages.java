package org.apache.ignite.raft.jraft.rpc;

import org.apache.ignite.network.annotations.Transferable;

/** Messages used in AbstractRpcTest. */
public class TestMessages {
    /**  */
    @Transferable(value = 0)
    public interface Request1 extends Message {
        /**  */
        int val();
    }

    /**  */
    @Transferable(value = 1)
    public interface Request2 extends Message {
        /**  */
        int val();
    }

    /**  */
    @Transferable(value = 2)
    public interface Response1 extends Message {
        /**  */
        int val();
    }

    /**  */
    @Transferable(value = 3)
    public interface Response2 extends Message {
        /**  */
        int val();
    }
}
