package org.apache.ignite.internal.replicator.exception;

import org.apache.ignite.lang.ErrorGroups.Replicator;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.network.NetworkAddress;

/**
 * The exception is thrown when the replica is not the current primary replica.
 */
public class PrimaryReplicaMissException extends IgniteInternalException {
    /**
     * The constructor.
     *
     * @param requestAddress Primary replica address from request.
     * @param localAddress Local primary replica address.
     * @param requestTerm Raft term from request.
     * @param localTerm Local raft term.
     */
    public PrimaryReplicaMissException(NetworkAddress requestAddress, NetworkAddress localAddress, Long requestTerm, Long localTerm) {
        this(requestAddress, localAddress, requestTerm, localTerm, null);
    }

    /**
     * The constructor.
     *
     * @param requestAddress Primary replica address from request.
     * @param localAddress Local primary replica address.
     * @param requestTerm Raft term from request.
     * @param localTerm Local raft term.
     * @param cause Cause exception.
     */
    public PrimaryReplicaMissException(NetworkAddress requestAddress, NetworkAddress localAddress, Long requestTerm, Long localTerm,
            Throwable cause) {
        super(Replicator.REPLICA_MISS_ERR, IgniteStringFormatter.format("The replica is not the current primary replica [requestAddress={}, " +
                "localAddress={}, requestTerm={}, localTerm={}]", requestAddress, localAddress, requestTerm, localTerm),
                cause);
    }
}
