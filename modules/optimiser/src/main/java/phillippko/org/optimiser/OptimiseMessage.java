package phillippko.org.optimiser;

import java.util.UUID;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;

@Transferable(OptimiserMessageGroup.OPTIMISE_MESSAGE_TYPE)
interface OptimiseMessage extends NetworkMessage {
    boolean writeIntensive();

    UUID id();
}
