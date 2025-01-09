package org.phillippko.ignite.internal.optimiser;

import java.util.UUID;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;

@Transferable(OptimiserMessageGroup.RUN_BENCHMARK_TYPE)
interface RunBenchmarkMessage extends NetworkMessage {
    String benchmarkFileName();

    UUID id();
}
