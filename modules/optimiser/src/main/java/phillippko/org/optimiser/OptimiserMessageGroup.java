package phillippko.org.optimiser;

import static phillippko.org.optimiser.OptimiserMessageGroup.GROUP_TYPE;

import org.apache.ignite.internal.network.annotations.MessageGroup;

@MessageGroup(groupType = GROUP_TYPE, groupName = "OptimiserMessage")
public interface OptimiserMessageGroup {
    short GROUP_TYPE = 17;
    short RUN_BENCHMARK_TYPE = 0;
    short OPTIMISE_MESSAGE_TYPE = 1;
}
