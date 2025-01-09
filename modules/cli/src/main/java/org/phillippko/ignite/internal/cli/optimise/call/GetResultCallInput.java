package org.phillippko.ignite.internal.cli.optimise.call;

import java.util.UUID;
import org.apache.ignite.internal.cli.core.call.CallInput;

public class GetResultCallInput implements CallInput {
    private final String clusterUrl;

    private final UUID id;

    public GetResultCallInput(String clusterUrl, UUID id) {
        this.clusterUrl = clusterUrl;
        this.id = id;
    }

    public static GetResultInputBuilder builder() {
        return new GetResultInputBuilder();
    }

    public String getClusterUrl() {
        return clusterUrl;
    }

    public UUID getId() {
        return id;
    }

    public static class GetResultInputBuilder {
        private String clusterUrl;
        private UUID id;

        public GetResultInputBuilder setClusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;

            return this;
        }

        public GetResultInputBuilder setId(UUID id) {
            this.id = id;

            return this;
        }

        public GetResultCallInput build() {
            return new GetResultCallInput(clusterUrl, id);
        }
    }
}
