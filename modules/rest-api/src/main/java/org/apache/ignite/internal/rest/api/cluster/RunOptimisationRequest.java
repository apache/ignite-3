package org.apache.ignite.internal.rest.api.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.micronaut.context.annotation.Any;
import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/** Disaster recovery request to restart partitions. */
@Schema(description = "restart partitions configuration.")
public class RunOptimisationRequest {
    @Schema(description = "If target configuration should be optimised for write-intensive use-cases.")
    private final boolean writeIntensive;
    @Schema(description = "Name of the node to run optimisation on.")
    private final @Nullable String nodeName;

    /** Constructor. */
    @JsonCreator
    public RunOptimisationRequest(
            @JsonProperty("nodeName") @Nullable String nodeName,
            @JsonProperty("writeIntensive") boolean writeIntensive
    ) {
        this.writeIntensive = writeIntensive;
        this.nodeName = nodeName;
    }

    @JsonGetter
    public boolean writeIntensive() {
        return writeIntensive;
    }

    @JsonGetter
    @Nullable
    public String nodeName() {
        return nodeName;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
