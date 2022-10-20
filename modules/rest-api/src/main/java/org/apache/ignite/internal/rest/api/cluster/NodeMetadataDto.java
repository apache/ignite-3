package org.apache.ignite.internal.rest.api.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;


@Schema(name = "NodeMetadata")
public class NodeMetadataDto {
    private final int restPort;

    @JsonCreator
    public NodeMetadataDto(@JsonProperty("restPort") int restPort) {
        this.restPort = restPort;
    }

    @JsonGetter("restPort")
    public int getRestPort() {
        return restPort;
    }
}
