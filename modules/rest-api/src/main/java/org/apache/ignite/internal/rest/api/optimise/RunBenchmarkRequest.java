package org.apache.ignite.internal.rest.api.optimise;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

@Schema(description = "Run benchmark.")
public class RunBenchmarkRequest {
    private final String benchmarkFilePath;
    private final @Nullable String nodeName;

    /** Constructor. */
    @JsonCreator
    public RunBenchmarkRequest(
            @JsonProperty("nodeName") @Nullable String nodeName,
            @JsonProperty("benchmarkFilePath") String benchmarkFilePath
    ) {
        this.benchmarkFilePath = benchmarkFilePath;
        this.nodeName = nodeName;
    }

    @JsonGetter
    public String benchmarkFilePath() {
        return benchmarkFilePath;
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
