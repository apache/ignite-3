/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.rest.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Collection;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.rest.constants.HttpCode;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Implements application/problem+json schema defined in <a href="https://www.rfc-editor.org/rfc/rfc7807.html">RFC-7807</a>.
 */
@Schema(description = "Extended description of the problem with the request.")
public class Problem {
    /** Short, human-readable summary of the problem type. */
    @Schema(description = "Short summary of the issue.")
    private final String title;

    /** HTTP status code. */
    @Schema(description = "Returned HTTP status code.")
    private final int status;

    /** Ignite 3 error code. */
    @Schema(description = "Ignite 3 error code.")
    private final String code;

    /** URI to the error documentation (optional). */
    @Schema(description = "URI to documentation regarding the issue.")
    private final String type;

    /** Human-readable explanation of the problem (optional). */
    @Schema(description = "Extended explanation of the issue.")
    private final String detail;

    /** Ignite 3 node name (optional). */
    @Schema(description = "Name of the node the issue happened on.")
    private final String node;

    /** Unique identifier that will help to trace the error in the log (optional). */
    @Schema(description = "Unique issue identifier. This identifier can be used to find logs related to the issue.")
    private final UUID traceId;

    /** List of parameters that did not pass the validation (optional). */
    @Schema(description = "A list of parameters that did not pass validation and the reason for it.")
    private final Collection<InvalidParam> invalidParams;

    /** Constructor. */
    @JsonCreator
    protected Problem(
            @JsonProperty("title") String title,
            @JsonProperty("status") int status,
            @JsonProperty("code") String code,
            @JsonProperty("type") @Nullable String type,
            @JsonProperty("detail") @Nullable String detail,
            @JsonProperty("node") @Nullable String node,
            @JsonProperty("traceId") @Nullable UUID traceId,
            @JsonProperty("invalidParams") @Nullable Collection<InvalidParam> invalidParams) {
        this.title = title;
        this.status = status;
        this.code = code;
        this.type = type;
        this.detail = detail;
        this.node = node;
        this.traceId = traceId;
        this.invalidParams = invalidParams;
    }

    /** Returns {@link ProblemBuilder}. */
    public static ProblemBuilder builder() {
        return new ProblemBuilder();
    }

    /** Returns {@link ProblemBuilder} with http status and title. */
    public static ProblemBuilder fromHttpCode(HttpCode httpCode) {
        ProblemBuilder builder = new ProblemBuilder();
        builder.status(httpCode.code());
        builder.title(httpCode.message());

        return builder;
    }

    /** Returns {@link ProblemBuilder} built from provided Ignite exception with http status and title. */
    public static ProblemBuilder fromIgniteException(IgniteException exception, HttpCode httpCode) {
        ProblemBuilder builder = new ProblemBuilder();
        builder.status(httpCode.code())
                .title(httpCode.message())
                .code(exception.codeAsString())
                .traceId(exception.traceId())
                .detail(extractDetailMessageOrNull(exception));

        return builder;
    }

    @JsonGetter("title")
    public String title() {
        return title;
    }

    @JsonGetter("status")
    public int status() {
        return status;
    }

    @JsonGetter("code")
    public String code() {
        return code;
    }

    @JsonGetter("type")
    public String type() {
        return type;
    }

    @JsonGetter("detail")
    public String detail() {
        return detail;
    }

    @JsonGetter("node")
    public String node() {
        return node;
    }

    @JsonGetter("traceId")
    public UUID traceId() {
        return traceId;
    }

    @JsonGetter("invalidParams")
    public Collection<InvalidParam> invalidParams() {
        return invalidParams;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Problem problem = (Problem) o;
        return status == problem.status && Objects.equals(title, problem.title) && Objects.equals(code, problem.code)
                && Objects.equals(type, problem.type) && Objects.equals(detail, problem.detail) && Objects.equals(
                node, problem.node) && Objects.equals(traceId, problem.traceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(title, status, code, type, detail, node, traceId);
    }

    @Override
    public String toString() {
        return "Problem{"
                + "title='" + title + '\''
                + ", status=" + status
                + ", code='" + code + '\''
                + ", type='" + type + '\''
                + ", detail='" + detail + '\''
                + ", node='" + node + '\''
                + ", invalidParams='" + invalidParams + '\''
                + ", traceId=" + traceId
                + '}';
    }

    /** Builder for {@link Problem}. */
    public static class ProblemBuilder {
        private String title;

        private int status;

        private String code;

        private String type;

        private String detail;

        private String node;

        private UUID traceId;

        private Collection<InvalidParam> invalidParams;

        public ProblemBuilder title(String title) {
            this.title = title;
            return this;
        }

        public ProblemBuilder status(int status) {
            this.status = status;
            return this;
        }

        public ProblemBuilder code(String code) {
            this.code = code;
            return this;
        }

        public ProblemBuilder type(String type) {
            this.type = type;
            return this;
        }

        public ProblemBuilder detail(String detail) {
            this.detail = detail;
            return this;
        }

        public ProblemBuilder node(String node) {
            this.node = node;
            return this;
        }

        public ProblemBuilder traceId(UUID traceId) {
            this.traceId = traceId;
            return this;
        }

        public ProblemBuilder invalidParams(Collection<InvalidParam> invalidParams) {
            this.invalidParams = invalidParams;
            return this;
        }

        public Problem build() {
            return new Problem(title, status, code, type, detail, node, traceId, invalidParams);
        }
    }

    @Nullable
    private static String extractDetailMessageOrNull(IgniteException exception) {
        String detail = ErrorGroups.extractCauseMessage(exception.getMessage());
        if (detail != null && detail.isBlank()) {
            detail = null;
        }
        return detail;
    }
}
