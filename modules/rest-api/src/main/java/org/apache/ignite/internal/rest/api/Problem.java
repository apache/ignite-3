/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.rest.constants.HttpCode;
import org.apache.ignite.internal.rest.problem.Builder;

/**
 * Implements application/problem+json schema defined in <a href="https://www.rfc-editor.org/rfc/rfc7807.html">RFC-7807</a>.
 */
public class Problem {
    /**
     * Short, human-readable summary of the problem type.
     */
    private final String title;

    /**
     * HTTP status code.
     */
    private final int status;

    /**
     * Ignite 3 error code.
     */
    private final String code;

    /**
     * URI to the error documentation (optional).
     */
    private final String type;

    /**
     * Human-readable explanation of the problem (optional).
     */
    private final String detail;

    /**
     * Ignite 3 node name (optional).
     */
    private final String node;

    /**
     * Unique identifier that will help to trace the error in the log (optional).
     */
    private final UUID traceId;

    /**
     * Constructor.
     */
    @JsonCreator
    protected Problem(
            @JsonProperty("title") String title,
            @JsonProperty("status") int status,
            @JsonProperty("code") String code,
            @JsonProperty("type") String type,
            @JsonProperty("detail") String detail,
            @JsonProperty("node") String node,
            @JsonProperty("traceId") UUID traceId) {
        this.title = title;
        this.status = status;
        this.code = code;
        this.type = type;
        this.detail = detail;
        this.node = node;
        this.traceId = traceId;
    }

    /**
     * Returns {@link ProblemBuilder}.
     */
    public static <T extends Problem, B extends ProblemBuilder<T, B>> ProblemBuilder<T, B> builder() {
        return new ProblemBuilder<>();
    }

    /**
     * Returns {@link ProblemBuilder} with http status and title.
     */
    public static <T extends Problem, B extends ProblemBuilder<T, B>> ProblemBuilder<T, B> fromHttpCode(HttpCode httpCode) {
        ProblemBuilder<T, B> builder = new ProblemBuilder<>();
        builder.status(httpCode.code());
        builder.title(httpCode.message());

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
                + ", traceId=" + traceId
                + '}';
    }

    /**
     * Builder for {@link Problem}.
     */
    public static class ProblemBuilder<T extends Problem, B extends ProblemBuilder<T, B>> implements Builder<T, B> {
        protected String title;

        protected int status;

        protected String code;

        protected String type;

        protected String detail;

        protected String node;

        protected UUID traceId;

        public B title(String title) {
            this.title = title;
            return (B) this;
        }

        public B status(int status) {
            this.status = status;
            return (B) this;
        }

        public B code(String code) {
            this.code = code;
            return (B) this;
        }

        public B type(String type) {
            this.type = type;
            return (B) this;
        }

        public B detail(String detail) {
            this.detail = detail;
            return (B) this;
        }

        public B node(String node) {
            this.node = node;
            return (B) this;
        }

        public B traceId(UUID traceId) {
            this.traceId = traceId;
            return (B) this;
        }

        @Override
        public T build() {
            return (T) new Problem(title, status, code, type, detail, node, traceId);
        }
    }
}
