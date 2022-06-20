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
import java.util.UUID;

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

    public static ProblemBuilder builder() {
        return new ProblemBuilder();
    }

    /**
     * Builder for {@link Problem}.
     */
    public static class ProblemBuilder {
        private String title;

        private int status;

        private String code;

        private String type;

        private String detail;

        private String node;

        private UUID traceId;

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

        public Problem build() {
            return new Problem(title, status, code, type, detail, node, traceId);
        }
    }
}
