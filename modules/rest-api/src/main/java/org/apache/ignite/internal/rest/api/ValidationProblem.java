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
import java.util.Collection;
import java.util.UUID;

/**
 * Validation problem that adds one more property (invalidParams) to the standard problem.
 */
public class ValidationProblem extends Problem {
    /**
     * List of parameter that did not pass the validation (optional).
     */
    private final Collection<InvalidParam> invalidParams;

    /**
     * Constructor.
     */
    @JsonCreator
    public ValidationProblem(
            @JsonProperty("title") String title,
            @JsonProperty("status") int status,
            @JsonProperty("code") String code,
            @JsonProperty("type") String type,
            @JsonProperty("detail") String detail,
            @JsonProperty("node") String node,
            @JsonProperty("traceId") UUID traceId,
            @JsonProperty("invalidParams") Collection<InvalidParam> invalidParams) {

        super(title, status, code, type, detail, node, traceId);
        this.invalidParams = invalidParams;
    }

    @JsonGetter("invalidParams")
    public Collection<InvalidParam> invalidParams() {
        return invalidParams;
    }

    public static ValidationProblemBuilder validationProblemBuilder() {
        return new ValidationProblemBuilder();
    }

    /**
     * Builder for {@link ValidationProblem}.
     */
    public static class ValidationProblemBuilder { // todo: use builder inheritance
        private String title;

        private int status;

        private String code;

        private String type;

        private String detail;

        private String node;

        private UUID traceId;

        private Collection<InvalidParam> invalidParams;

        public ValidationProblemBuilder title(String title) {
            this.title = title;
            return this;
        }

        public ValidationProblemBuilder status(int status) {
            this.status = status;
            return this;
        }

        public ValidationProblemBuilder code(String code) {
            this.code = code;
            return this;
        }

        public ValidationProblemBuilder type(String type) {
            this.type = type;
            return this;
        }

        public ValidationProblemBuilder detail(String detail) {
            this.detail = detail;
            return this;
        }

        public ValidationProblemBuilder node(String node) {
            this.node = node;
            return this;
        }

        public ValidationProblemBuilder traceId(UUID traceId) {
            this.traceId = traceId;
            return this;
        }

        public ValidationProblemBuilder invalidParams(Collection<InvalidParam> invalidParams) {
            this.invalidParams = invalidParams;
            return this;
        }

        public ValidationProblem build() {
            return new ValidationProblem(title, status, code, type, detail, node, traceId, invalidParams);
        }
    }
}
