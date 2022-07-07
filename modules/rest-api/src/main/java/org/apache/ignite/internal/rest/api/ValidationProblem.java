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
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.rest.constants.HttpCode;

/**
 * Validation problem that adds one more property (invalidParams) to the standard problem.
 */
public class ValidationProblem extends Problem {
    /** List of parameter that did not pass the validation (optional). */
    private final Collection<InvalidParam> invalidParams;

    /** Constructor. */
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ValidationProblem that = (ValidationProblem) o;
        return Objects.equals(invalidParams, that.invalidParams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), invalidParams);
    }

    @Override
    public String toString() {
        return "ValidationProblem{"
                + "invalidParams=" + invalidParams
                + "} " + super.toString();
    }

    /** Returns {@link ValidationProblemBuilder}. */
    public static ValidationProblemBuilder builder() {
        return new ValidationProblemBuilder();
    }

    /** Returns {@link ValidationProblemBuilder} with http status and title. */
    public static ValidationProblemBuilder fromHttpCode(HttpCode httpCode) {
        ValidationProblemBuilder builder = new ValidationProblemBuilder();
        builder.status(httpCode.code());
        builder.title(httpCode.message());

        return builder;
    }

    /** Builder for {@link ValidationProblem}. */
    public static class ValidationProblemBuilder extends ProblemBuilder<ValidationProblem, ValidationProblemBuilder> {
        private Collection<InvalidParam> invalidParams;

        public ValidationProblemBuilder invalidParams(Collection<InvalidParam> invalidParams) {
            this.invalidParams = invalidParams;
            return this;
        }

        @Override
        public ValidationProblem build() {
            return new ValidationProblem(title, status, code, type, detail, node, traceId, invalidParams);
        }
    }
}
