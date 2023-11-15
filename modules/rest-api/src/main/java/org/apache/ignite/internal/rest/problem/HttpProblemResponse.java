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

package org.apache.ignite.internal.rest.problem;

import static org.apache.ignite.internal.rest.problem.ProblemJsonMediaType.APPLICATION_JSON_PROBLEM_TYPE;

import io.micronaut.http.HttpResponseFactory;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpResponse;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.api.Problem.ProblemBuilder;

/**
 * Creates {@link MutableHttpResponse} from {@link Problem}.
 */
public final class HttpProblemResponse {
    private HttpProblemResponse() {
    }

    /**
     * Create {@link MutableHttpResponse} from {@link Problem}.
     */
    public static MutableHttpResponse<Problem> from(Problem problem) {
        return HttpResponseFactory.INSTANCE
                .status(HttpStatus.valueOf(problem.status()))
                .contentType(APPLICATION_JSON_PROBLEM_TYPE)
                .body(problem);
    }

    /**
     * Create {@link MutableHttpResponse} from {@link ProblemBuilder}.
     */
    public static MutableHttpResponse<? extends Problem> from(ProblemBuilder problemBuilder) {
        return from(problemBuilder.build());
    }
}
