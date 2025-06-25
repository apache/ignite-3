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

package org.apache.ignite.internal;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.PathItem.HttpMethod;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public class OpenAPIMatcher extends TypeSafeDiagnosingMatcher<OpenAPI> {
    private final OpenAPI baseApi;

    private OpenAPIMatcher(OpenAPI baseApi) {
        this.baseApi = baseApi;
    }

    public static Matcher<OpenAPI> isCompatibleWith(OpenAPI baseApi) {
        return new OpenAPIMatcher(baseApi);
    }

    @Override
    protected boolean matchesSafely(OpenAPI currentApi, Description mismatchDescription) {
        // Check for missing paths
        Set<String> missingPaths = new HashSet<>(baseApi.getPaths().keySet());
        missingPaths.removeAll(currentApi.getPaths().keySet());
        if (!missingPaths.isEmpty()) {
            mismatchDescription.appendText("has missing paths ").appendValueList("", ", ", "", missingPaths);
            return false;
        }

        for (Entry<String, PathItem> entry : baseApi.getPaths().entrySet()) {
            String path = entry.getKey();

            Map<HttpMethod, Operation> baseOperations = entry.getValue().readOperationsMap();
            Map<HttpMethod, Operation> currentOperations = currentApi.getPaths().get(path).readOperationsMap();

            if (!compareOperations(path, baseOperations, currentOperations, mismatchDescription)) {
                return false;
            }
        }

        return true;
    }

    private static boolean compareOperations(
            String path,
            Map<HttpMethod, Operation> baseOperations,
            Map<HttpMethod, Operation> currentOperations,
            Description mismatchDescription
    ) {
        //noinspection SetReplaceableByEnumSet EnumSet.copyOf requires at least one element
        Set<HttpMethod> missingMethods = new HashSet<>(baseOperations.keySet());
        missingMethods.removeAll(currentOperations.keySet());
        if (!missingMethods.isEmpty()) {
            mismatchDescription.appendText("path \"").appendText(path)
                    .appendText("\" has missing operations ").appendValueList("", ", ", "", missingMethods);
            return false;
        }

        for (Entry<HttpMethod, Operation> operationEntry : baseOperations.entrySet()) {
            Operation baseOperation = operationEntry.getValue();
            Operation currentOperation = currentOperations.get(operationEntry.getKey());

            if (!compareOperation(path, operationEntry.getKey(), baseOperation, currentOperation, mismatchDescription)) {
                return false;
            }
        }

        return true;
    }

    private static boolean compareOperation(
            String path,
            HttpMethod operation,
            Operation baseOperation,
            Operation currentOperation,
            Description mismatchDescription
    ) {
        return compareRequestBody(path, operation, baseOperation.getRequestBody(), currentOperation.getRequestBody(), mismatchDescription)
                && compareParameters(path, operation, baseOperation.getParameters(), currentOperation.getParameters(), mismatchDescription);
    }

    private static boolean compareRequestBody(
            String path,
            HttpMethod operation,
            RequestBody baseRequestBody,
            RequestBody currentRequestBody,
            Description mismatchDescription
    ) {
        if (baseRequestBody != null) {
            if (currentRequestBody == null) {
                mismatchDescription.appendText("operation ").appendValue(operation)
                        .appendText(" at path \"").appendText(path).appendText("\" has missing request body");
                return false;
            }
            if (!Objects.equals(baseRequestBody.getContent().keySet(), currentRequestBody.getContent().keySet())) {
                mismatchDescription.appendText("operation ").appendValue(operation)
                        .appendText(" at path \"").appendText(path)
                        .appendText("\" has request body ").appendValue(currentRequestBody.getContent().keySet());
                return false;
            }
        }
        return true;
    }

    private static boolean compareParameters(
            String path,
            HttpMethod operation,
            List<Parameter> baseParameters,
            List<Parameter> currentParameters,
            Description mismatchDescription)
    {
        if (currentParameters != null) {
            Set<String> baseRequiredParameters;
            if (baseParameters != null) {
                baseRequiredParameters = baseParameters.stream()
                        .filter(Parameter::getRequired)
                        .map(Parameter::getName)
                        .collect(Collectors.toSet());
            } else {
                baseRequiredParameters = Set.of();
            }

            Set<String> currentRequiredParameters = currentParameters.stream()
                    .filter(Parameter::getRequired)
                    .map(Parameter::getName)
                    .collect(Collectors.toSet());

            Set<String> newRequiredParameters = new HashSet<>(currentRequiredParameters);
            newRequiredParameters.removeAll(baseRequiredParameters);
            if (!newRequiredParameters.isEmpty()) {
                mismatchDescription.appendText("operation ").appendValue(operation)
                        .appendText(" at path \"").appendText(path)
                        .appendText("\" has new required paramters ").appendValue(newRequiredParameters);
                return false;
            }
        }

        return true;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("OpenAPI spec");
        if (baseApi.getInfo() != null) {
            description.appendText(" compatible with version ").appendValue(baseApi.getInfo().getVersion());
        }
    }
}
