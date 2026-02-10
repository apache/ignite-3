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

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.PathItem.HttpMethod;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.media.StringSchema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.jetbrains.annotations.Nullable;

/**
 * Matcher for checking OpenAPI compatibility between versions. Checks include missing paths, missing operations and others.
 */
@SuppressWarnings("rawtypes") // Mostly for the Schema class, which is parameterized but the API returns raw type.
public class OpenApiMatcher extends TypeSafeDiagnosingMatcher<OpenAPI> {
    private final OpenAPI baseApi;
    private final Set<String> removedPaths;

    private OpenApiMatcher(OpenAPI baseApi) {
        this(baseApi, Collections.emptySet());
    }

    private OpenApiMatcher(OpenAPI baseApi, Set<String> removedPaths) {
        this.baseApi = baseApi;
        this.removedPaths = removedPaths;
    }

    public static Matcher<OpenAPI> isCompatibleWith(OpenAPI baseApi) {
        return new OpenApiMatcher(baseApi);
    }

    public static Matcher<OpenAPI> isCompatibleWith(OpenAPI baseApi, Set<String> removedPaths) {
        return new OpenApiMatcher(baseApi, removedPaths);
    }

    @Override
    protected boolean matchesSafely(OpenAPI currentApi, Description mismatchDescription) {
        return new OpenApiDiff(baseApi, currentApi, mismatchDescription, removedPaths).compare();
    }

    private static class OpenApiDiff {
        private final OpenAPI baseApi;
        private final OpenAPI currentApi;
        private final Description mismatchDescription;
        private final Set<String> removedPaths;

        // Current direction
        private boolean isRequest;

        private OpenApiDiff(OpenAPI baseApi, OpenAPI currentApi, Description mismatchDescription, Set<String> removedPaths) {
            this.baseApi = baseApi;
            this.currentApi = currentApi;
            this.mismatchDescription = mismatchDescription;
            this.removedPaths = removedPaths;
        }

        boolean compare() {
            Paths basePaths = ofNullable(baseApi.getPaths()).orElse(new Paths());
            Paths currentPaths = ofNullable(currentApi.getPaths()).orElse(new Paths());
            return comparePaths(basePaths, currentPaths);
        }

        private boolean comparePaths(Paths basePaths, Paths currentPaths) {
            // Check for missing paths
            Set<String> missingPaths = basePaths.keySet().stream()
                    .filter(path -> !currentPaths.containsKey(path))
                    .filter(path -> !removedPaths.contains(path))
                    .collect(toSet());

            if (!missingPaths.isEmpty()) {
                mismatchDescription.appendText("has missing paths ").appendValueList("", ", ", "", missingPaths);
                return false;
            }

            for (Entry<String, PathItem> entry : basePaths.entrySet()) {
                String path = entry.getKey();

                if (removedPaths.contains(path)) {
                    continue;
                }

                Map<HttpMethod, Operation> baseOperations = entry.getValue().readOperationsMap();
                Map<HttpMethod, Operation> currentOperations = currentPaths.get(path).readOperationsMap();

                if (!compareOperations(path, baseOperations, currentOperations)) {
                    return false;
                }
            }

            return true;
        }

        private boolean compareOperations(
                String path,
                Map<HttpMethod, Operation> baseOperations,
                Map<HttpMethod, Operation> currentOperations
        ) {
            Set<HttpMethod> missingMethods = baseOperations.keySet().stream()
                    .filter(method -> !currentOperations.containsKey(method))
                    .collect(toSet());

            if (!missingMethods.isEmpty()) {
                mismatchDescription.appendText("path ").appendValue(path)
                        .appendText(" has missing operations ").appendValueList("", ", ", "", missingMethods);
                return false;
            }

            for (Entry<HttpMethod, Operation> operationEntry : baseOperations.entrySet()) {
                Operation baseOperation = operationEntry.getValue();
                Operation currentOperation = currentOperations.get(operationEntry.getKey());

                if (!compareOperation(path, operationEntry.getKey(), baseOperation, currentOperation)) {
                    return false;
                }
            }

            return true;
        }

        private boolean compareOperation(
                String path,
                HttpMethod method,
                Operation baseOperation,
                Operation currentOperation
        ) {
            if (!Objects.equals(baseOperation.getOperationId(), currentOperation.getOperationId())) {
                describeOperation(path, method)
                        .appendText(" has different operation id ").appendValue(currentOperation.getOperationId());
                return false;
            }

            return compareRequestBody(path, method, baseOperation.getRequestBody(), currentOperation.getRequestBody())
                    && compareParameters(path, method, baseOperation, currentOperation)
                    && compareResponses(path, method, baseOperation, currentOperation);
        }

        private boolean compareRequestBody(
                String path,
                HttpMethod method,
                RequestBody baseRequestBody,
                RequestBody currentRequestBody
        ) {
            isRequest = true;
            if (baseRequestBody != null) {
                if (currentRequestBody == null) {
                    describeOperation(path, method).appendText(" has missing request body");
                    return false;
                }

                StringDescription contentMismatchDescription = new StringDescription();
                if (!compareContent(contentMismatchDescription, baseRequestBody.getContent(), currentRequestBody.getContent())) {
                    describeOperation(path, method)
                            .appendText(" has different request body content: ").appendText(contentMismatchDescription.toString());
                    return false;
                }
            }
            return true;
        }

        private boolean compareParameters(
                String path,
                HttpMethod method,
                Operation baseOperation,
                Operation currentOperation
        ) {
            isRequest = true;
            Map<String, Parameter> baseParameters = parametersByName(baseOperation.getParameters());
            Map<String, Parameter> currentParameters = parametersByName(currentOperation.getParameters());

            for (Entry<String, Parameter> entry : baseParameters.entrySet()) {
                Parameter baseParameter = entry.getValue();
                Parameter currentParameter = currentParameters.get(entry.getKey());
                if (currentParameter != null && !compareParameter(path, method, baseParameter, currentParameter)) {
                    return false;
                }
            }

            Set<String> baseRequiredParameterNames = baseParameters.values().stream()
                    .filter(parameter -> parameter.getRequired() == Boolean.TRUE)
                    .map(Parameter::getName)
                    .collect(toSet());

            Set<String> newRequiredParameterNames = currentParameters.values().stream()
                    .filter(parameter -> parameter.getRequired() == Boolean.TRUE)
                    .map(Parameter::getName)
                    .filter(Predicate.not(baseRequiredParameterNames::contains))
                    .collect(toSet());

            if (!newRequiredParameterNames.isEmpty()) {
                describeOperation(path, method)
                        .appendText(" has new required parameters ").appendValue(newRequiredParameterNames);
                return false;
            }

            return true;
        }

        private boolean compareResponses(
                String path,
                HttpMethod method,
                Operation baseOperation,
                Operation currentOperation
        ) {
            isRequest = false;
            ApiResponses baseResponses = ofNullable(baseOperation.getResponses()).orElse(new ApiResponses());
            ApiResponses currentResponses = ofNullable(currentOperation.getResponses()).orElse(new ApiResponses());

            Set<String> missingResponses = baseResponses.keySet().stream()
                    .filter(response -> !currentResponses.containsKey(response))
                    .collect(toSet());

            if (!missingResponses.isEmpty()) {
                describeOperation(path, method)
                        .appendText(" has missing responses ").appendValueList("", ", ", "", missingResponses);
                return false;
            }

            for (Entry<String, ApiResponse> responseEntry : baseResponses.entrySet()) {
                ApiResponse baseResponse = responseEntry.getValue();
                ApiResponse currentResponse = currentResponses.get(responseEntry.getKey());

                StringDescription contentMismatchDescription = new StringDescription();
                if (!compareContent(contentMismatchDescription, baseResponse.getContent(), currentResponse.getContent())) {
                    describeOperation(path, method)
                            .appendText(" response ").appendValue(responseEntry.getKey())
                            .appendText(" has incompatible content: ").appendText(contentMismatchDescription.toString());
                    return false;
                }
            }

            return true;
        }

        private boolean compareContent(StringDescription mismatchDescription, Content baseContent, Content currentContent) {
            baseContent = baseContent != null ? baseContent : new Content();
            currentContent = currentContent != null ? currentContent : new Content();

            // Content is a map from media type to media type description object. Usually it's single item. Compare the types first.
            if (!Objects.equals(baseContent.keySet(), currentContent.keySet())) {
                mismatchDescription.appendText("Content has different type ").appendValue(currentContent.keySet());
                return false;
            }

            // Then compare descriptions.
            for (Entry<String, MediaType> entry : baseContent.entrySet()) {
                String schemaName = entry.getKey();
                Schema<?> baseSchema = entry.getValue().getSchema();
                Schema<?> currentSchema = currentContent.get(schemaName).getSchema();

                if (!compareSchema("content", mismatchDescription, baseSchema, currentSchema)) {
                    return false;
                }
            }

            return true;
        }

        private boolean compareParameter(
                String path,
                HttpMethod method,
                Parameter baseParameter,
                Parameter currentParameter
        ) {
            String parameterName = baseParameter.getName();
            if (!Objects.equals(baseParameter.getIn(), currentParameter.getIn())) {
                describeOperation(path, method)
                        .appendText(" has different locations of parameter ").appendValue(parameterName);
                return false;
            }

            if (!Objects.equals(baseParameter.get$ref(), currentParameter.get$ref())) {
                describeOperation(path, method)
                        .appendText(" has different references of parameter ").appendValue(parameterName);
                return false;
            }

            StringDescription schemaMismatchDescription = new StringDescription();
            if (!compareSchema(
                    path + "/" + parameterName,
                    schemaMismatchDescription,
                    baseParameter.getSchema(),
                    currentParameter.getSchema()
            )) {
                describeOperation(path, method)
                        .appendText(" has incompatible schemas of parameter ").appendValue(parameterName)
                        .appendText(" : ").appendText(schemaMismatchDescription.toString());
                return false;
            }

            return true;
        }

        private boolean compareSchema(
                String path,
                Description mismatchDescription,
                Schema<?> baseSchema,
                Schema<?> currentSchema
        ) {
            if (baseSchema == null || currentSchema == null) {
                return true;
            }

            String baseType = baseSchema.getType();
            String currentType = currentSchema.getType();

            if (!Objects.equals(baseType, currentType)) {
                mismatchDescription.appendText("Schema ").appendValue(path).appendText(" has different type");
                return false;
            }

            if (!Objects.equals(baseSchema.getFormat(), currentSchema.getFormat())) {
                mismatchDescription.appendText("Schema ").appendValue(path).appendText(" has different formats");
                return false;
            }

            String baseRef = baseSchema.get$ref();
            if (!Objects.equals(baseRef, currentSchema.get$ref())) {
                mismatchDescription.appendText("Schema ").appendValue(path).appendText(" has different reference");
                return false;
            }

            // Resolve base reference, current is the same
            if (baseRef != null) {
                Schema baseRefSchema = resolveSchema(baseApi, baseRef);
                Schema currentRefSchema = resolveSchema(currentApi, baseRef);
                return compareSchema(baseRef, mismatchDescription, baseRefSchema, currentRefSchema);
            }

            if (baseSchema.getAllOf() != null) {
                if (currentSchema.getAllOf() == null || currentSchema.getAllOf().size() != baseSchema.getAllOf().size()) {
                    mismatchDescription.appendText("Schema ").appendValue(path).appendText(" has different allOf combination");
                    return false;
                }

                List<Schema> baseAllOf = baseSchema.getAllOf();
                for (int i = 0; i < baseAllOf.size(); i++) {
                    Schema schema = baseAllOf.get(i);
                    if (!compareSchema(path, mismatchDescription, schema, currentSchema.getAllOf().get(i))) {
                        return false;
                    }
                }
            }

            if ("array".equals(baseType)) {
                return compareSchema(path + "/array", mismatchDescription, baseSchema.getItems(), currentSchema.getItems());
            }

            if ("object".equals(baseType)) {
                return compareObjectSchema(path, mismatchDescription, baseSchema, currentSchema);
            }

            if ("string".equals(baseType)) {
                return compareStringSchema(path, mismatchDescription, baseSchema, currentSchema);
            }

            return true;
        }

        private boolean compareObjectSchema(String path, Description mismatchDescription, Schema<?> baseSchema, Schema<?> currentSchema) {
            if (isRequest) {
                Set<String> baseRequired = new HashSet<>(ofNullable(baseSchema.getRequired()).orElse(List.of()));

                Set<String> newRequired = ofNullable(currentSchema.getRequired()).orElse(List.of()).stream()
                        .filter(Predicate.not(baseRequired::contains))
                        .collect(toSet());

                if (!newRequired.isEmpty()) {
                    mismatchDescription.appendText("Schema ").appendValue(path)
                            .appendText(" has new required properties ").appendValue(newRequired);
                    return false;
                }
            } else {
                Set<String> currentRequired = new HashSet<>(ofNullable(currentSchema.getRequired()).orElse(List.of()));

                Set<String> newOptional = ofNullable(baseSchema.getRequired()).orElse(List.of()).stream()
                        .filter(Predicate.not(currentRequired::contains))
                        .collect(toSet());

                if (!newOptional.isEmpty()) {
                    mismatchDescription.appendText("Schema ").appendValue(path)
                            .appendText(" has new optional properties ").appendValue(newOptional);
                    return false;
                }
            }

            Map<String, Schema> baseProperties = ofNullable(baseSchema.getProperties()).orElse(Map.of());
            Map<String, Schema> currentProperties = ofNullable(currentSchema.getProperties()).orElse(Map.of());
            if (!compareProperties(path + "/object", mismatchDescription, baseProperties, currentProperties)) {
                return false;
            }

            Object baseAdditionalProperties = baseSchema.getAdditionalProperties();
            Object currentAdditionalProperties = currentSchema.getAdditionalProperties();
            if (baseAdditionalProperties instanceof Schema && currentAdditionalProperties instanceof Schema) {
                return compareSchema(
                        path + "/additionalProperties",
                        mismatchDescription,
                        ((Schema) baseAdditionalProperties),
                        ((Schema) currentAdditionalProperties)
                );
            }

            return true;
        }

        private boolean compareStringSchema(String path, Description mismatchDescription, Schema<?> baseSchema, Schema<?> currentSchema) {
            if (!(baseSchema instanceof StringSchema) || !(currentSchema instanceof StringSchema)) {
                return true;
            }

            if (isRequest) {
                Set<String> currentEnum = new HashSet<>(ofNullable(((StringSchema) currentSchema).getEnum()).orElse(List.of()));

                Set<String> missingValues = ofNullable(((StringSchema) baseSchema).getEnum()).orElse(List.of()).stream()
                        .filter(Predicate.not(currentEnum::contains))
                        .collect(toSet());

                if (!missingValues.isEmpty()) {
                    mismatchDescription.appendText("Schema ").appendValue(path)
                            .appendText(" has missing enum values ").appendValue(missingValues);
                    return false;
                }
            }
            return true;
        }

        private static Schema resolveSchema(OpenAPI openApi, String baseRef) {
            return openApi.getComponents().getSchemas().get(baseRef.substring(baseRef.lastIndexOf('/') + 1));
        }

        private boolean compareProperties(
                String path,
                Description mismatchDescription,
                Map<String, Schema> baseProperties,
                Map<String, Schema> currentProperties
        ) {
            for (Entry<String, Schema> entry : baseProperties.entrySet()) {
                String schemaName = entry.getKey();
                Schema baseSchema = entry.getValue();
                Schema currentSchema = currentProperties.get(schemaName);
                // If property was required, we will detect it earlier. If it was optional and was removed, skip comparing.
                if (currentSchema != null && !compareSchema(path + "/" + schemaName, mismatchDescription, baseSchema, currentSchema)) {
                    return false;
                }
            }
            return true;
        }

        private Description describeOperation(String path, HttpMethod operation) {
            return mismatchDescription.appendText("operation ").appendValue(operation).appendText(" at path ").appendValue(path);
        }

        private static Map<String, Parameter> parametersByName(@Nullable List<Parameter> parameters) {
            return parameters != null ? parameters.stream().collect(toMap(Parameter::getName, Function.identity())) : Map.of();
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("OpenAPI spec");
        if (baseApi.getInfo() != null) {
            description.appendText(" compatible with version ").appendValue(baseApi.getInfo().getVersion());
        }
    }
}
