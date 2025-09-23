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

import static org.apache.ignite.internal.OpenApiMatcher.isCompatibleWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.media.StringSchema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.junit.jupiter.api.Test;

@SuppressWarnings("rawtypes") // Mostly for the Schema class, which is parameterized but the API returns raw type.
class OpenApiMatcherTest {
    @Test
    void identicalApis() {
        PathItem pathItem = new PathItem()
                .get(new Operation()
                        .operationId("test")
                        .requestBody(new RequestBody().content(new Content().addMediaType("text/plain", new MediaType()))))
                .post(new Operation().addParametersItem(new Parameter().name("param").in("path").required(true)));

        OpenAPI baseApi = new OpenAPI().path("test", pathItem);
        OpenAPI currentApi = new OpenAPI().path("test", pathItem);

        assertThat(currentApi, isCompatibleWith(baseApi));
    }

    @Test
    void newPath() {
        OpenAPI baseApi = new OpenAPI().path("test", new PathItem());
        OpenAPI currentApi = new OpenAPI().path("test", new PathItem()).path("testNew", new PathItem());

        assertThat(currentApi, isCompatibleWith(baseApi));
    }

    @Test
    void newOperation() {
        OpenAPI baseApi = new OpenAPI().path("test", new PathItem().get(new Operation()));
        OpenAPI currentApi = new OpenAPI().path("test", new PathItem().get(new Operation()).post(new Operation()));

        assertThat(currentApi, isCompatibleWith(baseApi));
    }

    @Test
    void newRequestBody() {
        OpenAPI baseApi = new OpenAPI().path("test", new PathItem().get(new Operation()));
        OpenAPI currentApi = new OpenAPI().path("test", new PathItem().get(new Operation().requestBody(new RequestBody())));

        assertThat(currentApi, isCompatibleWith(baseApi));
    }

    @Test
    void missingPaths() {
        OpenAPI baseApi = new OpenAPI().path("test", new PathItem()).path("test2", new PathItem());
        OpenAPI currentApi = new OpenAPI().path("testNew", new PathItem());

        assertThatMismatchedWithDescription(baseApi, currentApi, "has missing paths \"test2\", \"test\"");
    }

    @Test
    void missingOperation() {
        OpenAPI baseApi = new OpenAPI().path("test", new PathItem().get(new Operation()));
        OpenAPI currentApi = new OpenAPI().path("test", new PathItem().post(new Operation()));

        assertThatMismatchedWithDescription(baseApi, currentApi, "path \"test\" has missing operations <GET>");
    }

    @Test
    void changedOperationId() {
        OpenAPI baseApi = new OpenAPI().path("test", new PathItem().get(new Operation().operationId("test")));
        OpenAPI currentApi = new OpenAPI().path("test", new PathItem().get(new Operation().operationId("test1")));

        assertThatMismatchedWithDescription(baseApi, currentApi, "operation <GET> at path \"test\" has different operation id \"test1\"");
    }

    @Test
    void missingRequestBody() {
        OpenAPI baseApi = new OpenAPI().path("test", new PathItem().get(new Operation().requestBody(new RequestBody())));
        OpenAPI currentApi = new OpenAPI().path("test", new PathItem().get(new Operation()));

        assertThatMismatchedWithDescription(baseApi, currentApi, "operation <GET> at path \"test\" has missing request body");
    }

    @Test
    void changedRequestBodyContent() {
        OpenAPI baseApi = new OpenAPI().path("test", new PathItem().get(new Operation().requestBody(new RequestBody()
                .content(new Content().addMediaType("application/json", new MediaType())))));
        OpenAPI currentApi = new OpenAPI().path("test", new PathItem().get(new Operation().requestBody(new RequestBody()
                .content(new Content().addMediaType("text/plain", new MediaType())))));

        assertThatMismatchedWithDescription(baseApi, currentApi, "operation <GET> at path \"test\" has different request body content: "
                + "Content has different type <[text/plain]>");
    }

    @Test
    void differentParameterLocation() {
        OpenAPI baseApi = new OpenAPI().path("test", new PathItem().get(new Operation()
                .addParametersItem(new Parameter().name("param").in("path"))));
        OpenAPI currentApi = new OpenAPI().path("test", new PathItem().get(new Operation()
                .addParametersItem(new Parameter().name("param").in("query"))));

        assertThatMismatchedWithDescription(baseApi, currentApi, "operation <GET> at path \"test\" has"
                + " different locations of parameter \"param\"");
    }

    @Test
    void newRequiredParameter() {
        OpenAPI baseApi = new OpenAPI().path("test", new PathItem().get(new Operation()));
        OpenAPI currentApi = new OpenAPI().path("test", new PathItem().get(new Operation()
                .addParametersItem(new Parameter().name("param").in("path").required(true))));

        assertThatMismatchedWithDescription(baseApi, currentApi,
                "operation <GET> at path \"test\" has new required parameters <[param]>");
    }

    @Test
    void differentParameterSchemaType() {
        OpenAPI baseApi = new OpenAPI().path("test", new PathItem().get(new Operation()
                .addParametersItem(new Parameter().name("param").schema(new Schema().type("object")))));
        OpenAPI currentApi = new OpenAPI().path("test", new PathItem().get(new Operation()
                .addParametersItem(new Parameter().name("param").schema(new Schema().type("string")))));

        assertThatMismatchedWithDescription(baseApi, currentApi, "operation <GET> at path \"test\" has incompatible schemas "
                + "of parameter \"param\" : Schema \"test/param\" has different type");
    }

    @Test
    void differentParameterSchemaFormat() {
        OpenAPI baseApi = new OpenAPI().path("test", new PathItem().get(new Operation()
                .addParametersItem(new Parameter().name("param").schema(new Schema().type("integer").format("int32")))));
        OpenAPI currentApi = new OpenAPI().path("test", new PathItem().get(new Operation()
                .addParametersItem(new Parameter().name("param").schema(new Schema().type("integer").format("int64")))));

        assertThatMismatchedWithDescription(baseApi, currentApi, "operation <GET> at path \"test\" has incompatible schemas "
                + "of parameter \"param\" : Schema \"test/param\" has different formats");
    }

    @Test
    void differentParameterSchemaRef() {
        OpenAPI baseApi = new OpenAPI().path("test", new PathItem().get(new Operation()
                .addParametersItem(new Parameter().name("param").schema(new Schema().$ref("#/components/schemas/test")))));
        OpenAPI currentApi = new OpenAPI().path("test", new PathItem().get(new Operation()
                .addParametersItem(new Parameter().name("param").schema(new Schema().$ref("#/components/schemas/test1")))));

        assertThatMismatchedWithDescription(baseApi, currentApi, "operation <GET> at path \"test\" has incompatible schemas "
                + "of parameter \"param\" : Schema \"test/param\" has different reference");
    }

    @Test
    void differentComponentsSchemaType() {
        PathItem pathItem = createPathItem();
        OpenAPI baseApi = new OpenAPI().path("test", pathItem)
                .components(new Components().addSchemas("testSchema", new Schema().type("object")));
        OpenAPI currentApi = new OpenAPI().path("test", pathItem)
                .components(new Components().addSchemas("testSchema", new Schema().type("string")));

        assertThatMismatchedWithDescription(baseApi, currentApi, "operation <GET> at path \"test\" has incompatible schemas of "
                + "parameter \"param\" : Schema \"#/components/schemas/testSchema\" has different type");
    }

    @Test
    void differentComponentsSchemaAdditionalType() {
        PathItem pathItem = createPathItem();
        OpenAPI baseApi = new OpenAPI().path("test", pathItem)
                .components(new Components().addSchemas("testSchema", new Schema().type("object")
                        .additionalProperties(new Schema().type("object"))));
        OpenAPI currentApi = new OpenAPI().path("test", pathItem)
                .components(new Components().addSchemas("testSchema", new Schema().type("object")
                        .additionalProperties(new Schema().type("string"))));

        assertThatMismatchedWithDescription(baseApi, currentApi, "operation <GET> at path \"test\" has incompatible schemas of "
                + "parameter \"param\" : Schema \"#/components/schemas/testSchema/additionalProperties\" has different type");
    }

    @Test
    void differentComponentsSchemaInnerType() {
        PathItem pathItem = createPathItem();
        OpenAPI baseApi = new OpenAPI().path("test", pathItem)
                .components(new Components().addSchemas("testSchema", new Schema().type("object")
                        .addProperty("value", new Schema().type("object"))));
        OpenAPI currentApi = new OpenAPI().path("test", pathItem)
                .components(new Components().addSchemas("testSchema", new Schema().type("object")
                        .addProperty("value", new Schema().type("string"))));

        assertThatMismatchedWithDescription(baseApi, currentApi, "operation <GET> at path \"test\" has incompatible schemas "
                + "of parameter \"param\" : Schema \"#/components/schemas/testSchema/object/value\" has different type");
    }

    @Test
    void newRequiredParameters() {
        PathItem pathItem = createPathItem();
        OpenAPI baseApi = new OpenAPI().path("test", pathItem)
                .components(new Components().addSchemas("testSchema", new Schema().type("object")
                        .addProperty("prop", new Schema())));
        OpenAPI currentApi = new OpenAPI().path("test", pathItem)
                .components(new Components().addSchemas("testSchema", new Schema().type("object")
                        .addProperty("prop", new Schema())
                        .addProperty("propRequired", new Schema())
                        .addRequiredItem("propRequired")
                ));

        assertThatMismatchedWithDescription(baseApi, currentApi, "operation <GET> at path \"test\" has incompatible schemas of "
                + "parameter \"param\" : Schema \"#/components/schemas/testSchema\" has new required properties <[propRequired]>");
    }

    @Test
    void newOptionalParameters() {
        PathItem pathItem = new PathItem().get(new Operation().responses(new ApiResponses()
                .addApiResponse("200", new ApiResponse().content(new Content()
                        .addMediaType("application/json", new MediaType()
                                .schema(new Schema().$ref("#/components/schemas/testSchema")))))));

        OpenAPI baseApi = new OpenAPI().path("test", pathItem)
                .components(new Components().addSchemas("testSchema", new Schema().type("object")
                        .addProperty("prop", new Schema())
                        .addRequiredItem("prop")));
        OpenAPI currentApi = new OpenAPI().path("test", pathItem)
                .components(new Components().addSchemas("testSchema", new Schema().type("object")
                        .addProperty("prop", new Schema())));

        assertThatMismatchedWithDescription(baseApi, currentApi, "operation <GET> at path \"test\" response \"200\" has "
                + "incompatible content: Schema \"#/components/schemas/testSchema\" has new optional properties <[prop]>");
    }

    @Test
    void newEnumValuesInRequest() {
        PathItem pathItem = createPathItem();
        StringSchema baseSchema = new StringSchema().type("string");
        baseSchema.addEnumItemObject("FIRST");
        baseSchema.addEnumItemObject("SECOND");
        OpenAPI baseApi = new OpenAPI().path("test", pathItem)
                .components(new Components().addSchemas("testSchema", baseSchema));
        StringSchema currentSchema = new StringSchema().type("string");
        currentSchema.addEnumItemObject("FIRST");
        OpenAPI currentApi = new OpenAPI().path("test", pathItem)
                .components(new Components().addSchemas("testSchema", currentSchema));

        assertThatMismatchedWithDescription(baseApi, currentApi, "operation <GET> at path \"test\" has incompatible schemas "
                + "of parameter \"param\" : Schema \"#/components/schemas/testSchema\" has missing enum values <[SECOND]>");
    }

    private static void assertThatMismatchedWithDescription(OpenAPI baseApi, OpenAPI currentApi, String description) {
        Matcher<OpenAPI> matcher = isCompatibleWith(baseApi);
        assertThat(matcher.matches(currentApi), is(false));

        StringDescription stringDescription = new StringDescription();
        matcher.describeMismatch(currentApi, stringDescription);
        assertThat(stringDescription.toString(), is(description));
    }

    private static PathItem createPathItem() {
        return new PathItem().get(new Operation().addParametersItem(new Parameter().name("param")
                .schema(new Schema().$ref("#/components/schemas/testSchema"))));
    }
}
