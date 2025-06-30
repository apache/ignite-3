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

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.junit.jupiter.api.Test;

class OpenApiMatcherTest {
    @Test
    void identicalApis() {
        PathItem pathItem = new PathItem()
                .get(new Operation().requestBody(new RequestBody().content(new Content().addMediaType("text/plain", new MediaType()))))
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

        assertThatMismatchedWithDescription(baseApi, currentApi,
                "operation <GET> at path \"test\" has request body <[text/plain]>");
    }

    @Test
    void newRequiredParameter() {
        OpenAPI baseApi = new OpenAPI().path("test", new PathItem().get(new Operation()));
        OpenAPI currentApi = new OpenAPI().path("test", new PathItem().get(new Operation()
                .addParametersItem(new Parameter().name("param").in("path").required(true))));

        assertThatMismatchedWithDescription(baseApi, currentApi,
                "operation <GET> at path \"test\" has new required paramters <[param]>");
    }

    private static void assertThatMismatchedWithDescription(OpenAPI baseApi, OpenAPI currentApi, String description) {
        Matcher<OpenAPI> matcher = isCompatibleWith(baseApi);
        assertThat(matcher.matches(currentApi), is(false));

        StringDescription stringDescription = new StringDescription();
        matcher.describeMismatch(currentApi, stringDescription);
        assertThat(stringDescription.toString(), is(description));
    }
}
