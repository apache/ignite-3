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

package org.apache.ignite.internal.cli.core.style.component;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.UUID;
import org.apache.ignite.internal.cli.core.style.element.UiElements;
import org.junit.jupiter.api.Test;

class ErrorUiComponentTest {
    @Test
    void rendersHeader() {
        // Given
        ErrorUiComponent errorUiComponent = ErrorUiComponent.fromHeader("Just single header");

        // When
        String rendered = errorUiComponent.render();

        // Then
        assertThat(rendered, equalTo("Just single header"));
    }

    @Test
    void rendersAllParts() {
        // Given
        ErrorUiComponent errorUiComponent = ErrorUiComponent.builder()
                .header("I am header")
                .errorCode("IGN-TBL-01")
                .traceId(UUID.fromString("a0c5aca8-73ab-4e7e-bf7a-64fdcf08ece7"))
                .details("Some useful information about the error happened")
                .build();

        // When
        String rendered = errorUiComponent.render();

        // Then
        assertThat(rendered, equalTo(
                "IGN-TBL-01 Trace ID: a0c5aca8-73ab-4e7e-bf7a-64fdcf08ece7" + System.lineSeparator()
                + "I am header" + System.lineSeparator()
                + "Some useful information about the error happened"
        ));
    }

    @Test
    void renderAllPartsWithElements() {
        // Given
        ErrorUiComponent errorUiComponent = ErrorUiComponent.builder()
                .header("I am header with url %s", UiElements.url("http://host.com"))
                .errorCode("IGN-TBL-01")
                .traceId(UUID.fromString("a0c5aca8-73ab-4e7e-bf7a-64fdcf08ece7"))
                .details("Some useful information about the error happened with url %s", UiElements.url("http://host.com"))
                .build();

        // When
        String rendered = errorUiComponent.render();

        // Then
        assertThat(rendered, equalTo(
                "IGN-TBL-01 Trace ID: a0c5aca8-73ab-4e7e-bf7a-64fdcf08ece7" + System.lineSeparator()
                        + "I am header with url http://host.com" + System.lineSeparator()
                        + "Some useful information about the error happened with url http://host.com"
        ));
    }
}
