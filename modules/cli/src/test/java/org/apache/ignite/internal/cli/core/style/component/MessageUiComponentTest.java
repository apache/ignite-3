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

import org.apache.ignite.internal.cli.core.style.element.UiElements;
import org.junit.jupiter.api.Test;

class MessageUiComponentTest {
    @Test
    void rendersMessageAndHint() {
        // Given
        var messageUiComponent = MessageUiComponent.builder()
                .message("Hello, I am a message")
                .hint("And I am a hint")
                .build();

        // When
        String renderedUiComponent = messageUiComponent.render();

        // Then
        assertThat(renderedUiComponent, equalTo("Hello, I am a message" + System.lineSeparator() + "And I am a hint"));
    }

    @Test
    void rendersMessage() {
        // Given message without a hint
        var messageUiComponent = MessageUiComponent.fromMessage("Hello, I am just a message");

        // When
        String renderedUiComponent = messageUiComponent.render();

        // Then there is no \n at the end
        assertThat(renderedUiComponent, equalTo("Hello, I am just a message"));
    }

    @Test
    void rendersMessageWithUrl() {
        // Given
        MessageUiComponent messageUiComponent = MessageUiComponent.fromMessage(
                "This is a message with url %s",
                UiElements.url("https://host.com")
        );

        // When
        String renderedUiComponent = messageUiComponent.render();

        // Then
        assertThat(renderedUiComponent, equalTo("This is a message with url https://host.com"));
    }
}
