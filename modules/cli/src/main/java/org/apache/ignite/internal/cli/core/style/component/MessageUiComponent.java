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

import static org.apache.ignite.internal.cli.core.style.AnsiStringSupport.ansi;

import org.apache.ignite.internal.cli.core.style.element.UiElement;
import org.apache.ignite.internal.cli.core.style.element.UiString;

/**
 * UI component that represents a message.
 */
public class MessageUiComponent implements UiComponent {
    private final String message;

    private final UiElement[] messageUiElements;

    private final String hint;

    private final UiElement[] hintUiElements;

    private MessageUiComponent(
            String message,
            UiElement[] messageUiElements,
            String hint,
            UiElement[] hintUiElements) {
        this.message = message;
        this.messageUiElements = messageUiElements;
        this.hint = hint;
        this.hintUiElements = hintUiElements;
    }

    @Override
    public String render() {
        String messageString = UiString.format(message, messageUiElements);
        String hintString = UiString.format(hint, hintUiElements);
        return ansi(
                messageString
                        + (hintString == null ? "" : System.lineSeparator() + hintString)
        );
    }

    public static MessageUiComponent fromMessage(String message, UiElement... messageUiElements) {
        return builder().message(message, messageUiElements).build();
    }

    public static MessageUiComponent from(UiElement messageUiElement) {
        return builder().message("%s", messageUiElement).build();
    }

    /** Builder. */
    public static MessageComponentBuilder builder() {
        return new MessageComponentBuilder();
    }

    /** Builder. */
    public static class MessageComponentBuilder {
        private String message;

        private String hint;

        private UiElement[] messageUiElements;

        private UiElement[] hintUiElements;

        /** Sets message. */
        public MessageComponentBuilder message(String message, UiElement... uiElements) {
            this.message = message;
            this.messageUiElements = uiElements;
            return this;
        }

        /** Sets hint. */
        public MessageComponentBuilder hint(String hint, UiElement... uiElements) {
            this.hint = hint;
            this.hintUiElements = uiElements;
            return this;
        }

        public MessageUiComponent build() {
            return new MessageUiComponent(message, messageUiElements, hint, hintUiElements);
        }
    }
}
