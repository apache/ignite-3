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
import org.apache.ignite.internal.cli.core.style.element.UiElements;
import org.apache.ignite.internal.cli.core.style.element.UiString;
import org.apache.ignite.internal.util.ArrayUtils;

/**
 * UI component that represents a question.
 */
public class QuestionUiComponent implements UiComponent {
    private final String question;

    private final UiElement[] questionUiElements;

    private QuestionUiComponent(
            String question,
            UiElement[] questionUiElements) {
        this.question = question;
        this.questionUiElements = questionUiElements;
    }

    @Override
    public String render() {
        return ansi(UiString.format(question, questionUiElements));
    }

    public static QuestionUiComponent fromQuestion(String question, UiElement... questionUiElements) {
        return builder().question(question, questionUiElements).build();
    }

    public static QuestionUiComponent fromYesNoQuestion(String question, UiElement... questionUiElements) {
        return fromQuestion(question + " %s ", ArrayUtils.concat(questionUiElements, UiElements.yesNo()));
    }

    /** Builder. */
    public static MessageComponentBuilder builder() {
        return new MessageComponentBuilder();
    }

    /** Builder. */
    public static class MessageComponentBuilder {
        private String question;

        private UiElement[] questionUiElements;

        /** Sets question. */
        public MessageComponentBuilder question(String question, UiElement... uiElements) {
            this.question = question;
            this.questionUiElements = uiElements;
            return this;
        }

        public QuestionUiComponent build() {
            return new QuestionUiComponent(question, questionUiElements);
        }
    }
}
