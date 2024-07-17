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

package org.apache.ignite.internal.configuration.validation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.Nullable;
import org.mockito.ArgumentCaptor;
import org.mockito.quality.Strictness;

/**
 * Useful class for testing {@link Validator}.
 */
public class TestValidationUtil {
    /**
     * Creates mock {@link ValidationContext}.
     *
     * @param <VIEWT> Type of the subtree or the value that is being validated.
     * @param oldValue Previous value of the configuration. Might be {@code null} for leaves only.
     * @param newValue Updated value of the configuration.
     */
    public static <VIEWT> ValidationContext<VIEWT> mockValidationContext(@Nullable VIEWT oldValue, VIEWT newValue) {
        ValidationContext<VIEWT> mock = mock(ValidationContext.class, withSettings().strictness(Strictness.LENIENT));

        when(mock.getOldValue()).thenReturn(oldValue);

        when(mock.getNewValue()).thenReturn(newValue);

        return mock;
    }

    /**
     * Adds mocking for method {@link ValidationContext#addIssue}.
     *
     * @param <VIEWT> Type of the subtree or the value that is being validated.
     * @param mock Mocked validation context.
     * @return New {@link ArgumentCaptor}.
     */
    public static <VIEWT> ArgumentCaptor<ValidationIssue> addIssuesCaptor(ValidationContext<VIEWT> mock) {
        ArgumentCaptor<ValidationIssue> issuesCaptor = ArgumentCaptor.forClass(ValidationIssue.class);

        doNothing().when(mock).addIssue(issuesCaptor.capture());

        return issuesCaptor;
    }

    /**
     * Checks that validation completed without errors.
     *
     * @param validator Checked validator.
     * @param annotation Mocked annotation.
     * @param ctx Mocked validation context.
     */
    public static <A extends Annotation, VIEWT> void validate(
            Validator<A, VIEWT> validator,
            A annotation,
            ValidationContext<VIEWT> ctx
    ) {
        validate(validator, annotation, ctx, (String[]) null);
    }

    /**
     * Performs validation checking whether there are errors.
     *
     * @param validator Checked validator.
     * @param annotation Mocked annotation.
     * @param ctx Mocked validation context.
     * @param errorMessagePrefixes Error prefixes, if {@code null} it is expected that there will be no errors.
     */
    public static <A extends Annotation, VIEWT> void validate(
            Validator<A, VIEWT> validator,
            A annotation,
            ValidationContext<VIEWT> ctx,
            String @Nullable ... errorMessagePrefixes
    ) {
        ArgumentCaptor<ValidationIssue> argumentCaptor = addIssuesCaptor(ctx);

        validator.validate(annotation, ctx);

        if (errorMessagePrefixes == null) {
            assertThat(argumentCaptor.getAllValues(), empty());
        } else {
            List<String> messages = argumentCaptor.getAllValues().stream()
                    .map(ValidationIssue::message).collect(Collectors.toList());

            List<Matcher<? super String>> matchers = Arrays.stream(errorMessagePrefixes)
                    .map(Matchers::startsWith).collect(Collectors.toList());

            assertThat(messages, contains(matchers));
        }
    }
}
