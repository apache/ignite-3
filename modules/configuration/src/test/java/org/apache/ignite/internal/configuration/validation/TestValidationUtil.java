/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.jetbrains.annotations.Nullable;
import org.mockito.ArgumentCaptor;

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
        ValidationContext<VIEWT> mock = mock(ValidationContext.class);

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
    public static <VIEWT> ArgumentCaptor<ValidationIssue> mockAddIssue(ValidationContext<VIEWT> mock) {
        ArgumentCaptor<ValidationIssue> issuesCaptor = ArgumentCaptor.forClass(ValidationIssue.class);

        doNothing().when(mock).addIssue(issuesCaptor.capture());

        return issuesCaptor;
    }
}
