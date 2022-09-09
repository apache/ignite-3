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

package org.apache.ignite.lang;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class ErrorGroupTest {
    @Test
    void extractsCauseMessageFromIgniteExceptionMessage() {
        // Given
        String igniteExceptionMessage = "IGN-CMN-65535 TraceId:24103638-d079-4a19-a8f6-ca9c23662908 I'm the reason";

        // When
        String extractedMessage = ErrorGroup.extractCauseMessage(igniteExceptionMessage);

        // Then
        assertThat(extractedMessage, equalTo("I'm the reason"));
    }

    @Test
    void extractsEmptyCauseMessageFromIgniteExceptionMessage() {
        // Given message without the reason of the error
        String igniteExceptionMessage = "IGN-CMN-65535 TraceId:24103638-d079-4a19-a8f6-ca9c23662908";

        // When
        String extractedMessage = ErrorGroup.extractCauseMessage(igniteExceptionMessage);

        // Then
        assertThat(extractedMessage, equalTo(""));
    }

    @SuppressWarnings({"rawtypes", "OptionalGetWithoutIsPresent"})
    @Test
    void testGroupIdsAreUnique() throws IllegalAccessException {
        Map<Integer, ErrorGroup> errGroups = new HashMap<>();

        for (Class cls : ErrorGroups.class.getDeclaredClasses()) {
            var errGroupField = Arrays.stream(cls.getFields()).filter(f -> f.getName().endsWith("_ERR_GROUP")).findFirst().get();
            var errGroup = (ErrorGroup)errGroupField.get(null);

            var existing = errGroups.putIfAbsent(errGroup.code(), errGroup);

            if (existing != null) {
                fail("Duplicate error group id: " + errGroup.code() + " (" + existing.name() + ", " + errGroup.name() + ")");
            }
        }
    }
}
