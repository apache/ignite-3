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

package org.apache.ignite.internal.configuration.tree;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.lenient;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.UUID;
import org.apache.ignite.configuration.annotation.Secret;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConverterToMapVisitorTest extends BaseIgniteAbstractTest {

    private final ConverterToMapVisitor maskingVisitor = ConverterToMapVisitor.builder()
            .maskSecretValues(true)
            .build();

    private final ConverterToMapVisitor notMaskingVisitor = ConverterToMapVisitor.builder()
            .maskSecretValues(false)
            .build();

    @Mock
    Field secretField;

    @Mock
    Field notSecretField;


    @BeforeEach
    void setUp() {
        lenient().when(secretField.isAnnotationPresent(Secret.class)).thenReturn(true);
        lenient().when(notSecretField.isAnnotationPresent(Secret.class)).thenReturn(false);
    }

    @Test
    public void notSecretStringNotMasked() {
        Object password = maskingVisitor.visitLeafNode(notSecretField, null, "password");
        assertEquals("password", password.toString());
    }

    @Test
    public void secretStringIsMasked() {
        Object password = maskingVisitor.visitLeafNode(secretField, null, "password");
        assertThat(password.toString(), matchesPattern("^[\\*]+$"));
    }

    @Test
    public void secretStringIsNotMaskedIfMaskSecretValuesIsFalse() {
        Object password = notMaskingVisitor.visitLeafNode(secretField, null, "password");
        assertEquals("password", password.toString());
    }

    @Test
    public void secretStringsHaveSameLength() {
        Object string1 = maskingVisitor.visitLeafNode(secretField, null, "password");
        Object string2 = maskingVisitor.visitLeafNode(secretField, null, "admin");
        assertEquals(string1.toString().length(), string2.toString().length());
    }

    @Test
    public void notStringsNotMasked() {
        // int value
        assertEquals(1, maskingVisitor.visitLeafNode(secretField, null, 1));

        // long value
        assertEquals(1L, maskingVisitor.visitLeafNode(secretField, null, 1L));

        // double value
        assertEquals(1.1, maskingVisitor.visitLeafNode(secretField, null, 1.1));

        // Character value
        assertEquals("a", maskingVisitor.visitLeafNode(secretField, null, 'a'));

        // UUID value
        UUID uuid = UUID.randomUUID();
        assertEquals(uuid.toString(), maskingVisitor.visitLeafNode(secretField, null, uuid));

        // array value
        String[] strings = {"a", "b", "c", "d"};
        assertEquals(Arrays.asList(strings), maskingVisitor.visitLeafNode(secretField, null, strings));
    }
}
