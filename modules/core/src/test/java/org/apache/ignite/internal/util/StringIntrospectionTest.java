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

package org.apache.ignite.internal.util;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class StringIntrospectionTest {

    private static final String ASCII = "ASCII";
    private static final String NOT_ASCII_BUT_LATIN1 = "Not ASCII: é";

    @Test
    void asciiStringsSupportFastGetLatin1Bytes() {
        assertTrue(StringIntrospection.supportsFastGetLatin1Bytes(ASCII));
    }

    @Test
    void latin1StringsSupportsFastGetLatin1Bytes() {
        assertTrue(StringIntrospection.supportsFastGetLatin1Bytes(NOT_ASCII_BUT_LATIN1));
    }

    @Test
    void nonLatin1StringsDoNotSupportFastGetLatin1Bytes() {
        assertFalse(StringIntrospection.supportsFastGetLatin1Bytes("Not Latin1: кириллица"));
    }

    @Test
    void fastAsciiBytesReturnsCorrectBytes() {
        byte[] asciiBytes = StringIntrospection.fastAsciiBytes(ASCII);

        assertThat(asciiBytes, is(equalTo(ASCII.getBytes(UTF_8))));
    }

    @Test
    void fastLatin1BytesReturnsCorrectBytes() {
        byte[] asciiBytes = StringIntrospection.fastLatin1Bytes(NOT_ASCII_BUT_LATIN1);

        assertThat(asciiBytes, is(equalTo(NOT_ASCII_BUT_LATIN1.getBytes(ISO_8859_1))));
    }
}
