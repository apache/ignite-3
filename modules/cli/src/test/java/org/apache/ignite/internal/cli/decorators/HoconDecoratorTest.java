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

package org.apache.ignite.internal.cli.decorators;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class HoconDecoratorTest {

    private static List<Arguments> testCases() {
        return List.of(
                arguments("{foo=[bar, baz]}", "foo=[\n    bar,\n    baz\n]\n"),
                arguments("foo=[bar, baz]", "foo=[\n    bar,\n    baz\n]\n"),
                arguments("foo=bar", "foo=bar\n"),
                arguments("foo=[]", "foo=[]\n"),
                arguments("[bar, baz]", "[\n    \"bar\",\n    \"baz\"\n]"), // top-level list
                arguments("[{bar=baz}]", "[\n    {\n        \"bar\" : \"baz\"\n    }\n\n]"), // top-level list with objects
                arguments("[]", "[]") // top-level list
        );
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void prettyPrint(String input, String expected) {
        assertThat(HoconDecorator.prettyPrint(input), is(expected));
    }
}
