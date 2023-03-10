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

package org.apache.ignite.internal.cli.core.repl.completer.filter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.cli.commands.Options;
import org.junit.jupiter.api.Test;

class ShortOptionsFilterTest {

    @Test
    void filterShortOptions() {
        String[] allOptions = Arrays.stream(Options.values())
                .flatMap(it -> Stream.of(it.fullName(), it.shortName()))
                .distinct()
                .toArray(String[]::new);
        String[] candidates = new ShortOptionsFilter().filter(new String[0], allOptions);
        List<String> longOptionNames = Arrays.stream(Options.values())
                .map(Options::fullName)
                .distinct()
                .collect(Collectors.toList());
        assertThat(candidates, arrayWithSize(longOptionNames.size()));
        assertThat(Arrays.asList(candidates), containsInAnyOrder(longOptionNames.toArray(new String[0])));
    }
}
