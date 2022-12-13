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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;

import java.util.List;
import org.apache.ignite.internal.cli.core.repl.completer.filter.ExclusionsCompleterFilter;
import org.junit.jupiter.api.Test;

class ExclusionsCompleterFilterTest {

    @Test
    void returnsCandidatesWithoutExclusions() {
        ExclusionsCompleterFilter filter = new ExclusionsCompleterFilter("exclusion1", "exclusion2");
        String[] candidates = filter.filter(new String[0], new String[]{"exclusion1", "exclusion2", "candidate"});
        List<String> candidatesList = List.of(candidates);
        assertThat(candidatesList, hasSize(1));
        assertThat(candidatesList, contains("candidate"));
    }
}
