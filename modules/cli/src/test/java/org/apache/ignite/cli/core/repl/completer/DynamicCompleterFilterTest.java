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

package org.apache.ignite.cli.core.repl.completer;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import org.apache.ignite.cli.core.repl.Session;
import org.junit.jupiter.api.Test;

class DynamicCompleterFilterTest {

    @Test
    void filtersHelp() {
        // Given
        String[] words = new String[]{"cluster", "config", "show", ""};
        // And completion candidates
        String[] candidates = new String[]{"--selector", "--cluster-endpoint-url", "--help", "-h"};
        // And user is not connected to the cluster
        Session session = notConnected();

        // When
        String[] filtered = new DynamicCompleterFilter(session).filter(words, candidates);

        // Then help is filtered out
        assertThat(asList(filtered), containsInAnyOrder("--selector", "--cluster-endpoint-url"));
    }

    private static Session notConnected() {
        Session session = new Session();
        session.setConnectedToNode(false);
        return session;
    }

    private static Session connected() {
        Session session = new Session();
        session.setConnectedToNode(true);
        return session;
    }

    @Test
    void doesNotFilterHelpIfOptionIsTyped() {
        // Given typed words that end with "-"
        String[] words = new String[]{"cluster", "config", "show", "-"};
        // And completion candidates
        String[] candidates = new String[]{"--selector", "--cluster-endpoint-url", "--help", "-h"};
        // And user is not connected to the cluster
        Session session = notConnected();

        // When
        String[] filtered = new DynamicCompleterFilter(session).filter(words, candidates);

        // Then help is NOT filtered out
        assertThat(asList(filtered), containsInAnyOrder("--selector", "--cluster-endpoint-url", "--help", "-h"));
    }

    @Test
    void filtersClusterUrlWhenConnected() {
        // Given typed words that end with "-"
        String[] words = new String[]{"cluster", "config", "show", ""};
        // And completion candidates
        String[] candidates = new String[]{"--selector", "--cluster-endpoint-url", "--help", "-h"};
        // And
        Session session = connected();

        // When
        String[] filtered = new DynamicCompleterFilter(session).filter(words, candidates);

        // Then cluster-endpoint-url and help are filtered out
        assertThat(asList(filtered), containsInAnyOrder("--selector"));
    }

    @Test
    void doesNotFilterHelpIfOptionIsTypedAndConnected() {
        // Given typed words that end with "-"
        String[] words = new String[]{"cluster", "config", "show", "-"};
        // And completion candidates
        String[] candidates = new String[]{"--selector", "--cluster-endpoint-url", "--help", "-h"};
        // And
        Session session = connected();

        // When
        String[] filtered = new DynamicCompleterFilter(session).filter(words, candidates);

        // Then help is NOT filtered out
        assertThat(asList(filtered), containsInAnyOrder("--selector", "--cluster-endpoint-url", "--help", "-h"));
    }
}
