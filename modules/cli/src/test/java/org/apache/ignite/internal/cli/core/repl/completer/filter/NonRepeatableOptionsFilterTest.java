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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

import io.micronaut.configuration.picocli.MicronautFactory;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.cli.commands.TopLevelCliReplCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

@MicronautTest
class NonRepeatableOptionsFilterTest {

    @Inject
    private ApplicationContext ctx;

    private CommandLine cmd;

    @BeforeEach
    void setUp() {
        cmd = new CommandLine(TopLevelCliReplCommand.class, new MicronautFactory(ctx));
    }

    @Test
    void filterNonRepeatableOptions() {
        NonRepeatableOptionsFilter filter = new NonRepeatableOptionsFilter(cmd.getCommandSpec());
        String[] words = {"cluster", "init", "--name", "name", "--cmg-node", "node"};
        String[] candidates = {"--name", "--cmg-node", "--url", "--ms-node"};
        List<String> filteredCandidates = Arrays.asList(filter.filter(words, candidates));
        assertThat(filteredCandidates, hasSize(3));
        assertThat(filteredCandidates, containsInAnyOrder("--cmg-node", "--url", "--ms-node"));
    }
}
