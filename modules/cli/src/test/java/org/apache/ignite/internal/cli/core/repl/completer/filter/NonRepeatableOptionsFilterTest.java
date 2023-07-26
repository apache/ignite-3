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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.micronaut.configuration.picocli.MicronautFactory;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.cli.commands.TopLevelCliReplCommand;
import org.apache.ignite.internal.cli.util.CommandSpecUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;

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
        String[] words = {"cluster", "init", "--cluster-name", "name", "--cmg-node", "node"};
        String[] candidates = {"--cluster-name", "--cmg-node", "--cluster-endpoint-url", "--meta-storage-node"};
        List<String> filteredCandidates = Arrays.asList(filter.filter(words, candidates));
        assertThat(filteredCandidates, hasSize(3));
        assertThat(filteredCandidates, containsInAnyOrder("--cmg-node", "--cluster-endpoint-url", "--meta-storage-node"));
    }

    @Test
    void additionalFiltersCalled() {
        // given.
        CommandSpec deployCommand = CommandSpecUtil.findCommand(cmd.getCommandSpec(), "cluster", "unit", "deploy");
        CommandCompleterFilter deployCommandCompleterFilter = mock(CommandCompleterFilter.class);
        doReturn(deployCommand).when(deployCommandCompleterFilter).commandSpec();

        NonRepeatableOptionsFilter filter = new NonRepeatableOptionsFilter(cmd.getCommandSpec(), List.of(deployCommandCompleterFilter));

        // when deploy command is executed.
        String[] deployCommandWords = {"cluster", "unit", "deploy", "--nodes", "node1", "--path", "path1", "unit"};
        String[] deployCommandCandidates = {"--nodes", "--path", "--version"};
        filter.filter(deployCommandWords, deployCommandCandidates);

        // then deploy command completer filter is called.
        verify(deployCommandCompleterFilter, times(1)).filter(deployCommandWords, new String[]{"--nodes", "--version"});
    }

    @Test
    void additionalFiltersNotCalled() {
        // given.
        CommandSpec deployCommand = CommandSpecUtil.findCommand(cmd.getCommandSpec(), "cluster", "unit", "deploy");
        CommandCompleterFilter deployCommandCompleterFilter = mock(CommandCompleterFilter.class);
        doReturn(deployCommand).when(deployCommandCompleterFilter).commandSpec();

        NonRepeatableOptionsFilter filter = new NonRepeatableOptionsFilter(cmd.getCommandSpec(), List.of(deployCommandCompleterFilter));

        // then deploy command completer filter is not called.
        verify(deployCommandCompleterFilter, never()).filter(any(), any());
    }
}
