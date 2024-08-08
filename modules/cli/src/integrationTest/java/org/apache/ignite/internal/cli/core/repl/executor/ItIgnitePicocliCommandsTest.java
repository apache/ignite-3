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

package org.apache.ignite.internal.cli.core.repl.executor;

import static java.util.stream.Collectors.flatMapping;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.ClusterPerTestIntegrationTest.FAST_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.when;

import jakarta.inject.Inject;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.apache.ignite.internal.cli.commands.TopLevelCliReplCommand;
import org.apache.ignite.internal.cli.core.repl.Session;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.internal.cli.core.repl.completer.DynamicCompleterActivationPoint;
import org.apache.ignite.internal.cli.core.repl.completer.DynamicCompleterRegistry;
import org.apache.ignite.internal.cli.core.repl.completer.filter.CompleterFilter;
import org.apache.ignite.internal.cli.core.repl.completer.filter.DynamicCompleterFilter;
import org.apache.ignite.internal.cli.core.repl.completer.filter.NonRepeatableOptionsFilter;
import org.apache.ignite.internal.cli.core.repl.completer.filter.ShortOptionsFilter;
import org.apache.ignite.internal.cli.event.EventPublisher;
import org.apache.ignite.internal.cli.event.Events;
import org.apache.ignite.internal.configuration.ServiceLoaderModulesProvider;
import org.assertj.core.util.Files;
import org.jline.reader.Candidate;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.completer.SystemCompleter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/** Integration test for all completions in interactive mode. */
public class ItIgnitePicocliCommandsTest extends CliIntegrationTest {

    private static final String DEFAULT_REST_URL = "http://localhost:10300";

    private static final List<String> DISTRIBUTED_CONFIGURATION_KEYS;

    private static final List<String> LOCAL_CONFIGURATION_KEYS;

    static {
        Map<ConfigurationType, List<String>> configKeysByType = new ServiceLoaderModulesProvider()
                .modules(ItIgnitePicocliCommandsTest.class.getClassLoader())
                .stream()
                .collect(groupingBy(
                        ConfigurationModule::type,
                        flatMapping(module -> module.rootKeys().stream().map(RootKey::key), toUnmodifiableList())
                ));

        DISTRIBUTED_CONFIGURATION_KEYS = configKeysByType.get(ConfigurationType.DISTRIBUTED);
        LOCAL_CONFIGURATION_KEYS = configKeysByType.get(ConfigurationType.LOCAL);
    }

    @Inject
    DynamicCompleterRegistry dynamicCompleterRegistry;

    @Inject
    DynamicCompleterActivationPoint dynamicCompleterActivationPoint;

    @Inject
    DynamicCompleterFilter dynamicCompleterFilter;

    @Inject
    Session session;

    @Inject
    EventPublisher eventPublisher;

    SystemCompleter completer;

    LineReader lineReader;

    @Override
    protected Class<?> getCommandClass() {
        return TopLevelCliReplCommand.class;
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return FAST_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE;
    }

    @BeforeEach
    void setupSystemCompleter() {
        dynamicCompleterActivationPoint.activateDynamicCompleter(dynamicCompleterRegistry);

        List<CompleterFilter> filters = List.of(
                dynamicCompleterFilter,
                new ShortOptionsFilter(),
                new NonRepeatableOptionsFilter(commandLine().getCommandSpec())
        );

        IgnitePicocliCommands commandRegistry = new IgnitePicocliCommands(commandLine(), dynamicCompleterRegistry, filters);

        // This completer is used by jline to suggest all completions
        completer = commandRegistry.compileCompleters();
        completer.compile();

        lineReader = Mockito.mock(LineReader.class);
        when(lineReader.getParser()).thenReturn(new DefaultParser());
    }

    private Stream<Arguments> helpAndVerboseAreNotCompletedSource() {
        return Stream.of(
                words(""), words("-"), words(" -"),
                words("node"),
                words("node", ""),
                words("node", "config"),
                words("node", "config", ""),
                words("node", "config", "show"),
                words("node", "config", "show", ""),
                words("node", "config", "show", "--node", "name"),
                words("node", "config", "show", "--node", "name", "")
        ).map(this::named).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("helpAndVerboseAreNotCompletedSource")
    @DisplayName("--help and --verbose are not suggested")
    void helpAndVerboseAreNotCompleted(ParsedLine givenParsedLine) {
        // When
        List<String> suggestions = complete(givenParsedLine);

        // Then
        assertThat(
                "For given parsed words: " + givenParsedLine.words(),
                suggestions,
                not(hasItems("--help", "--verbose"))
        );
    }

    private Stream<Arguments> helpAndVerboseCompletedSource() {
        return Stream.of(
                words("node", "config", "show", "-"),
                words("node", "config", "show", "--"),
                words("node", "status", "-"),
                words("node", "status", "--"),
                words("node", "config", "show", "--node", "name", "-")
        ).map(this::named).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("helpAndVerboseCompletedSource")
    @DisplayName("--help and --verbose are suggested if '-' or '--' typed")
    void helpAndVerboseAreCompleted(ParsedLine givenParsedLine) {
        // When
        List<String> suggestions = complete(givenParsedLine);

        // Then
        assertThat(
                "For given parsed words: " + givenParsedLine.words(),
                suggestions,
                hasItems("--verbose", "--help")
        );
    }

    private Stream<Arguments> helpCompletedSource() {
        return Stream.of(
                words("node", "-"),
                words("node", "", "-"),
                words("node", "config", "-"),
                words("node", "config", "--"),
                words("node", "config", " ", "-")
        ).map(this::named).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("helpCompletedSource")
    @DisplayName("--help suggested if '-' or '--' typed for keywords that is not complete commands")
    void helpSuggested(ParsedLine givenParsedLine) {
        // When
        List<String> suggestions = complete(givenParsedLine);

        // Then
        assertThat(
                "For given parsed words: " + givenParsedLine.words(),
                suggestions,
                hasItem("--help")
        );
    }

    private Stream<Arguments> nodeConfigShowSuggestedSource() {
        return Stream.of(
                words("node", "config", "show", ""),
                words("node", "config", "show", " --node", "nodeName", ""),
                words("node", "config", "show", " --verbose", ""),
                words("node", "config", "show", " -v", "")
        ).map(this::named).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("nodeConfigShowSuggestedSource")
    @DisplayName("node config show selector parameters suggested")
    void nodeConfigShowSuggested(ParsedLine givenParsedLine) {
        // Given
        connected();

        // wait for lazy init of node config completer
        await("For given parsed words: " + givenParsedLine.words()).until(
                () -> complete(givenParsedLine),
                containsInAnyOrder(LOCAL_CONFIGURATION_KEYS.toArray(String[]::new))
        );
    }

    private void connected() {
        eventPublisher.publish(Events.connect(SessionInfo.builder().nodeUrl(DEFAULT_REST_URL).build()));
    }

    private Stream<Arguments> nodeConfigUpdateSuggestedSource() {
        return Stream.of(
                words("node", "config", "update", ""),
                words("node", "config", "update", " --node", "nodeName", ""),
                words("node", "config", "update", " --verbose", ""),
                words("node", "config", "update", " -v", "")
        ).map(this::named).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("nodeConfigUpdateSuggestedSource")
    @DisplayName("node config update selector parameters suggested")
    void nodeConfigUpdateSuggested(ParsedLine givenParsedLine) {
        // Given
        connected();

        // wait for lazy init of node config completer
        await("For given parsed words: " + givenParsedLine.words()).until(
                () -> complete(givenParsedLine),
                containsInAnyOrder(
                        "rest",
                        "clientConnector",
                        "network",
                        "deployment",
                        "nodeAttributes",
                        "storage",
                        "criticalWorkers",
                        "sql",
                        "failureHandler",
                        "system"
                )
        );
    }

    private Stream<Arguments> clusterConfigShowSuggestedSource() {
        return Stream.of(
                words("cluster", "config", "show", ""),
                words("cluster", "config", "show", " --node", "nodeName", ""),
                words("cluster", "config", "show", " --verbose", ""),
                words("cluster", "config", "show", " -v", "")
        ).map(this::named).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("clusterConfigShowSuggestedSource")
    @DisplayName("cluster config selector parameters suggested")
    void clusterConfigShowSuggested(ParsedLine givenParsedLine) {
        // Given
        connected();

        // wait for lazy init of cluster config completer
        await("For given parsed words: " + givenParsedLine.words()).until(
                () -> complete(givenParsedLine),
                containsInAnyOrder(DISTRIBUTED_CONFIGURATION_KEYS.toArray(String[]::new))
        );
    }

    private Stream<Arguments> clusterConfigUpdateSuggestedSource() {
        return Stream.of(
                words("cluster", "config", "update", ""),
                words("cluster", "config", "update", " --node", "nodeName", ""),
                words("cluster", "config", "update", " --verbose", ""),
                words("cluster", "config", "update", " -v", "")
        ).map(this::named).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("clusterConfigUpdateSuggestedSource")
    @DisplayName("cluster config selector parameters suggested")
    void clusterConfigUpdateSuggested(ParsedLine givenParsedLine) {
        // Given
        connected();

        // wait for lazy init of cluster config completer
        await("For given parsed words: " + givenParsedLine.words()).until(
                () -> complete(givenParsedLine),
                containsInAnyOrder(DISTRIBUTED_CONFIGURATION_KEYS.toArray(String[]::new))
        );
    }


    private Stream<Arguments> nodeNamesSource() {
        return Stream.of(
                words("cluster", "config", "show", "--node", ""),
                words("cluster", "config", "update", "--node", ""),
                words("cluster", "status", "--node", ""),
                words("cluster", "init", "--node", ""),
                words("cluster", "init", "--cluster-management-group", ""),
                words("cluster", "init", "--metastorage-group", ""),
                words("node", "config", "show", "--node", ""),
                words("node", "config", "show", "--verbose", "--node", ""),
                words("node", "config", "update", "--node", ""),
                words("node", "status", "--node", ""),
                words("node", "version", "--node", ""),
                words("node", "metric", "list", "--node", "")
        ).map(this::named).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("nodeNamesSource")
    @DisplayName("node names suggested after --node option")
    void nodeNameSuggested(ParsedLine givenParsedLine) {
        // Given
        connected();
        // And the first update is fetched
        await().until(() -> nodeNameRegistry.names(), not(empty()));

        // Then
        assertThat(
                "For given parsed words: " + givenParsedLine.words(),
                complete(givenParsedLine),
                containsInAnyOrder(allNodeNames().toArray())
        );
    }

    @Test
    @DisplayName("start/stop node affects --node suggestions")
    void startStopNodeWhenCompleteNodeName() {
        // Given
        int nodeIndex = 1;
        var igniteNodeName = allNodeNames().get(nodeIndex);
        // And
        var givenParsedLine = words("node", "status", "--node", "");
        // And
        assertThat(nodeNameRegistry.names(), empty());

        // Then
        assertThat(complete(givenParsedLine), not(contains(igniteNodeName)));

        // When
        connected();
        // And the first update is fetched
        await().until(() -> nodeNameRegistry.names(), not(empty()));

        // Then
        assertThat(complete(givenParsedLine), containsInAnyOrder(allNodeNames().toArray()));

        // When stop one node
        CLUSTER.stopNode(nodeIndex);
        assertThat(allNodeNames(), not(hasItem(igniteNodeName)));

        // Then node name suggestions does not contain the stopped node
        await().until(() -> complete(givenParsedLine), containsInAnyOrder(allNodeNames().toArray()));

        // When start the node again
        CLUSTER.startNode(nodeIndex);

        // Then node name comes back to suggestions
        await().until(() -> complete(givenParsedLine), containsInAnyOrder(allNodeNames().toArray()));
    }

    private static List<String> allNodeNames() {
        return CLUSTER.runningNodes().map(IgniteImpl::name).collect(toList());
    }

    @Test
    @DisplayName("jdbc url suggested after --jdbc-url option")
    void suggestedJdbcUrl() {
        // Given
        connected();
        // And the first update is fetched
        await().until(() -> jdbcUrlRegistry.jdbcUrls(), not(empty()));

        // Then
        List<String> completions = complete(words("sql", "--jdbc-url", ""));
        assertThat(completions, containsInAnyOrder(jdbcUrlRegistry.jdbcUrls().toArray()));
    }

    private Stream<Arguments> clusterUrlSource() {
        return Stream.of(
                words("cluster", "config", "show", "--url", ""),
                words("cluster", "config", "update", "--url", ""),
                words("cluster", "status", "--url", ""),
                words("cluster", "init", "--url", "")
        ).map(this::named).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("clusterUrlSource")
    @DisplayName("cluster url suggested after --url option")
    void suggestedClusterUrl(ParsedLine parsedLine) {
        // Given
        connected();
        // And the first update is fetched
        await().until(() -> nodeNameRegistry.urls(), not(empty()));

        // Then
        String[] expectedUrls = nodeNameRegistry.urls().toArray(String[]::new);
        assertThat("For given parsed words: " + parsedLine.words(),
                complete(parsedLine),
                containsInAnyOrder(expectedUrls));
    }

    @Test
    @DisplayName("files suggested after -script-file option")
    void suggestedScriptFile() {
        // Given files

        // Create temp folders
        File rootFolder = Files.newTemporaryFolder();
        File emptyFolder = Files.newFolder(rootFolder.getPath() + File.separator + "emptyFolder");
        emptyFolder.deleteOnExit();
        File scriptsFolder = Files.newFolder(rootFolder.getPath() + File.separator + "scriptsFolder");
        scriptsFolder.deleteOnExit();

        // Create temp files
        String script1 = scriptsFolder.getPath() + File.separator + "script1.sql";
        Files.newFile(script1).deleteOnExit();

        String script2 = scriptsFolder.getPath() + File.separator + "script2.sql";
        Files.newFile(script2).deleteOnExit();

        String someFile = scriptsFolder.getPath() + File.separator + "someFile.sql";
        Files.newFile(someFile).deleteOnExit();

        // When complete --file with folder typed
        List<String> completions1 = complete(words("sql", "--file", rootFolder.getPath()));
        // Then completions contain emptyFolder and scriptsFolder
        assertThat(completions1, containsInAnyOrder(emptyFolder.getPath(), scriptsFolder.getPath()));

        List<String> completions2 = complete(words("sql", "--file", scriptsFolder.getPath()));
        // Then completions contain all given files
        assertThat(completions2, containsInAnyOrder(script1, script2, someFile));

        // When complete --file with partial path to script
        List<String> completions3 = complete(words("sql", "--file", scriptsFolder.getPath() + File.separator + "script"));
        // Then completions contain script1 and script2 files
        assertThat(completions3, containsInAnyOrder(script1, script2));
    }

    List<String> complete(ParsedLine typedWords) {
        List<Candidate> candidates = new ArrayList<>();
        completer.complete(lineReader, typedWords, candidates);

        return candidates.stream().map(Candidate::displ).collect(toList());
    }

    Named<ParsedLine> named(ParsedLine parsedLine) {
        return Named.of("cmd: " + String.join(" ", parsedLine.words()), parsedLine);
    }

    ParsedLine words(String... words) {
        return new ParsedLine() {
            @Override
            public String word() {
                return null;
            }

            @Override
            public int wordCursor() {
                return words.length - 1;
            }

            @Override
            public int wordIndex() {
                return words.length;
            }

            @Override
            public List<String> words() {
                return Arrays.stream(words).collect(toList());
            }

            @Override
            public String line() {
                return null;
            }

            @Override
            public int cursor() {
                return 0;
            }
        };
    }
}
