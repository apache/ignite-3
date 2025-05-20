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

package org.apache.ignite.migrationtools.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ConfigurationConverterTest {
    private static Stream<String> configPaths() {
        return ConfigExamples.configPaths();
    }

    private static Stream<Arguments> nodePorts() {
        return Stream.of(
                Arguments.of("configs-custom/ignite-config.0.xml", 49500),
                Arguments.of("configs-custom/ignite-config.1.xml", 49501),
                Arguments.of("configs-custom/ignite-config.2.xml", 40500)
        );
    }

    private static Stream<Arguments> apiPorts() {
        return Stream.of(
                Arguments.of("configs-custom/ignite-config.0.xml", null),
                Arguments.of("configs-custom/ignite-config.1.xml", null),
                Arguments.of("configs-custom/ignite-config.2.xml", 10801)
        );
    }

    private static Stream<Arguments> dataStorageConfigs() {
        return Stream.of(
                Arguments.of("configs-custom/ignite-config.0.xml", List.of(
                    (Consumer<Map.Entry<String, Map<String, Object>>>) e -> {
                        var name = e.getKey();
                        var conf = e.getValue();

                        assertThat(name).isEqualTo("default");
                        assertThat(conf.get("engine")).isEqualTo("aimem");
                        assertThat(conf.get("initSizeBytes")).isEqualTo(100 * 1024 * 1024);
                    },
                    (Consumer<Map.Entry<String, Map<String, Object>>>) e -> {
                        var name = e.getKey();
                        var conf = e.getValue();

                        assertThat(name).isEqualTo("40MB_Region_Eviction");
                        assertThat(conf.get("engine")).isEqualTo("aimem");
                        assertThat(conf.get("initSizeBytes")).isEqualTo(20 * 1024 * 1024);
                        assertThat(conf.get("maxSizeBytes")).isEqualTo(40 * 1024 * 1024);
                    }
                )),
                Arguments.of("configs-custom/ignite-config.1.xml", List.of(
                    (Consumer<Map.Entry<String, Map<String, Object>>>) e -> {
                        var name = e.getKey();
                        var conf = e.getValue();

                        assertThat(name).isEqualTo("default");
                        assertThat(conf.get("engine")).isEqualTo("aimem");
                        assertThat(conf.get("initSizeBytes")).isEqualTo(50 * 1024 * 1024);
                        assertThat(conf.get("maxSizeBytes")).isEqualTo(150 * 1024 * 1024);
                    }
                )),
                Arguments.of("configs-custom/ignite-config.2.xml", List.of(
                    (Consumer<Map.Entry<String, Map<String, Object>>>) e -> {
                            var name = e.getKey();
                            var conf = e.getValue();

                            assertThat(name).isEqualTo("default");
                            assertThat(conf.get("engine")).isEqualTo("aipersist");
                            assertThat(conf.get("sizeBytes")).isEqualTo(50 * 1024 * 1024);
                        }
                ))
        );
    }

    private static Stream<Arguments> clusterNodes() {
        var cfg0 = Stream.concat(IntStream.rangeClosed(49500, 49520).boxed(), IntStream.rangeClosed(4500, 5000).boxed())
                .map(i -> "127.0.0.1:" + i)
                .collect(Collectors.toList());

        return Stream.of(
                Arguments.of("configs-custom/ignite-config.0.xml", cfg0),
                Arguments.of("configs-custom/ignite-config.1.xml", List.of("node1:49500", "node2:49500", "node3:49500")),
                Arguments.of("configs-custom/ignite-config.2.xml", List.of("node1.my-company.com:47500", "node2.my-company.com:47500"))
        );
    }

    private static Stream<Arguments> sslConfigs() {
        Consumer<Config> notConfigured = c -> assertThat(c.hasPath("network.ssl")).isFalse();

        BiConsumer<Config, String> c1 = (c, module) -> {
            assertThat(c.getString(module + ".ssl.keyStore.password")).isEqualTo("123456");
            assertThat(c.getString(module + ".ssl.keyStore.path")).isEqualTo("keystore/node.jks");
            assertThat(c.getString(module + ".ssl.trustStore.password")).isEqualTo("123456");
            assertThat(c.getString(module + ".ssl.trustStore.path")).isEqualTo("keystore/trust.jks");
            assertThat(c.hasPath(module +  ".ssl.clientAuth")).isFalse();
            assertThat(c.hasPath(module +  ".ssl.ciphers")).isFalse();
        };

        BiConsumer<Config, String> c2 = (c, module) -> {
            assertThat(c.getString(module + ".ssl.ciphers")).isEqualTo("Blowfish");
            assertThat(c.getString(module + ".ssl.clientAuth")).isEqualTo("NONE");
            assertThat(c.getString(module + ".ssl.keyStore.password")).isEqualTo("123456");
            assertThat(c.getString(module + ".ssl.keyStore.path")).isEqualTo("keystore/node.jks");
        };

        Function<BiConsumer<Config, String>, Consumer<Config>> binder = checker -> cfg -> {
            checker.accept(cfg, "network");
            checker.accept(cfg, "clientConnector");
        };

        return Stream.of(
                Arguments.of("configs-custom/ignite-config.0.xml", binder.apply(c1)),
                Arguments.of("configs-custom/ignite-config.1.xml", notConfigured),
                Arguments.of("configs-custom/ignite-config.2.xml", binder.apply(c2))
        );
    }

    @Order(1)
    @ParameterizedTest
    @MethodSource("configPaths")
    void noExceptionIsThrownWhenConverting(String inputPath) throws Exception {
        ConfigTestUtils.loadResourceFile(inputPath);
    }

    @ParameterizedTest
    @MethodSource("nodePorts")
    void testNodePort(String inputPath, Integer expectedNodePort) throws Exception {
        var testDescr = ConfigTestUtils.loadResourceFile(inputPath);
        Config cfg = testDescr.getNodeCfg().getConfig("ignite");
        if (expectedNodePort != null) {
            var port = cfg.getInt("network.port");
            assertThat(port).isEqualTo(expectedNodePort);
        } else {
            assertThat(cfg.hasPath("network.port")).isFalse();
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("clusterNodes")
    void testClusterNodes(String inputPath, List<String> expectedClusterNodes) throws Exception {
        var testDescr = ConfigTestUtils.loadResourceFile(inputPath);
        Config cfg = testDescr.getNodeCfg().getConfig("ignite");
        var key = "network.nodeFinder.netClusterNodes";
        if (expectedClusterNodes != null) {
            var port = cfg.getStringList(key);
            assertThat(port).containsExactlyElementsOf(expectedClusterNodes);
        } else {
            assertThat(cfg.hasPath(key)).isFalse();
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("sslConfigs")
    void testSslConfigs(String inputPath, Consumer<Config> configAssertions) throws Exception {
        var testDescr = ConfigTestUtils.loadResourceFile(inputPath);
        Config cfg = testDescr.getNodeCfg().getConfig("ignite");
        configAssertions.accept(cfg);
    }

    @ParameterizedTest
    @MethodSource("apiPorts")
    void testClientApiPort(String inputPath, Integer expectedPort) throws Exception {
        var testDescr = ConfigTestUtils.loadResourceFile(inputPath);
        Config cfg = testDescr.getNodeCfg().getConfig("ignite");
        if (expectedPort != null) {
            var port = cfg.getInt("clientConnector.port");
            assertThat(port).isEqualTo(expectedPort);
        } else {
            assertThat(cfg.hasPath("clientConnector.port")).isFalse();
        }
    }

    @ParameterizedTest
    @MethodSource("dataStorageConfigs")
    void testDataStorageConfigurations(String inputPath, List<Consumer<Map.Entry<String, Map<String, Object>>>> reqs) throws Exception {
        var testDescr = ConfigTestUtils.loadResourceFile(inputPath);
        Config cfg = testDescr.getNodeCfg().getConfig("ignite");

        Set<Map.Entry<String, Object>> profiles = cfg.getList("storage.profiles").stream().collect(Collectors.toMap(
                (ConfigValue o) -> ((Map<String, Object>) o.unwrapped()).get("name").toString(),
                ConfigValue::unwrapped
        )).entrySet();
        assertThat(profiles).satisfiesExactlyInAnyOrder(reqs.toArray(new Consumer[0]));
    }

    @Test
    void testDefaultsAreNotWritten() throws Exception {
        var testDescr = ConfigTestUtils.loadResourceFile("configs-custom/empty-config.xml");
        assertThat(Files.size(testDescr.getNodeCfgPath())).isZero();
    }
}
