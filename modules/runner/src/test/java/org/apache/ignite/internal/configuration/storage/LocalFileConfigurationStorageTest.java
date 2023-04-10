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

package org.apache.ignite.internal.configuration.storage;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalToCompressingWhiteSpace;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasValue;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.TestConfigurationChanger;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Test for local file configurations storage. */
@ExtendWith(WorkDirectoryExtension.class)
public class LocalFileConfigurationStorageTest {

    private static final String CONFIG_NAME = "ignite-config.conf";

    private static ConfigurationAsmGenerator cgen;

    @WorkDirectory
    private Path tmpDir;

    /** Test configuration storage. */
    private LocalFileConfigurationStorage storage;

    /** Test configuration changer. */
    private TestConfigurationChanger changer;

    /** Instantiates {@link #cgen}. */
    @BeforeAll
    public static void beforeAll() {
        cgen = new ConfigurationAsmGenerator();
    }

    /** Nullifies {@link #cgen} to prevent memory leak from having runtime ClassLoader accessible from GC root. */
    @AfterAll
    public static void afterAll() {
        cgen = null;
    }

    private Path getConfigFile() {
        return tmpDir.resolve(CONFIG_NAME);
    }

    @BeforeEach
    void before() {
        storage = new LocalFileConfigurationStorage(getConfigFile(), List.of(TopConfiguration.KEY));

        changer = new TestConfigurationChanger(
                cgen,
                List.of(TopConfiguration.KEY),
                Set.of(),
                storage,
                List.of(),
                List.of()
        );

        changer.start();
    }

    @AfterEach
    void after() {
        changer.stop();
    }

    @Test
    @DisplayName("Default values are not added enriched on read when the config file is empty")
    void empty() throws IOException {
        // Given
        assertThat(configFileContent(), emptyString());

        // When
        var storageValues = readAllLatest();

        // Then storage data only contains top level defaults
        assertThat(storageValues.entrySet(), hasSize(1));
    }

    @Test
    @DisplayName("Named list entities can be added")
    void add() throws Exception {
        // Given
        assertThat(configFileContent(), emptyString());
        // And
        var topConfiguration = (TopConfiguration) cgen.instantiateCfg(TopConfiguration.KEY, changer);
        topConfiguration.namedList().change(b -> b.create("name1", x -> {
            x.changeStrVal("strVal1");
            x.changeIntVal(-1);
        })).get();

        // When
        var storageValues = readAllLatest();

        // Then
        assertThat(storageValues, allOf(aMapWithSize(6), hasValue(-1)));
        assertThat(storageValues, allOf(aMapWithSize(6), hasValue("strVal1")));
        // And
        assertThat(configFileContent(), equalToCompressingWhiteSpace(
                "top {\n"
                        + "    namedList=[\n"
                        + "        {\n"
                        + "            intVal=-1\n"
                        + "            name=name1\n"
                        + "            strVal=strVal1\n"
                        + "        }\n"
                        + "    ]\n"
                        + "    shortVal=1\n"
                        + "}"
        ));

        // When
        topConfiguration.namedList().change(b -> b.create("name2", x -> {
            x.changeStrVal("strVal2");
            x.changeIntVal(-2);
        })).get();
        // And
        storageValues = readAllLatest();

        // Then
        assertThat(storageValues, allOf(aMapWithSize(11), hasValue(-2)));
        assertThat(storageValues, allOf(aMapWithSize(11), hasValue("strVal2")));
        // And
        assertThat(storageValues, allOf(aMapWithSize(11), hasValue(-1)));
        assertThat(storageValues, allOf(aMapWithSize(11), hasValue("strVal1")));
        // And
        assertThat(configFileContent(), equalToCompressingWhiteSpace(
                "top {\n"
                        + "    namedList=[\n"
                        + "        {\n"
                        + "            intVal=-1\n"
                        + "            name=name1\n"
                        + "            strVal=strVal1\n"
                        + "        },\n"
                        + "        {\n"
                        + "            intVal=-2\n"
                        + "            name=name2\n"
                        + "            strVal=strVal2\n"
                        + "        }\n"
                        + "    ]\n"
                        + "    shortVal=1\n"
                        + "}\n"
        ));
    }

    @DisplayName("Update values")
    @Test
    void update() throws Exception {
        // Given
        assertThat(configFileContent(), emptyString());

        // When
        var topConfiguration = (TopConfiguration) cgen.instantiateCfg(TopConfiguration.KEY, changer);
        topConfiguration.shortVal().update((short) 3).get();
        // And
        var storageValues = readAllLatest();

        // Then
        assertThat(storageValues, allOf(aMapWithSize(1), hasValue((short) 3)));
        // And
        assertThat(configFileContent(), equalToCompressingWhiteSpace(
                "top {\n"
                        + "    namedList=[]\n"
                        + "    shortVal=3\n"
                        + "}\n"
        ));

        // When create named list entity with defaults
        topConfiguration.namedList().change(b -> b.create("name1", x -> {
        })).get();
        // And
        storageValues = readAllLatest();

        // Then
        assertThat(storageValues, allOf(aMapWithSize(6), hasValue(1)));
        assertThat(storageValues, allOf(aMapWithSize(6), hasValue("foo")));
        // And
        assertThat(configFileContent(), equalToCompressingWhiteSpace(
                "top {\n"
                        + "    namedList=[\n"
                        + "        {\n"
                        + "            intVal=1\n"
                        + "            name=name1\n"
                        + "            strVal=foo\n"
                        + "        }\n"
                        + "    ]\n"
                        + "    shortVal=3\n"
                        + "}"
        ));

        // When update named list entity
        topConfiguration.namedList().change(b -> b.update("name1", x -> {
            x.changeStrVal("strVal1");
            x.changeIntVal(-1);
        })).get();
        // And
        storageValues = readAllLatest();

        // Then
        assertThat(storageValues, allOf(aMapWithSize(6), hasValue(-1)));
        assertThat(storageValues, allOf(aMapWithSize(6), hasValue("strVal1")));
        // And
        assertThat(configFileContent(), equalToCompressingWhiteSpace(
                "top {\n"
                        + "    namedList=[\n"
                        + "        {\n"
                        + "            intVal=-1\n"
                        + "            name=name1\n"
                        + "            strVal=strVal1\n"
                        + "        }\n"
                        + "    ]\n"
                        + "    shortVal=3\n"
                        + "}"
        ));
    }

    @DisplayName("Remove values")
    @Test
    void remove() throws Exception {
        // Given
        var topConfiguration = (TopConfiguration) cgen.instantiateCfg(TopConfiguration.KEY, changer);
        topConfiguration.namedList().change(b -> {
            b.create("name1", x -> {
                x.changeStrVal("strVal1");
                x.changeIntVal(-1);
            });
            b.create("name2", x -> {
                x.changeStrVal("strVal2");
                x.changeIntVal(-2);
            });
        }).get();
        topConfiguration.shortVal().update((short) 3).get();
        // And values are saved to file
        assertThat(configFileContent(), equalToCompressingWhiteSpace(
                "top {\n"
                        + "    namedList=[\n"
                        + "        {\n"
                        + "            intVal=-1\n"
                        + "            name=name1\n"
                        + "            strVal=strVal1\n"
                        + "        },\n"
                        + "        {\n"
                        + "            intVal=-2\n"
                        + "            name=name2\n"
                        + "            strVal=strVal2\n"
                        + "        }\n"
                        + "    ]\n"
                        + "    shortVal=3\n"
                        + "}\n"
        ));

        // When remove named list entity
        topConfiguration.namedList().change(b -> b.delete("name1")).get();
        // And
        var storageValues = readAllLatest();

        // Then
        assertThat(storageValues, allOf(aMapWithSize(6), Matchers.not(hasValue("strVal1"))));
        // And entity removed from file
        assertThat(configFileContent(), equalToCompressingWhiteSpace(
                "top {\n"
                        + "    namedList=[\n"
                        + "        {\n"
                        + "            intVal=-2\n"
                        + "            name=name2\n"
                        + "            strVal=strVal2\n"
                        + "        }\n"
                        + "    ]\n"
                        + "    shortVal=3\n"
                        + "}\n"
        ));

        // When remove the last entity
        topConfiguration.namedList().change(b -> b.delete("name2")).get();
        // And
        storageValues = readAllLatest();

        // Then
        assertThat(storageValues, allOf(aMapWithSize(1), hasValue((short) 3)));
        // And entity removed from file
        assertThat(configFileContent(), equalToCompressingWhiteSpace(
                "top {\n"
                        + "    namedList=[]\n"
                        + "    shortVal=3\n"
                        + "}\n"
        ));
    }

    @DisplayName("Delete file before read on recovery")
    @Test
    void deleteFileBeforeReadOnRecovery() throws IOException {
        // Given
        Files.delete(getConfigFile());

        // When
        var storageValues = storage.readDataOnRecovery().join().values();

        // Then storage data only contains top level defaults
        assertThat(storageValues.entrySet(), hasSize(1));
        // And
        assertThat(configFileContent(), equalToCompressingWhiteSpace(
                "top {\n"
                        + "    namedList=[]\n"
                        + "    shortVal=1\n"
                        + "}\n"
        ));
    }

    @DisplayName("Delete file before read all")
    @Test
    void deleteFileBeforeReadAll() throws Exception {
        // Given
        Files.delete(getConfigFile());

        // When
        var storageValues = readAllLatest();

        // Then storage data only contains top level defaults
        assertThat(storageValues.entrySet(), hasSize(1));
        // And there is no file
        assertThat(Files.exists(getConfigFile()), is(false));

        // When update configuration
        var topConfiguration = (TopConfiguration) cgen.instantiateCfg(TopConfiguration.KEY, changer);
        topConfiguration.namedList().change(b -> b.create("name1", x -> {
                x.changeStrVal("strVal1");
                x.changeIntVal(-1);
            })).get();

        // Then file is created
        assertThat(configFileContent(), equalToCompressingWhiteSpace(
                "top {\n"
                        + "    namedList=[\n"
                        + "        {\n"
                        + "            intVal=-1\n"
                        + "            name=name1\n"
                        + "            strVal=strVal1\n"
                        + "        }\n"
                        + "    ]\n"
                        + "    shortVal=1\n"
                        + "}\n"
        ));
    }

    private String configFileContent() throws IOException {
        return Files.readString(getConfigFile());
    }

    private Map<String, ? extends Serializable> readAllLatest() {
        return storage.readAllLatest("").join();
    }

    // null == remove

    // when read file you can see all values
    //

    /** Root that has a single named list. */
    @ConfigurationRoot(rootName = "top")
    public static class TopConfigurationSchema {
        @NamedConfigValue
        public NamedListConfigurationSchema namedList;

        @Value(hasDefault = true)
        public short shortVal = 1;
    }

    /** Named list element node that contains another named list. */
    @Config
    public static class NamedListConfigurationSchema {
        @Value(hasDefault = true)
        public String strVal = "foo";

        @Value(hasDefault = true)
        public int intVal = 1;
    }
}
