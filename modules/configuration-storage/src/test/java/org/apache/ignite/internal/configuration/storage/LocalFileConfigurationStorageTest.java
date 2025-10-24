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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToCompressingWhiteSpace;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasValue;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.configuration.KeyIgnorer;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.PublicName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.Immutable;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.TestConfigurationChanger;
import org.apache.ignite.internal.configuration.hocon.HoconConverter;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Test for local file configurations storage. */
@ExtendWith(WorkDirectoryExtension.class)
// TODO: https://issues.apache.org/jira/browse/IGNITE-19303
public class LocalFileConfigurationStorageTest {

    private static final String CONFIG_NAME = "ignite-config.conf";

    private static ConfigurationTreeGenerator treeGenerator;

    @WorkDirectory
    private Path tmpDir;

    private LocalFileConfigurationStorage storage;

    private TestConfigurationChanger changer;

    @BeforeAll
    public static void beforeAll() {
        treeGenerator = new ConfigurationTreeGenerator(
                List.of(TopConfiguration.KEY, TopEmptyConfiguration.KEY),
                List.of(),
                List.of(FirstNamedListConfigurationSchema.class)
        );
    }

    @AfterAll
    public static void afterAll() {
        treeGenerator.close();
    }

    private Path getConfigFile() {
        return tmpDir.resolve(CONFIG_NAME);
    }

    @BeforeEach
    void before() {
        LocalFileConfigurationModule module = new LocalFileConfigurationModule();
        storage = new LocalFileConfigurationStorage(getConfigFile(), treeGenerator, new InMemoryVaultService(), module);

        changer = new TestConfigurationChanger(
                List.of(TopConfiguration.KEY),
                storage,
                treeGenerator,
                new ConfigurationValidatorImpl(treeGenerator, Set.of()),
                change -> {},
                KeyIgnorer.fromDeletedPrefixes(module.deletedPrefixes())
        );
    }

    @AfterEach
    void after() {
        changer.stop();
    }


    /** Default values are not enriched on read when the config file is empty. */
    @Test
    void empty() throws IOException {
        // Given
        assertThat(configFileContent(), emptyString());

        // When
        var storageValues = readAllLatest();

        // Then
        assertThat(storageValues.entrySet(), hasSize(0));
    }

    /** Named list entities can be added. */
    @Test
    void add() throws Exception {
        // Given
        assertThat(configFileContent(), emptyString());

        // And
        var topConfiguration = (TopConfiguration) treeGenerator.instantiateCfg(TopConfiguration.KEY, changer);

        changer.start();

        topConfiguration.namedList().change(b -> b.create("name1", x -> {
            x.changeStrVal("strVal1");
            x.changeIntVal(-1);
        })).get();

        // When
        var storageValues = readAllLatest();

        // Then the map has updated values
        //
        // top.namedList.<generatedUUID>.strVal  -> strVal1
        // top.namedList.<generatedUUID>.intVal  -> -1
        // top.namedList.<generatedUUID>.<name>  -> name1
        // top.namedList.<ids>.name1             -> "<generatedUUID>"
        // top.namedList.<generatedUUID>.<order> -> 0

        assertThat(storageValues, allOf(aMapWithSize(5), hasValue(-1)));
        assertThat(storageValues, allOf(aMapWithSize(5), hasValue("strVal1")));

        // And
        // Enriched with the defaults
        assertThat(configFileContent(), equalToCompressingWhiteSpace(
                "top {\n"
                        + "    namedList=[\n"
                        + "        {\n"
                        + "            intVal=-1\n"
                        + "            name=name1\n"
                        + "            strVal=strVal1\n"
                        + "        }\n"
                        + "    ]\n"
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
        assertThat(storageValues, allOf(aMapWithSize(10), hasValue(-2)));
        assertThat(storageValues, allOf(aMapWithSize(10), hasValue("strVal2")));
        // And
        assertThat(storageValues, allOf(aMapWithSize(10), hasValue(-1)));
        assertThat(storageValues, allOf(aMapWithSize(10), hasValue("strVal1")));
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
                        + "}\n"
        ));
    }

    /** Update values. */
    @Test
    void update() throws Exception {
        // Given
        assertThat(configFileContent(), emptyString());

        // When
        var topConfiguration = (TopConfiguration) treeGenerator.instantiateCfg(TopConfiguration.KEY, changer);

        changer.start();

        topConfiguration.shortVal().update((short) 3).get();
        // And
        var storageValues = readAllLatest();

        // Then
        assertThat(storageValues, allOf(aMapWithSize(1), hasValue((short) 3)));
        // And
        assertThat(configFileContent(), equalToCompressingWhiteSpace(
                "top {\n"
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

    /** Remove values. */
    @Test
    void remove() throws Exception {
        // Given
        var topConfiguration = (TopConfiguration) treeGenerator.instantiateCfg(TopConfiguration.KEY, changer);

        changer.start();

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
                        + "    shortVal=3\n"
                        + "}\n"
        ));
    }

    /** Delete file before read on recovery. */
    @Test
    void deleteFileBeforeReadOnRecovery() throws IOException {
        // Given
        Files.delete(getConfigFile());

        // When
        var storageValues = storage.readDataOnRecovery().join().values();

        // Then
        assertThat(storageValues.entrySet(), hasSize(0));
        // And empty file was created
        assertThat(configFileContent(), equalTo(""));
    }


    /** File content is not changed when read data on recovery. */
    @Test
    void fileContentIsNotChanged() throws IOException {
        // Given
        String fileContent = "top {\n"
                + "    namedList=[\n"
                + "        {\n"
                + "            intVal=-1\n"
                + "            name=name1\n"
                + "        }\n"
                + "    ]\n"
                + "}\n";

        Files.write(getConfigFile(), fileContent.getBytes(StandardCharsets.UTF_8));

        // When
        var storageValues = storage.readDataOnRecovery().join().values();
        // Then
        assertThat(storageValues, allOf(aMapWithSize(5), hasValue(-1)));
        assertThat(storageValues, allOf(aMapWithSize(5), hasValue("foo"))); // default value
        // And file was not changed
        assertThat(configFileContent(), equalTo(fileContent));
    }

    /** Delete file before read all. */
    @Test
    void deleteFileBeforeReadAll() throws Exception {
        // Given
        Files.delete(getConfigFile());

        // When
        var storageValues = readAllLatest();

        // Then
        assertThat(storageValues.entrySet(), hasSize(0));
        // And there is no file
        assertThat(Files.exists(getConfigFile()), is(false));

        // When update configuration
        changer.start();

        var topConfiguration = (TopConfiguration) treeGenerator.instantiateCfg(TopConfiguration.KEY, changer);
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
                        + "}\n"
        ));
    }

    /** Read configuration when inner node configured with partial content (some fields are empty). */
    @Test
    void innerNodeWithPartialContent() throws Exception {
        // Given
        String content = "top: { inner.boolVal: true }";
        Files.write(getConfigFile(), content.getBytes(StandardCharsets.UTF_8));

        // Expect
        assertThat(storage.readDataOnRecovery().get().values(), aMapWithSize(1));
    }

    /** File content is parsed using HOCON format regardless of the file extension. */
    @Test
    void hoconContentInJsonFile() throws IOException {
        // Given config in JSON format
        String fileContent
                = "{\n"
                + "    \"top\" : {\n"
                + "        \"namedList\" : [\n"
                + "            {\n"
                + "                \"intVal\" : -1,\n"
                + "                \"name\" : \"name1\"\n"
                + "            }\n"
                + "        ]\n"
                + "    }\n"
                + "}\n";

        Path configFile = tmpDir.resolve("ignite-config.json");

        Files.write(configFile, fileContent.getBytes(StandardCharsets.UTF_8));

        // Then check that the JSON is valid
        ConfigParseOptions parseOptions = ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON).setAllowMissing(false);
        assertDoesNotThrow(() -> ConfigFactory.parseFile(configFile.toFile(), parseOptions));

        LocalFileConfigurationStorage storage = new LocalFileConfigurationStorage(configFile, treeGenerator, new InMemoryVaultService(), null);

        // And storage reads the file successfully
        assertDoesNotThrow(storage::readDataOnRecovery);
    }

    @Test
    void testValidateDuplicates() throws IOException {
        // Given config in JSON format
        String fileContent
                = "top {\n"
                + "    inner {\n"
                + "        boolVal=false,\n"
                + "        boolVal=true\n"
                + "        someConfigurationValue {\n"
                + "            intVal=1\n"
                + "            strVal=foo\n"
                + "        }\n"
                + "        strVal=foo\n"
                + "    }\n"
                + "    shortVal=3\n"
                + "}\n";

        Path configFile = getConfigFile();

        Files.write(configFile, fileContent.getBytes(StandardCharsets.UTF_8));

        // And storage detects duplicates
        assertThrows(
                ConfigurationChangeException.class,
                changer::start,
                "Validation did not pass for keys: [top.inner.boolVal, Duplicated key]"
        );
    }

    @Test
    void testReadDataOnStartupWithDeletedProperty() throws IOException {
        // Given config in JSON format
        String fileContent = "top.deleted_property = 3";

        Path configFile = getConfigFile();

        Files.write(configFile, fileContent.getBytes(StandardCharsets.UTF_8));

        // Deleted properties are ignored and removed from the storage.
        assertDoesNotThrow(changer::start);
        assertThat(storage.readLatest("top.deleted_property"), willBe(nullValue()));
    }

    @Test
    void testReadDataOnStartupWithRenamedProperty() throws IOException {
        // Given config in JSON format
        String fileContent = "top.oldShortValName = 3";

        Path configFile = getConfigFile();

        Files.write(configFile, fileContent.getBytes(StandardCharsets.UTF_8));

        // Storage handles renamed property.
        assertDoesNotThrow(changer::start);

        assertThat(storage.readLatest("top.shortVal"), willBe((short) 3));
    }

    @Test
    void testReadOnly() throws Exception {
        Path configFile = tmpDir.resolve(CONFIG_NAME + "-read-only");
        File file = configFile.toFile();
        assertTrue(file.createNewFile());
        assertTrue(file.setReadOnly());

        assertFalse(Files.isWritable(configFile));

        var storage = new LocalFileConfigurationStorage(
                configFile,
                treeGenerator,
                new InMemoryVaultService(),
                new LocalFileConfigurationModule()
        );
        assertThat(storage.write(new WriteEntryImpl(Map.of(), storage.localRevision().get() + 1)), willCompleteSuccessfully());
    }

    @Test
    void test() throws Exception {
        Path configFile = tmpDir.resolve(CONFIG_NAME + "test");
        File file = configFile.toFile();

        assertFalse(Files.isWritable(configFile));

        var storage = new LocalFileConfigurationStorage(
                configFile,
                treeGenerator,
                new InMemoryVaultService(),
                new LocalFileConfigurationModule()
        );
        assertThat(storage.write(new WriteEntryImpl(Map.of(), storage.localRevision().get() + 1)), willCompleteSuccessfully());
    }

    @Test
    void updateAfterRestart() throws Exception {
        assertThat(configFileContent(), emptyString());

        // Initialize value
        com.typesafe.config.Config config = ConfigFactory.parseString("top.polyNamedList = [{name=name1,strVal=foo,type=first}]");
        ConfigurationSource source = HoconConverter.hoconSource(config.root());

        changer.start();
        assertThat(changer.change(source), willCompleteSuccessfully());

        // Force restart of the storage
        after();
        before();

        config = ConfigFactory.parseString("top.polyNamedList.name1.strVal=strVal1");
        source = HoconConverter.hoconSource(config.root());

        changer.start();
        assertThat(changer.change(source), willCompleteSuccessfully());
    }

    private String configFileContent() throws IOException {
        return Files.readString(getConfigFile());
    }

    private Map<String, ? extends Serializable> readAllLatest() {
        return storage.readAllLatest("").join();
    }

    /** Root that has a single named list. */
    @ConfigurationRoot(rootName = "top")
    public static class TopConfigurationSchema {
        @NamedConfigValue
        public NamedListConfigurationSchema namedList;

        @ConfigValue
        public InnerConfigurationSchema inner;

        @Value(hasDefault = true)
        @PublicName(legacyNames = "oldShortValName")
        public short shortVal = 1;

        @Deprecated
        @Value(hasDefault = true)
        public int deprecated = 0;

        @NamedConfigValue
        public PolyNamedListConfigurationSchema polyNamedList;

        @Immutable
        @Value(hasDefault = true)
        public int immutable = 0;
    }


    /** Empty root that is needed to test empty configuration root rendering to file. */
    @ConfigurationRoot(rootName = "emptyTop")
    public static class TopEmptyConfigurationSchema {
        @Value(hasDefault = true)
        public short ignore = 1;
    }

    /** Inner config to test that it won't be saved to the file if not changed by the user.*/
    @Config
    public static class InnerConfigurationSchema {
        @Value(hasDefault = true)
        public String strVal = "foo";

        @Value(hasDefault = true)
        public boolean boolVal = false;

        @ConfigValue
        public NamedListConfigurationSchema someConfigurationValue;
    }

    /** Named list element node that contains another named list. */
    @Config
    public static class NamedListConfigurationSchema {
        @Value(hasDefault = true)
        public String strVal = "foo";

        @Value(hasDefault = true)
        public int intVal = 1;
    }

    /**
     * Polymorphic configuration schema.
     */
    @PolymorphicConfig
    public static class PolyNamedListConfigurationSchema {
        @PolymorphicId
        public String type;

        @InjectedName
        public String name;
    }

    /**
     * Simple implementation of polymorphic base config.
     */
    @PolymorphicConfigInstance("first")
    public static class FirstNamedListConfigurationSchema extends PolyNamedListConfigurationSchema {
        @Value(hasDefault = true)
        public String strVal = "foo";
    }
}
