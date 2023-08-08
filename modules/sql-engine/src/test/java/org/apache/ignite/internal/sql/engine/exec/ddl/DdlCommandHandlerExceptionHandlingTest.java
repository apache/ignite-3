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

package org.apache.ignite.internal.sql.engine.exec.ddl;

import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.createZone;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;
import org.apache.ignite.internal.distributionzones.DistributionZoneAlreadyExistsException;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZoneNotFoundException;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.schema.configuration.TableChange;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateZoneCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropZoneCommand;
import org.apache.ignite.internal.storage.impl.TestPersistStorageConfigurationSchema;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests distribution zone command exception handling.
 */
public class DdlCommandHandlerExceptionHandlingTest extends IgniteAbstractTest {
    private DdlCommandHandler commandHandler;

    private static final String ZONE_NAME = "zone1";

    private static final ConfigurationTreeGenerator generator = new ConfigurationTreeGenerator(
            List.of(DistributionZonesConfiguration.KEY),
            List.of(),
            List.of(TestPersistStorageConfigurationSchema.class)
    );

    private final ConfigurationRegistry registry = new ConfigurationRegistry(
            List.of(DistributionZonesConfiguration.KEY),
            new TestConfigurationStorage(DISTRIBUTED),
            generator,
            ConfigurationValidatorImpl.withDefaultValidators(generator, Set.of())
    );

    private DistributionZoneManager distributionZoneManager;

    /** Inner initialisation. */
    @BeforeEach
    void before() {
        registry.start();

        assertThat(registry.onDefaultsPersisted(), willCompleteSuccessfully());

        DistributionZonesConfiguration zonesConfiguration = registry.getConfiguration(DistributionZonesConfiguration.KEY);

        NamedConfigurationTree<TableConfiguration, TableView, TableChange> tables = mock(NamedConfigurationTree.class);

        NamedListView<TableView> value = mock(NamedListView.class);

        when(tables.value()).thenReturn(value);

        when(value.namedListKeys()).thenReturn(new ArrayList<>());

        CatalogManager catalogManager = mock(CatalogManager.class);

        distributionZoneManager = new DistributionZoneManager(
                "node",
                null,
                zonesConfiguration,
                null,
                null,
                null,
                null,
                catalogManager
        );

        commandHandler = new DdlCommandHandler(mock(TableManager.class), catalogManager);
    }

    @AfterEach
    public void after() throws Exception {
        registry.stop();
    }

    @AfterAll
    static void afterAll() {
        generator.close();
    }

    @Test
    public void testZoneAlreadyExistsOnCreate1() {
        assertThat(handleCreateZoneCommand(false), willThrow(DistributionZoneAlreadyExistsException.class));
    }

    @Test
    public void testZoneAlreadyExistsOnCreate2() {
        assertThat(handleCreateZoneCommand(true), willCompleteSuccessfully());
    }

    @Test
    public void testZoneNotFoundOnDrop1() {
        DropZoneCommand cmd = new DropZoneCommand();
        cmd.zoneName(ZONE_NAME);

        assertThat(commandHandler.handle(cmd), willThrow(DistributionZoneNotFoundException.class));
    }

    @Test
    public void testZoneNotFoundOnDrop2() {
        DropZoneCommand cmd = new DropZoneCommand();
        cmd.zoneName(ZONE_NAME);
        cmd.ifExists(true);

        assertThat(commandHandler.handle(cmd), willCompleteSuccessfully());
    }

    private CompletableFuture<Boolean> handleCreateZoneCommand(boolean ifNotExists) {
        createZone(distributionZoneManager, ZONE_NAME, null, null, null);

        CreateZoneCommand cmd = new CreateZoneCommand();
        cmd.zoneName(ZONE_NAME);
        cmd.ifNotExists(ifNotExists);

        return commandHandler.handle(cmd);
    }
}
