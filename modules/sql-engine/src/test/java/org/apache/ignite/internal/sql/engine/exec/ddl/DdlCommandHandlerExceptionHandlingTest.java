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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;
import org.apache.ignite.internal.distributionzones.DistributionZoneConfigurationParameters;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateZoneCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropZoneCommand;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.impl.TestPersistStorageConfigurationSchema;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.lang.DistributionZoneAlreadyExistsException;
import org.apache.ignite.lang.DistributionZoneNotFoundException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Tests distribution zone command exception handling.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class DdlCommandHandlerExceptionHandlingTest extends IgniteAbstractTest {
    @Mock
    private DataStorageManager dataStorageManager;

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

        CatalogManager catalogManager = mock(CatalogManager.class);
        when(catalogManager.createDistributionZone(any())).thenReturn(completedFuture(null));
        when(catalogManager.alterDistributionZone(any())).thenReturn(completedFuture(null));
        when(catalogManager.renameDistributionZone(any())).thenReturn(completedFuture(null));
        when(catalogManager.dropDistributionZone(any())).thenReturn(completedFuture(null));
        CatalogZoneDescriptor desc = mock(CatalogZoneDescriptor.class);
        when(catalogManager.zone(anyString(), anyLong())).thenReturn(desc);
        when(desc.id()).thenReturn(42);

        distributionZoneManager = new DistributionZoneManager(
                zonesConfiguration,
                null,
                catalogManager,
                null,
                null,
                null,
                "node"
        );

        commandHandler = new DdlCommandHandler(distributionZoneManager, dataStorageManager, catalogManager);
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
    public void testZoneAlreadyExistsOnCreate1() throws ExecutionException, InterruptedException, TimeoutException {
        CompletableFuture<Boolean> fut = handleCreateZoneCommand(false);

        Exception e = null;

        try {
            fut.get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertTrue(e != null, "Expected exception was not thrown.");
        assertTrue(
                e.getCause() instanceof DistributionZoneAlreadyExistsException,
                "Unexpected type of exception (requires DistributionZoneAlreadyExistsException): " + e
        );
    }

    @Test
    public void testZoneAlreadyExistsOnCreate2() throws ExecutionException, InterruptedException, TimeoutException {
        CompletableFuture<Boolean> fut = handleCreateZoneCommand(true);

        try {
            fut.get(5, TimeUnit.SECONDS);
        } catch (Throwable e) {
            fail("Expected no exception but was: " + e);
        }
    }

    @Test
    public void testZoneNotFoundOnDrop1() {
        DropZoneCommand cmd = new DropZoneCommand();
        cmd.zoneName(ZONE_NAME);

        CompletableFuture<Boolean> fut = commandHandler.handle(cmd);

        Exception e = null;

        try {
            fut.get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertTrue(e != null, "Expected exception was not thrown.");
        assertTrue(
                e.getCause() instanceof DistributionZoneNotFoundException,
                "Unexpected type of exception (requires DistributionZoneNotFoundException): " + e
        );
    }

    @Test
    public void testZoneNotFoundOnDrop2() {
        DropZoneCommand cmd = new DropZoneCommand();
        cmd.zoneName(ZONE_NAME);
        cmd.ifExists(true);

        CompletableFuture<Boolean> fut = commandHandler.handle(cmd);

        try {
            fut.get(5, TimeUnit.SECONDS);
        } catch (Throwable e) {
            fail("Expected no exception but was: " + e);
        }
    }

    private CompletableFuture<Boolean> handleCreateZoneCommand(boolean ifNotExists)
            throws ExecutionException, InterruptedException, TimeoutException {
        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME).build()
                )
                .get(5, TimeUnit.SECONDS);

        CreateZoneCommand cmd = new CreateZoneCommand();
        cmd.zoneName(ZONE_NAME);
        cmd.ifNotExists(ifNotExists);

        return commandHandler.handle(cmd);
    }
}
