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
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.createZone;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.ClockWaiter;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.distributionzones.DistributionZoneAlreadyExistsException;
import org.apache.ignite.internal.distributionzones.DistributionZoneNotFoundException;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateZoneCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropZoneCommand;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests distribution zone command exception handling.
 */
public class DdlCommandHandlerExceptionHandlingTest extends IgniteAbstractTest {
    private DdlCommandHandler commandHandler;

    private static final String ZONE_NAME = "zone1";

    private VaultManager vault;

    private MetaStorageManager metastore;

    private ClockWaiter clockWaiter;

    private CatalogManager catalogManager;

    @BeforeEach
    void before() {
        String nodeName = "test";

        vault = new VaultManager(new InMemoryVaultService());

        metastore = StandaloneMetaStorageManager.create(vault, new SimpleInMemoryKeyValueStorage(nodeName));

        clockWaiter = new ClockWaiter(nodeName, new HybridClockImpl());

        catalogManager = new CatalogManagerImpl(new UpdateLogImpl(metastore), clockWaiter);

        commandHandler = new DdlCommandHandler(mock(TableManager.class), catalogManager);

        Stream.of(vault, metastore, clockWaiter, catalogManager).forEach(IgniteComponent::start);

        assertThat(metastore.deployWatches(), willCompleteSuccessfully());
    }

    @AfterEach
    public void after() throws Exception {
        IgniteUtils.stopAll(catalogManager, clockWaiter, metastore, vault);
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
        createZone(catalogManager, ZONE_NAME, null, null, null);

        CreateZoneCommand cmd = new CreateZoneCommand();
        cmd.zoneName(ZONE_NAME);
        cmd.ifNotExists(ifNotExists);

        return commandHandler.handle(cmd);
    }
}
