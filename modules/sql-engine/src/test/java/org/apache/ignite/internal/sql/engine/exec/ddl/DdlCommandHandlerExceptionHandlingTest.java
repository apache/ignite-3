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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.createTestCatalogManager;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.createZone;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.parseStorageProfiles;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.DistributionZoneExistsValidationException;
import org.apache.ignite.internal.catalog.DistributionZoneNotFoundValidationException;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommand;
import org.apache.ignite.internal.catalog.commands.DropZoneCommand;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests distribution zone command exception handling. */
public class DdlCommandHandlerExceptionHandlingTest extends IgniteAbstractTest {
    private DdlCommandHandler commandHandler;

    private static final String ZONE_NAME = "zone1";

    private CatalogManager catalogManager;

    private ClockWaiter clockWaiter;

    @BeforeEach
    void before() {
        HybridClock clock = new HybridClockImpl();
        catalogManager = createTestCatalogManager("test", clock);
        assertThat(catalogManager.startAsync(ForkJoinPool.commonPool()), willCompleteSuccessfully());

        clockWaiter = new ClockWaiter("test", clock);
        assertThat(clockWaiter.startAsync(ForkJoinPool.commonPool()), willCompleteSuccessfully());

        commandHandler = new DdlCommandHandler(catalogManager, new TestClockService(clock, clockWaiter), () -> 100);
    }

    @AfterEach
    public void after() {
        assertThat(stopAsync(clockWaiter, catalogManager), willCompleteSuccessfully());
    }

    @Test
    public void testZoneAlreadyExistsOnCreate1() {
        assertThat(handleCreateZoneCommand(false), willThrow(DistributionZoneExistsValidationException.class));
    }

    @Test
    public void testZoneAlreadyExistsOnCreate2() {
        assertThat(handleCreateZoneCommand(true), willCompleteSuccessfully());
    }

    @Test
    public void testZoneNotFoundOnDrop1() {
        CatalogCommand cmd = DropZoneCommand.builder()
                .zoneName(ZONE_NAME)
                .build();

        assertThat(commandHandler.handle(cmd), willThrow(DistributionZoneNotFoundValidationException.class));
    }

    @Test
    public void testZoneNotFoundOnDrop2() {
        CatalogCommand cmd = DropZoneCommand.builder()
                .zoneName(ZONE_NAME)
                .ifExists(true)
                .build();

        assertThat(commandHandler.handle(cmd), willCompleteSuccessfully());
    }

    private CompletableFuture<Boolean> handleCreateZoneCommand(boolean ifNotExists) {
        createZone(catalogManager, ZONE_NAME, null, null, null);

        CatalogCommand cmd = CreateZoneCommand.builder()
                .zoneName(ZONE_NAME)
                .storageProfilesParams(parseStorageProfiles(DEFAULT_STORAGE_PROFILE))
                .ifNotExists(ifNotExists)
                .build();

        return commandHandler.handle(cmd);
    }
}
