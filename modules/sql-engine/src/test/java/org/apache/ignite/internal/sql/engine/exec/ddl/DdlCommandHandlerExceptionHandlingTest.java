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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommand;
import org.apache.ignite.internal.catalog.commands.DropZoneCommand;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.hamcrest.core.Is;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Tests distribution zone command exception handling. */
@ExtendWith(ExecutorServiceExtension.class)
public class DdlCommandHandlerExceptionHandlingTest extends IgniteAbstractTest {
    private DdlCommandHandler commandHandler;

    private static final String ZONE_NAME = "zone1";

    private CatalogManager catalogManager;

    private ClockWaiter clockWaiter;

    @InjectExecutorService
    private ScheduledExecutorService scheduledExecutor;

    @BeforeEach
    void before() {
        HybridClock clock = new HybridClockImpl();
        catalogManager = createTestCatalogManager("test", clock);
        assertThat(catalogManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        clockWaiter = new ClockWaiter("test", clock, scheduledExecutor);
        assertThat(clockWaiter.startAsync(new ComponentContext()), willCompleteSuccessfully());

        commandHandler = new DdlCommandHandler(catalogManager, new TestClockService(clock, clockWaiter));
    }

    @AfterEach
    public void after() throws Exception {
        commandHandler.stop();

        List.of(clockWaiter, catalogManager).forEach(IgniteComponent::beforeNodeStop);
        assertThat(stopAsync(new ComponentContext(), clockWaiter, catalogManager), willCompleteSuccessfully());
    }

    @Test
    public void testZoneAlreadyExistsOnCreate1() {
        assertThat(handleCreateZoneCommand(false),
                willThrow(CatalogValidationException.class, "Distribution zone with name 'zone1' already exists."));
    }

    @Test
    public void testZoneAlreadyExistsOnCreate2() {
        assertThat(handleCreateZoneCommand(true), willBe(Is.is(false)));
    }

    @Test
    public void testZoneNotFoundOnDrop1() {
        CatalogCommand cmd = DropZoneCommand.builder()
                .zoneName(ZONE_NAME)
                .build();

        assertThat(commandHandler.handle(cmd),
                willThrow(CatalogValidationException.class, "Distribution zone with name 'zone1' not found."));
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

        return commandHandler.handle(cmd)
                .thenApply(result -> result.isApplied(0));
    }
}
