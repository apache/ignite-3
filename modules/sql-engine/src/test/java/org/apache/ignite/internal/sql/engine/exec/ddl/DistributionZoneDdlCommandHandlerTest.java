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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommand;
import org.apache.ignite.internal.catalog.commands.RenameZoneCommand;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterZoneRenameCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterZoneSetCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateZoneCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DdlCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropZoneCommand;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests distribution zone commands handling.
 */
public class DistributionZoneDdlCommandHandlerTest extends IgniteAbstractTest {
    private static final String ZONE_NAME = "test_zone";

    private DdlCommandHandler commandHandler;

    private CatalogManager catalogManager;

    @BeforeEach
    void before() {
        catalogManager = mock(CatalogManager.class, invocation -> nullCompletedFuture());

        commandHandler = new DdlCommandHandler(catalogManager, mock(ClockService.class, invocation -> nullCompletedFuture()), () -> 100);
    }

    @Test
    public void testCreateZone() {
        CreateZoneCommand cmd = new CreateZoneCommand();
        cmd.zoneName(ZONE_NAME);
        cmd.storageProfiles(DEFAULT_STORAGE_PROFILE);

        invokeHandler(cmd);

        verify(catalogManager).execute(any(org.apache.ignite.internal.catalog.commands.CreateZoneCommand.class));
    }

    @Test
    public void testRenameZone() {
        AlterZoneRenameCommand renameCmd = new AlterZoneRenameCommand();
        renameCmd.zoneName(ZONE_NAME);
        renameCmd.newZoneName(ZONE_NAME + "_new");

        invokeHandler(renameCmd);

        verify(catalogManager).execute(any(RenameZoneCommand.class));
    }

    @Test
    public void testAlterZone() {
        AlterZoneSetCommand cmd = new AlterZoneSetCommand();
        cmd.zoneName(ZONE_NAME);

        invokeHandler(cmd);

        verify(catalogManager).execute(any(AlterZoneCommand.class));
    }

    @Test
    public void testDropZone() {
        DropZoneCommand cmd = new DropZoneCommand();
        cmd.zoneName(ZONE_NAME);

        invokeHandler(cmd);

        verify(catalogManager).execute(any(org.apache.ignite.internal.catalog.commands.DropZoneCommand.class));
    }

    private void invokeHandler(DdlCommand cmd) {
        assertThat(commandHandler.handle(cmd), willCompleteSuccessfully());
    }
}
