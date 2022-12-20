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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.distributionzones.DistributionZoneConfigurationParameters;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.index.IndexManager;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateZoneCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DdlCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropZoneCommand;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;

/**
 * Tests distribution zone commands handling.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class DistributionZoneDdlCommandHandlerTest extends IgniteAbstractTest {
    /** Holder of the result of the invoked method. */
    private final AtomicReference<Object> invocationResultHolder = new AtomicReference<>();

    @Mock
    private TableManager tableManager;

    @Mock
    private IndexManager indexManager;

    @Mock
    private DataStorageManager dataStorageManager;

    /** DDL commands handler. */
    private DdlCommandHandler commandHandler;

    /** Inner initialisation. */
    @BeforeEach
    void before() {
        DistributionZoneManager distributionZoneManager = Mockito.mock(DistributionZoneManager.class,
                (Answer<CompletableFuture<Void>>) invocationOnMock -> {
                    invocationResultHolder.set(invocationOnMock.getArgument(0));

                    return CompletableFuture.completedFuture(null);
                });

        commandHandler = new DdlCommandHandler(distributionZoneManager, tableManager, indexManager, dataStorageManager);
    }


    @Test
    public void testCreateZone() {
        CreateZoneCommand cmd = new CreateZoneCommand();
        cmd.zoneName("test_zone");

        DistributionZoneConfigurationParameters params = invokeHandler(cmd);

        assertNotNull(params);
        assertThat(params.name(), equalTo(cmd.zoneName()));
    }

    @Test
    public void testCreateZoneOptions() {
        String name = "test_zone";
        int autoAdjust = 1;
        int autoAdjustScaleUp = 2;
        int autoAdjustScaleDown = 3;

        // Invalid options combination.
        CreateZoneCommand cmdInvalidOptions = new CreateZoneCommand();
        cmdInvalidOptions.zoneName(name);
        cmdInvalidOptions.dataNodesAutoAdjust(autoAdjust);
        cmdInvalidOptions.dataNodesAutoAdjustScaleUp(autoAdjustScaleUp);
        cmdInvalidOptions.dataNodesAutoAdjustScaleDown(autoAdjustScaleDown);

        assertThrows(IllegalArgumentException.class, () -> invokeHandler(cmdInvalidOptions));

        // Valid options combination.
        CreateZoneCommand cmdValidArguments1 = new CreateZoneCommand();
        cmdValidArguments1.zoneName(name);
        cmdValidArguments1.dataNodesAutoAdjust(autoAdjust);

        DistributionZoneConfigurationParameters params = invokeHandler(cmdValidArguments1);

        assertNotNull(params);
        assertThat(params.dataNodesAutoAdjust(), equalTo(autoAdjust));

        // Another valid options combination.
        CreateZoneCommand cmdValidArguments2 = new CreateZoneCommand();
        cmdValidArguments2.zoneName(name);
        cmdValidArguments2.dataNodesAutoAdjustScaleUp(autoAdjustScaleUp);
        cmdValidArguments2.dataNodesAutoAdjustScaleDown(autoAdjustScaleDown);

        params = invokeHandler(cmdValidArguments2);

        assertThat(params.dataNodesAutoAdjustScaleUp(), equalTo(autoAdjustScaleUp));
        assertThat(params.dataNodesAutoAdjustScaleDown(), equalTo(autoAdjustScaleDown));
    }

    @Test
    public void testDropZone() {
        DropZoneCommand cmd = new DropZoneCommand();
        cmd.zoneName("test_zone");

        String name = invokeHandler(cmd);
        assertThat(name, equalTo(cmd.zoneName()));
    }

    private <T> T invokeHandler(DdlCommand cmd) {
        commandHandler.handle(cmd);

        return (T) invocationResultHolder.get();
    }
}
