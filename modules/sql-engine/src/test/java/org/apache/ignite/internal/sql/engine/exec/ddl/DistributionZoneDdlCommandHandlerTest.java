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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.distributionzones.DistributionZoneConfigurationParameters;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterZoneRenameCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterZoneSetCommand;
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
    private final AtomicReference<Object[]> invocationResultHolder = new AtomicReference<>();

    @Mock
    private TableManager tableManager;

    @Mock
    private DataStorageManager dataStorageManager;

    /** DDL commands handler. */
    private DdlCommandHandler commandHandler;

    /** Inner initialisation. */
    @BeforeEach
    void before() {
        DistributionZoneManager distributionZoneManager = mock(DistributionZoneManager.class,
                (Answer<CompletableFuture<Void>>) invocationOnMock -> {
                    Object[] arguments = invocationOnMock.getArguments();
                    invocationResultHolder.set(Arrays.copyOf(arguments, arguments.length, Object[].class));

                    return CompletableFuture.completedFuture(null);
                });

        commandHandler = new DdlCommandHandler(distributionZoneManager, tableManager, dataStorageManager, mock(CatalogManager.class));
    }


    @Test
    public void testCreateZone() {
        CreateZoneCommand cmd = new CreateZoneCommand();
        cmd.zoneName("test_zone");

        invokeHandler(cmd);
        DistributionZoneConfigurationParameters params = getArgument(0);

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

        invokeHandler(cmdValidArguments1);
        DistributionZoneConfigurationParameters params = getArgument(0);

        assertNotNull(params);
        assertThat(params.dataNodesAutoAdjust(), equalTo(autoAdjust));

        // Another valid options combination.
        CreateZoneCommand cmdValidArguments2 = new CreateZoneCommand();
        cmdValidArguments2.zoneName(name);
        cmdValidArguments2.dataNodesAutoAdjustScaleUp(autoAdjustScaleUp);
        cmdValidArguments2.dataNodesAutoAdjustScaleDown(autoAdjustScaleDown);

        invokeHandler(cmdValidArguments2);
        params = getArgument(0);

        assertThat(params.dataNodesAutoAdjustScaleUp(), equalTo(autoAdjustScaleUp));
        assertThat(params.dataNodesAutoAdjustScaleDown(), equalTo(autoAdjustScaleDown));
    }

    @Test
    public void testRenameZone() {
        String name = "test_zone";
        String newName = "new_test_zone";

        AlterZoneRenameCommand renameCmd = new AlterZoneRenameCommand();
        renameCmd.zoneName(name);
        renameCmd.newZoneName(newName);
        invokeHandler(renameCmd);

        String zoneName = getArgument(0);
        DistributionZoneConfigurationParameters params = getArgument(1);

        assertEquals(name, zoneName);
        assertEquals(newName, params.name());
    }

    @Test
    public void testAlterZone() {
        String name = "test_zone";
        int autoAdjust = 1;
        int autoAdjustScaleUp = 2;
        int autoAdjustScaleDown = 3;
        int partitions = 4;
        String nodeFilter = "a = 1";
        int replicas = 5;

        // Valid options combination.
        AlterZoneSetCommand cmdValidArguments1 = new AlterZoneSetCommand();
        cmdValidArguments1.zoneName(name);
        cmdValidArguments1.dataNodesAutoAdjust(autoAdjust);
        cmdValidArguments1.partitions(partitions);
        cmdValidArguments1.nodeFilter(nodeFilter);
        cmdValidArguments1.replicas(replicas);

        invokeHandler(cmdValidArguments1);

        assertEquals(name, getArgument(0));
        DistributionZoneConfigurationParameters params = getArgument(1);

        assertNotNull(params);
        assertThat(params.dataNodesAutoAdjust(), equalTo(autoAdjust));
        assertThat(params.partitions(), equalTo(partitions));
        assertThat(params.filter(), equalTo(nodeFilter));
        assertThat(params.replicas(), equalTo(replicas));

        // Invalid options combination.
        AlterZoneSetCommand cmdInvalidOptions = new AlterZoneSetCommand();
        cmdInvalidOptions.zoneName(name);
        cmdInvalidOptions.dataNodesAutoAdjust(autoAdjust);
        cmdInvalidOptions.dataNodesAutoAdjustScaleUp(autoAdjustScaleUp);
        cmdInvalidOptions.dataNodesAutoAdjustScaleDown(autoAdjustScaleDown);

        assertThrows(IllegalArgumentException.class, () -> invokeHandler(cmdInvalidOptions));
    }

    @Test
    public void testDropZone() {
        DropZoneCommand cmd = new DropZoneCommand();
        cmd.zoneName("test_zone");

        invokeHandler(cmd);

        String name = getArgument(0);
        assertThat(name, equalTo(cmd.zoneName()));
    }

    private void invokeHandler(DdlCommand cmd) {
        commandHandler.handle(cmd);
    }

    private <T> T getArgument(int index) {
        Object[] arguments = invocationResultHolder.get();
        if (index >= arguments.length) {
            String errorMessage = String.format(
                    "DistributionZoneManager has been called with the following arguments: %s"
                            + "The index %d is out of bounds for the arguments array of size %d.",
                    Arrays.toString(arguments), index, arguments.length);

            throw new IllegalArgumentException(errorMessage);
        }

        return (T) arguments[index];
    }
}
