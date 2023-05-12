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

package org.apache.ignite.internal.configuration;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.UpdateDistributedConfigurationAction;
import org.apache.ignite.internal.configuration.presentation.ConfigurationPresentation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DistributedConfigurationUpdaterTest {

    @Mock
    public ConfigurationPresentation<String> presentation;

    @Mock
    public ClusterManagementGroupManager cmgMgr;

    @Test
    public void nextActionIsCompletedAfterUpdatingConfiguration() {

        // Set up mocks.
        when(presentation.update(anyString())).thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<Void> nextAction = new CompletableFuture<>();
        String configuration = "security.authentication.enabled:true";
        UpdateDistributedConfigurationAction updateDistributedConfigurationAction =
                new UpdateDistributedConfigurationAction(
                        configuration,
                        (result) -> result.whenComplete((v, e) -> nextAction.complete(null)));

        when(cmgMgr.clusterConfigurationToUpdate())
                .thenReturn(CompletableFuture.completedFuture(updateDistributedConfigurationAction));

        // Run updater.
        DistributedConfigurationUpdater distributedConfigurationUpdater = new DistributedConfigurationUpdater(
                cmgMgr,
                presentation
        );

        distributedConfigurationUpdater.start();

        // Verify that configuration was updated.
        verify(presentation, times(1)).update(configuration);

        // Verify that next action is completed.
        nextAction.join();
        assertThat(nextAction.isDone(), is(true));
    }

    @Test
    public void nextActionIsCompletedIfConfigurationNull() {

        // Set up mocks.
        CompletableFuture<Void> nextAction = new CompletableFuture<>();
        UpdateDistributedConfigurationAction updateDistributedConfigurationAction =
                new UpdateDistributedConfigurationAction(
                        null,
                        (result) -> result.whenComplete((v, e) -> nextAction.complete(null)));

        when(cmgMgr.clusterConfigurationToUpdate())
                .thenReturn(CompletableFuture.completedFuture(updateDistributedConfigurationAction));

        // Run updater.
        DistributedConfigurationUpdater distributedConfigurationUpdater = new DistributedConfigurationUpdater(
                cmgMgr,
                presentation
        );

        distributedConfigurationUpdater.start();

        // Verify that configuration wasn't updated.
        verify(presentation, never()).update(any());

        // Verify that next action is completed.
        nextAction.join();
        assertThat(nextAction.isDone(), is(true));
    }
}
