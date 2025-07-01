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

package org.apache.ignite.internal.raft.configuration;

import static org.apache.ignite.internal.raft.configuration.DisruptorConfigurationSchema.DEFAULT_LOG_MANAGER_STRIPES_COUNT;
import static org.apache.ignite.internal.raft.configuration.DisruptorConfigurationSchema.DEFAULT_STRIPES_COUNT;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.SuperRootChange;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/** For {@link RaftConfigurationModule} testing. */
public class RaftConfigurationModuleTest extends BaseIgniteAbstractTest {
    private final RaftConfigurationModule configModule = new RaftConfigurationModule();

    @Test
    void testMigrateDeprecatedConfigurationsForDisruptorConfigurationNotChange() {
        SuperRootChange superRootChange = mock(SuperRootChange.class);

        RaftExtensionView view = createRaftExtensionView(DEFAULT_STRIPES_COUNT, DEFAULT_LOG_MANAGER_STRIPES_COUNT);
        RaftExtensionChange change = createRaftExtensionChange();

        add(superRootChange, RaftExtensionConfiguration.KEY, view, change);

        configModule.migrateDeprecatedConfigurations(superRootChange);

        DisruptorChange disruptorChange = change.changeRaft().changeDisruptor();
        verify(disruptorChange, never()).changeStripes(anyInt());
        verify(disruptorChange, never()).changeLogManagerStripes(anyInt());
    }

    @Test
    void testMigrateDeprecatedConfigurationsForDisruptorConfiguration() {
        SuperRootChange superRootChange = mock(SuperRootChange.class);

        RaftExtensionView view = createRaftExtensionView(DEFAULT_STRIPES_COUNT + 1, DEFAULT_LOG_MANAGER_STRIPES_COUNT + 1);
        RaftExtensionChange change = createRaftExtensionChange();

        add(superRootChange, RaftExtensionConfiguration.KEY, view, change);

        configModule.migrateDeprecatedConfigurations(superRootChange);

        DisruptorChange disruptorChange = change.changeRaft().changeDisruptor();
        verify(disruptorChange).changeStripes(DEFAULT_STRIPES_COUNT + 1);
        verify(disruptorChange).changeLogManagerStripes(DEFAULT_LOG_MANAGER_STRIPES_COUNT + 1);
    }

    private static <V, C extends V> void add(SuperRootChange superRootChange, RootKey<?, V, C> rootKey, V view, C change) {
        when(superRootChange.viewRoot(rootKey)).thenReturn(view);
        when(superRootChange.changeRoot(rootKey)).thenReturn(change);
    }

    private static RaftExtensionView createRaftExtensionView(int stripes, int logStripesCount) {
        RaftExtensionView raftExtensionView = mock(RaftExtensionView.class);
        RaftView raftView = mock(RaftView.class);

        when(raftView.stripes()).thenReturn(stripes);
        when(raftView.logStripesCount()).thenReturn(logStripesCount);

        when(raftExtensionView.raft()).thenReturn(raftView);

        return raftExtensionView;
    }

    private static RaftExtensionChange createRaftExtensionChange() {
        RaftExtensionChange raftExtensionChange = mock(RaftExtensionChange.class);
        RaftChange raftChange = mock(RaftChange.class);
        DisruptorChange disruptorChange = mock(DisruptorChange.class);

        when(raftChange.changeDisruptor()).thenReturn(disruptorChange);
        when(raftExtensionChange.changeRaft()).thenReturn(raftChange);

        return raftExtensionChange;
    }
}
