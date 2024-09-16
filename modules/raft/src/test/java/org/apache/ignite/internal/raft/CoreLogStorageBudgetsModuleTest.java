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

package org.apache.ignite.internal.raft;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

import org.apache.ignite.internal.raft.configuration.EntryCountBudgetConfigurationSchema;
import org.apache.ignite.internal.raft.configuration.EntryCountBudgetView;
import org.apache.ignite.internal.raft.configuration.LogStorageBudgetView;
import org.apache.ignite.internal.raft.configuration.UnlimitedBudgetConfigurationSchema;
import org.apache.ignite.internal.raft.storage.impl.EntryCountBudget;
import org.apache.ignite.internal.raft.storage.impl.UnlimitedBudget;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.raft.jraft.core.LogStorageBudgetFactory;
import org.junit.jupiter.api.Test;

class CoreLogStorageBudgetsModuleTest extends BaseIgniteAbstractTest {
    private final CoreLogStorageBudgetsModule module = new CoreLogStorageBudgetsModule();

    @Test
    void providesUnlimitedBudget() {
        LogStorageBudgetFactory factory = module.budgetFactories().get(UnlimitedBudgetConfigurationSchema.NAME);

        assertThat(factory, is(notNullValue()));

        assertThat(factory.create(mock(LogStorageBudgetView.class)), is(instanceOf(UnlimitedBudget.class)));
    }

    @Test
    void providesEntryCountBudget() {
        LogStorageBudgetFactory factory = module.budgetFactories().get(EntryCountBudgetConfigurationSchema.NAME);

        assertThat(factory, is(notNullValue()));

        assertThat(factory.create(new EntryCountBudgetView() {
            @Override
            public long entriesCountLimit() {
                return 0;
            }

            @Override
            public String name() {
                return EntryCountBudgetConfigurationSchema.NAME;
            }
        }), is(instanceOf(EntryCountBudget.class)));
    }
}
