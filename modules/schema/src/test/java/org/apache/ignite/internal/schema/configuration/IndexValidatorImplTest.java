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

package org.apache.ignite.internal.schema.configuration;

import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.validate;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.table.HashIndexChange;
import org.apache.ignite.configuration.schemas.table.IndexValidator;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesView;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.internal.configuration.NamedListConfiguration;
import org.junit.jupiter.api.Test;

/** Index validator tests. */
public class IndexValidatorImplTest extends AbstractTableIndexValidatorTest {
    /** Tests that validator finds no issues in a simple valid configuration. */
    @Test
    public void testNoIssues() throws Exception {
        ValidationContext<NamedListView<TableIndexView>> ctxIdx = mock(ValidationContext.class);

        NamedConfigurationTree<TableConfiguration, TableView, TableChange> cfg0 = tablesCfg.tables();

        List<UUID> ids = ((NamedListConfiguration<TableConfiguration, ?, ?>) cfg0).internalIds();

        assertEquals(1, ids.size());

        UUID tableId = ids.get(0);

        tablesCfg.indexes().change(c -> c.create("schema.idx", idxCng -> idxCng.convert(HashIndexChange.class)
                .changeColumnNames("ID2").changeTableId(tableId))).get();

        TablesView view = tablesCfg.value();

        when(ctxIdx.getNewRoot(any())).thenReturn(view);

        validate(IndexValidatorImpl.INSTANCE, mock(IndexValidator.class), ctxIdx, null);
    }
}
