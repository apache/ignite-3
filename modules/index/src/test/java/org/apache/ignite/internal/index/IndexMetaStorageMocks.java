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

package org.apache.ignite.internal.index;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.table.distributed.index.IndexMeta;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.index.MetaIndexStatusChange;

class IndexMetaStorageMocks {
    static void configureMocksForBuildingPhase(IndexMetaStorage indexMetaStorage) {
        doAnswer(invocation -> {
            int indexId = invocation.getArgument(0);

            IndexMeta indexMeta = mock(IndexMeta.class);
            when(indexMeta.indexId()).thenReturn(indexId);
            when(indexMeta.statusChange(any())).thenReturn(new MetaIndexStatusChange(1, HybridTimestamp.MIN_VALUE.longValue()));

            return indexMeta;
        }).when(indexMetaStorage).indexMeta(anyInt());
    }
}
