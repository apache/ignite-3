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

package org.apache.ignite.internal.storage.pagememory.index;

import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.io.PageIoModule;
import org.apache.ignite.internal.storage.pagememory.index.hash.io.HashIndexTreeInnerIo;
import org.apache.ignite.internal.storage.pagememory.index.hash.io.HashIndexTreeLeafIo;
import org.apache.ignite.internal.storage.pagememory.index.hash.io.HashIndexTreeMetaIo;
import org.apache.ignite.internal.storage.pagememory.index.meta.io.IndexMetaInnerIo;
import org.apache.ignite.internal.storage.pagememory.index.meta.io.IndexMetaLeafIo;
import org.apache.ignite.internal.storage.pagememory.index.meta.io.IndexMetaTreeMetaIo;
import org.apache.ignite.internal.storage.pagememory.index.sorted.io.SortedIndexTreeInnerIo;
import org.apache.ignite.internal.storage.pagememory.index.sorted.io.SortedIndexTreeLeafIo;
import org.apache.ignite.internal.storage.pagememory.index.sorted.io.SortedIndexTreeMetaIo;

/**
 * {@link PageIoModule} related to {@link PageMemory} based indexes.
 */
@AutoService(PageIoModule.class)
public class IndexPageIoModule implements PageIoModule {
    /** {@inheritDoc} */
    @Override
    public Collection<IoVersions<?>> ioVersions() {
        List<IoVersions<?>> ioVersions = new ArrayList<>();

        // Meta tree IO.
        ioVersions.add(IndexMetaTreeMetaIo.VERSIONS);
        ioVersions.add(IndexMetaInnerIo.VERSIONS);
        ioVersions.add(IndexMetaLeafIo.VERSIONS);

        // Hash index IO.
        ioVersions.add(HashIndexTreeMetaIo.VERSIONS);
        ioVersions.addAll(HashIndexTreeInnerIo.VERSIONS);
        ioVersions.addAll(HashIndexTreeLeafIo.VERSIONS);

        // Sorted index IO.
        ioVersions.add(SortedIndexTreeMetaIo.VERSIONS);
        ioVersions.addAll(SortedIndexTreeInnerIo.VERSIONS);
        ioVersions.addAll(SortedIndexTreeLeafIo.VERSIONS);

        return ioVersions;
    }
}
