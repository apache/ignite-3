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

package org.apache.ignite.internal.storage.pagememory.mv;

import com.google.auto.service.AutoService;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.io.PageIoModule;
import org.apache.ignite.internal.storage.pagememory.mv.gc.io.GcInnerIo;
import org.apache.ignite.internal.storage.pagememory.mv.gc.io.GcLeafIo;
import org.apache.ignite.internal.storage.pagememory.mv.gc.io.GcMetaIo;
import org.apache.ignite.internal.storage.pagememory.mv.io.BlobFragmentIo;
import org.apache.ignite.internal.storage.pagememory.mv.io.VersionChainInnerIo;
import org.apache.ignite.internal.storage.pagememory.mv.io.VersionChainLeafIo;
import org.apache.ignite.internal.storage.pagememory.mv.io.VersionChainMetaIo;

/**
 * {@link PageIoModule} related to {@link VolatilePageMemoryMvPartitionStorage} and {@link PersistentPageMemoryMvPartitionStorage}
 * implementations.
 */
@AutoService(PageIoModule.class)
public class MvPageIoModule implements PageIoModule {
    @Override
    public Collection<IoVersions<?>> ioVersions() {
        return List.of(
                VersionChainMetaIo.VERSIONS,
                VersionChainInnerIo.VERSIONS,
                VersionChainLeafIo.VERSIONS,
                BlobFragmentIo.VERSIONS,
                GcMetaIo.VERSIONS,
                GcInnerIo.VERSIONS,
                GcLeafIo.VERSIONS
        );
    }
}
