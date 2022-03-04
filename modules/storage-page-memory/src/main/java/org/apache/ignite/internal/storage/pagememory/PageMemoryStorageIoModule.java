/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.storage.pagememory;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.io.PageIoModule;
import org.apache.ignite.internal.storage.pagememory.io.TableDataIo;
import org.apache.ignite.internal.storage.pagememory.io.TableInnerIo;
import org.apache.ignite.internal.storage.pagememory.io.TableLeafIo;
import org.apache.ignite.internal.storage.pagememory.io.TableMetaIo;

/**
 * {@link PageIoModule} implementation in storage-page-memory module.
 */
public class PageMemoryStorageIoModule implements PageIoModule {
    /** {@inheritDoc} */
    @Override
    public Collection<IoVersions<?>> ioVersions() {
        return List.of(
                TableMetaIo.VERSIONS,
                TableInnerIo.VERSIONS,
                TableLeafIo.VERSIONS,
                TableDataIo.VERSIONS
        );
    }
}
