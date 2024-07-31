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

package org.apache.ignite.internal.pagememory;

import com.google.auto.service.AutoService;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.pagememory.freelist.io.PagesListMetaIo;
import org.apache.ignite.internal.pagememory.freelist.io.PagesListNodeIo;
import org.apache.ignite.internal.pagememory.io.DataPageIo;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.io.PageIoModule;

/**
 * {@link PageIoModule} implementation in page-memory module.
 */
@AutoService(PageIoModule.class)
public class PageMemoryIoModule implements PageIoModule {
    @Override
    public Collection<IoVersions<?>> ioVersions() {
        return List.of(
                PagesListMetaIo.VERSIONS,
                PagesListNodeIo.VERSIONS,
                DataPageIo.VERSIONS
        );
    }
}
