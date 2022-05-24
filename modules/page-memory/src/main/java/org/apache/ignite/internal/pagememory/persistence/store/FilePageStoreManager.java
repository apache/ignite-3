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

package org.apache.ignite.internal.pagememory.persistence.store;

import java.nio.file.Path;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteLogger;

/**
 * Waiting IGNITE-17011.
 */
public class FilePageStoreManager implements IgniteComponent {
    public FilePageStoreManager(
            IgniteLogger log,
            String igniteInstanceName,
            Path storagePath,
            FileIoFactory filePageStoreFileIoFactory,
            // TODO: IGNITE-17017 Move to common config
            int pageSize
    ) throws IgniteInternalCheckedException {
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() throws Exception {

    }
}
