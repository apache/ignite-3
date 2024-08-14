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

package org.apache.ignite.internal.raft.util;

import java.nio.file.Path;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.storage.impl.DefaultLogStorageFactory;
import org.apache.ignite.internal.raft.storage.logit.LogitLogStorageFactory;
import org.apache.ignite.raft.jraft.storage.logit.option.StoreOptions;
import org.jetbrains.annotations.TestOnly;

/** Utility methods for creating {@link LogStorageFactory}is for the Shared Log. */
public class SharedLogStorageFactoryUtils {
    /**
     * Enables logit log storage. {@code false} by default. This is a temporary property, that should only be used for testing and comparing
     * the two storages.
     */
    public static final String LOGIT_STORAGE_ENABLED_PROPERTY = "LOGIT_STORAGE_ENABLED";

    /** Creates a LogStorageFactory with the {@link DefaultLogStorageFactory} implementation. */
    @TestOnly
    public static LogStorageFactory create(String nodeName, Path lazyLogStoragePath) {
        return create("test", nodeName, lazyLogStoragePath);
    }

    /** Creates a LogStorageFactory with the {@link DefaultLogStorageFactory} implementation. */
    public static LogStorageFactory create(String factoryName, String nodeName, Path lazyLogStoragePath) {
        return IgniteSystemProperties.getBoolean(LOGIT_STORAGE_ENABLED_PROPERTY, false)
                ? new LogitLogStorageFactory(nodeName, new StoreOptions(), lazyLogStoragePath)
                : new DefaultLogStorageFactory(nodeName, lazyLogStoragePath);
    }
}
