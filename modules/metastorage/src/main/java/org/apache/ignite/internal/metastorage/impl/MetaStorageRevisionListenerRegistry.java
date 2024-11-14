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

package org.apache.ignite.internal.metastorage.impl;

import org.apache.ignite.internal.causality.RevisionListener;
import org.apache.ignite.internal.causality.RevisionListenerRegistry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;

/** Implementation based on {@link MetaStorageManager}. */
public class MetaStorageRevisionListenerRegistry implements RevisionListenerRegistry {
    private final MetaStorageManager metaStorageManager;

    /** Constructor. */
    public MetaStorageRevisionListenerRegistry(MetaStorageManager metaStorageManager) {
        this.metaStorageManager = metaStorageManager;
    }

    @Override
    public void listen(RevisionListener listener) {
        metaStorageManager.registerRevisionUpdateListener(listener::onUpdate);
        metaStorageManager.registerCompactionRevisionUpdateListener(listener::onDelete);
    }
}
