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

package org.apache.ignite.internal.metastorage.server;

import java.util.Collection;
import org.apache.ignite.internal.metastorage.Entry;

/**
 * Interface for declaring callbacks that get called after all Meta Storage watches have been notified of a particular revision.
 */
@FunctionalInterface
public interface OnRevisionAppliedCallback {
    /**
     * Notifies of completion of processing of Meta Storage watches for a particular revision.
     *
     * @param revision Revision.
     * @param updatedEntries Entries that have been modified under this revision and processed by at least one watch.
     */
    void onRevisionApplied(long revision, Collection<Entry> updatedEntries);
}
