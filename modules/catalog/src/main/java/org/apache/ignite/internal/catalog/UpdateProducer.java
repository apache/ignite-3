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

package org.apache.ignite.internal.catalog;

import java.util.List;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;

/**
 * Interface that describes object that can generate list of changes to bring given catalog
 * to desired state.
 */
@FunctionalInterface
public interface UpdateProducer {
    /**
     * Returns list of {@link UpdateEntry entries} to be applied to catalog to bring it to the state
     * described in the command.
     *
     * @param catalog Catalog on the basis of which to generate the list of updates.
     * @return List of updates. Should be empty if no updates actually required.
     */
    List<UpdateEntry> get(Catalog catalog);
}
