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

package org.apache.ignite.internal.catalog.storage;

import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;

import org.apache.ignite.internal.catalog.commands.MakeIndexAvailableCommand;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.MakeIndexAvailableEventParameters;

/** Entry for {@link MakeIndexAvailableCommand}. */
public class MakeIndexAvailableEntry extends AbstractChangeIndexStatusEntry implements Fireable {
    private static final long serialVersionUID = -5686678143537999594L;

    /** Constructor. */
    public MakeIndexAvailableEntry(int indexId) {
        super(indexId, AVAILABLE);
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.INDEX_AVAILABLE;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new MakeIndexAvailableEventParameters(causalityToken, catalogVersion, indexId);
    }
}
