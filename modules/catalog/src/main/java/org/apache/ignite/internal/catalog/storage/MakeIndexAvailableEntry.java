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

import java.io.IOException;
import org.apache.ignite.internal.catalog.commands.MakeIndexAvailableCommand;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.MakeIndexAvailableEventParameters;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/** Entry for {@link MakeIndexAvailableCommand}. */
public class MakeIndexAvailableEntry extends AbstractChangeIndexStatusEntry implements Fireable {
    public static final CatalogObjectSerializer<MakeIndexAvailableEntry> SERIALIZER = new MakeIndexAvailableEntrySerializer();

    /** Constructor. */
    public MakeIndexAvailableEntry(int indexId) {
        super(indexId, AVAILABLE);
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.MAKE_INDEX_AVAILABLE.id();
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.INDEX_AVAILABLE;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new MakeIndexAvailableEventParameters(causalityToken, catalogVersion, indexId);
    }

    /**
     * Serializer for {@link MakeIndexAvailableEntry}.
     */
    private static class MakeIndexAvailableEntrySerializer implements CatalogObjectSerializer<MakeIndexAvailableEntry> {
        @Override
        public MakeIndexAvailableEntry readFrom(IgniteDataInput input) throws IOException {
            int indexId = input.readInt();

            return new MakeIndexAvailableEntry(indexId);
        }

        @Override
        public void writeTo(MakeIndexAvailableEntry object, IgniteDataOutput output) throws IOException {
            output.writeInt(object.indexId);
        }
    }
}
