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

package org.apache.ignite.internal.catalog.serialization;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import com.jayway.jsonpath.internal.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.apache.ignite.internal.catalog.storage.VersionedUpdate;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataInput;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataOutput;
import org.jetbrains.annotations.TestOnly;

/**
 * Marshaller of update log entries that uses an external serializer.
 */
public class UpdateLogMarshallerImpl implements UpdateLogMarshaller {
    /** Current protocol version. */
    private static final int PROTOCOL_VERSION = 1;

    /** Serialization version. */
    private final int version;

    private final IntFunction<CatalogEntrySerializer<UpdateEntry>> serializerProvider;

    public UpdateLogMarshallerImpl() {
        this.version = PROTOCOL_VERSION;
        this.serializerProvider = (id) -> CatalogEntrySerializer.forTypeId((short) id);
    }

    @TestOnly
    UpdateLogMarshallerImpl(int version, IntFunction<CatalogEntrySerializer<UpdateEntry>> serializerProvider) {
        this.version = version;
        this.serializerProvider = serializerProvider;
    }

    @Override
    public byte[] marshall(VersionedUpdate update) {
        IgniteUnsafeDataOutput output = new IgniteUnsafeDataOutput(32);

        try {
            output.writeShort(version);

            output.writeInt(update.entries().size());

            for (UpdateEntry entry : update.entries()) {
                output.writeShort(entry.typeId());

                CatalogEntrySerializer<UpdateEntry> serializer = serializerProvider.apply(entry.typeId());
                serializer.writeTo(entry, version, output);
            }

            output.writeInt(update.version());
            output.writeLong(update.delayDurationMs());

            return output.array();
        } catch (IOException e) {
            throw new IgniteInternalException("Could not serialize update: " + update, e);
        } finally {
            Utils.closeQuietly(output);
        }
    }

    @Override
    public VersionedUpdate unmarshall(byte[] bytes) {
        try (IgniteUnsafeDataInput input = new IgniteUnsafeDataInput(bytes)) {
            int updateEntryVersion = input.readShort();

            if (updateEntryVersion > version) {
                throw new IllegalStateException(format("An object could not be deserialized because it was using "
                        + "a newer version of the serialization protocol [objectVersion={}, supported={}]", updateEntryVersion, version));
            }

            int size = input.readInt();
            List<UpdateEntry> entries = new ArrayList<>(size);

            for (int i = 0; i < size; i++) {
                short entryTypeId = input.readShort();

                UpdateEntryType entryType = UpdateEntryType.getById(entryTypeId);

                if (entryType == null) {
                    throw new IllegalStateException("Unknown entry type: " + entryTypeId);
                }

                CatalogEntrySerializer<UpdateEntry> serializer = serializerProvider.apply(entryTypeId);

                entries.add(serializer.readFrom((short) updateEntryVersion, input));
            }

            int version = input.readInt();
            long delayDurationMs = input.readLong();

            return new VersionedUpdate(version, delayDurationMs, entries);
        } catch (IOException e) {
            throw new IgniteInternalException("Could not deserialize an object", e);
        }
    }
}
