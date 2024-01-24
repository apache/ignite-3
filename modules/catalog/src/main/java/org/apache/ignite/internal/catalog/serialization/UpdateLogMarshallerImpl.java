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
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.apache.ignite.internal.catalog.storage.VersionedUpdate;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataInput;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataOutput;
import org.apache.ignite.lang.MarshallerException;
import org.jetbrains.annotations.TestOnly;

/**
 * Marshaller of update log entries that uses custom serializer.
 */
public class UpdateLogMarshallerImpl implements UpdateLogMarshaller {
    /** Current data format version. */
    private static final int PROTOCOL_VERSION = 1;

    /** Required data format version. */
    private final int version;

    /** Serializers provider. */
    private final CatalogEntrySerializerProvider serializers;

    public UpdateLogMarshallerImpl() {
        this.version = PROTOCOL_VERSION;
        this.serializers = CatalogEntrySerializerProvider.DEFAULT_PROVIDER;
    }

    @TestOnly
    UpdateLogMarshallerImpl(int version, CatalogEntrySerializerProvider serializers) {
        this.version = version;
        this.serializers = serializers;
    }

    @Override
    public byte[] marshall(VersionedUpdate update) {
        IgniteUnsafeDataOutput output = new IgniteUnsafeDataOutput(32);

        try {
            output.writeShort(version);

            output.writeInt(update.entries().size());

            for (UpdateEntry entry : update.entries()) {
                output.writeShort(entry.typeId());

                CatalogObjectSerializer<? super UpdateEntry> serializer = serializers.get(entry.typeId());
                serializer.writeTo(entry, version, output);
            }

            output.writeInt(update.version());
            output.writeLong(update.delayDurationMs());

            return output.array();
        } catch (Throwable t) {
            throw new MarshallerException(t);
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

                CatalogObjectSerializer<UpdateEntry> serializer = serializers.get(entryTypeId);

                entries.add(serializer.readFrom(updateEntryVersion, input));
            }

            int version = input.readInt();
            long delayDurationMs = input.readLong();

            return new VersionedUpdate(version, delayDurationMs, entries);
        } catch (Throwable t) {
            throw new MarshallerException(t);
        }
    }
}
