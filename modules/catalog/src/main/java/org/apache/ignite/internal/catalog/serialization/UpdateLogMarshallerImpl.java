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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.catalog.storage.SnapshotEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.apache.ignite.internal.catalog.storage.UpdateLogEvent;
import org.apache.ignite.internal.catalog.storage.VersionedUpdate;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataInput;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataOutput;
import org.apache.ignite.lang.MarshallerException;

/**
 * Marshaller of update log entries that uses custom serializer.
 */
public class UpdateLogMarshallerImpl implements UpdateLogMarshaller {
    /** Current data format version. */
    private static final int PROTOCOL_VERSION = 1;

    /** Required data format version. */
    private final int protocolVersion;

    private final CatalogObjectSerializer<VersionedUpdate> versionedUpdateSerializer;

    public UpdateLogMarshallerImpl() {
        this(PROTOCOL_VERSION, CatalogEntrySerializerProvider.DEFAULT_PROVIDER);
    }

    UpdateLogMarshallerImpl(int protocolVersion, CatalogEntrySerializerProvider serializers) {
        this.protocolVersion = protocolVersion;
        this.versionedUpdateSerializer = new VersionedUpdateSerializer(serializers);
    }

    @Override
    public byte[] marshall(UpdateLogEvent update) {
        try (IgniteUnsafeDataOutput output = new IgniteUnsafeDataOutput(256)) {
            output.writeShort(protocolVersion);

            if (update instanceof VersionedUpdate) {
                output.writeByte(0);

                versionedUpdateSerializer.writeTo((VersionedUpdate) update, protocolVersion, output);
            } else {
                assert update instanceof SnapshotEntry;

                output.writeByte(1);

                SnapshotEntry.SERIALIZER.writeTo((SnapshotEntry) update, protocolVersion, output);
            }

            return output.array();
        } catch (Throwable t) {
            throw new MarshallerException(t);
        }
    }

    @Override
    public UpdateLogEvent unmarshall(byte[] bytes) {
        try (IgniteUnsafeDataInput input = new IgniteUnsafeDataInput(bytes)) {
            int entryVersion = input.readShort();

            if (entryVersion > protocolVersion) {
                throw new IllegalStateException(format("An object could not be deserialized because it was using "
                        + "a newer version of the serialization protocol [objectVersion={}, supported={}]", entryVersion, protocolVersion));
            }

            int updateLogEventType = input.readByte();

            assert updateLogEventType == 0 || updateLogEventType == 1 : "Unknown type: " + updateLogEventType;

            if (updateLogEventType == 0) {
                return versionedUpdateSerializer.readFrom(entryVersion, input);
            } else {
                return SnapshotEntry.SERIALIZER.readFrom(entryVersion, input);
            }
        } catch (Throwable t) {
            throw new MarshallerException(t);
        }
    }

    private static class VersionedUpdateSerializer implements CatalogObjectSerializer<VersionedUpdate> {
        private final CatalogEntrySerializerProvider serializers;

        private VersionedUpdateSerializer(CatalogEntrySerializerProvider provider) {
            this.serializers = provider;
        }

        @Override
        public VersionedUpdate readFrom(int version, IgniteDataInput input) throws IOException {
            int ver = input.readInt();
            long delayDurationMs = input.readLong();

            int size = input.readInt();
            List<UpdateEntry> entries = new ArrayList<>(size);

            for (int i = 0; i < size; i++) {
                short entryTypeId = input.readShort();

                CatalogObjectSerializer<UpdateEntry> serializer = serializers.get(entryTypeId);

                entries.add(serializer.readFrom(version, input));
            }

            return new VersionedUpdate(ver, delayDurationMs, entries);
        }

        @Override
        public void writeTo(VersionedUpdate update, int version, IgniteDataOutput output) throws IOException {
            output.writeInt(update.version());
            output.writeLong(update.delayDurationMs());

            output.writeInt(update.entries().size());
            for (UpdateEntry entry : update.entries()) {
                output.writeShort(entry.typeId());
                CatalogObjectSerializer<? super UpdateEntry> serializer = serializers.get(entry.typeId());
                serializer.writeTo(entry, version, output);
            }
        }
    }
}
