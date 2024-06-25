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

package org.apache.ignite.internal.catalog.storage.serialization;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import org.apache.ignite.internal.catalog.storage.SnapshotEntry;
import org.apache.ignite.internal.catalog.storage.UpdateLogEvent;
import org.apache.ignite.internal.catalog.storage.VersionedUpdate;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataInput;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataOutput;
import org.jetbrains.annotations.TestOnly;

/**
 * Marshaller of update log entries that uses custom serializer.
 *
 *<p>External serialization was implemented for two reasons:
 *<ul>
 * <li>Minimize the size of the serialized update log entry.</li>
 * <li>Backward compatibility support when changing existing or adding new entry.</li>
 * </ul>
 * At the moment, the serialization format is specified manually.
 *
 *<p>While backward compatibility is not required, entities can change arbitrarily without changing the data format version.
 * However, every change to an entry's fields requires a change to the serializer that serializes that entry.
 *
 *<p>Basic format description:
 *<pre>
 * (size) | description
 * ------------------------------
 *     2  | data format version ({@link #PROTOCOL_VERSION})
 *     2  | entry type ({@link VersionedUpdate} or {@link SnapshotEntry})
 * &lt;list&gt; | fields of the corresponding event follow (for example, {@link VersionedUpdate}
 * </pre>
 *
 *<p>Versioned Update format description:
 *<pre>
 *     4  | version
 *     8  | delayDurationMs
 * &lt;list&gt; | list of {@link MarshallableEntry}
 *</pre>
 *
 *<p>MarshallableEntry format:
 * <pre>
 *     4  | entries size
 *     2  | entry type (see {@link MarshallableEntryType#id()})
 *        | other entry fields
 *
 *     ...
 * </pre>
 */
public class UpdateLogMarshallerImpl implements UpdateLogMarshaller {
    /** Current data format version. */
    private static final int PROTOCOL_VERSION = 1;

    /** Initial capacity (in bytes) of the buffer used for data output. */
    private static final int INITIAL_BUFFER_CAPACITY = 256;

    /** Serializers provider. */
    private final CatalogEntrySerializerProvider serializers;

    public UpdateLogMarshallerImpl() {
        this.serializers = CatalogEntrySerializerProvider.DEFAULT_PROVIDER;
    }

    @TestOnly
    public UpdateLogMarshallerImpl(CatalogEntrySerializerProvider serializers) {
        this.serializers = serializers;
    }

    @Override
    public byte[] marshall(UpdateLogEvent update) {
        try (IgniteUnsafeDataOutput output = new IgniteUnsafeDataOutput(INITIAL_BUFFER_CAPACITY)) {
            output.writeShort(PROTOCOL_VERSION);

            output.writeShort(update.typeId());

            serializers.get(update.typeId()).writeTo(update, output);

            return output.array();
        } catch (Throwable t) {
            throw new CatalogMarshallerException(t);
        }
    }

    @Override
    public UpdateLogEvent unmarshall(byte[] bytes) {
        try (IgniteUnsafeDataInput input = new IgniteUnsafeDataInput(bytes)) {
            int version = input.readShort();

            if (version > PROTOCOL_VERSION) {
                throw new IllegalStateException(format("An object could not be deserialized because it was using "
                        + "a newer version of the serialization protocol [objectVersion={}, supported={}]", version, PROTOCOL_VERSION));
            }

            int typeId = input.readShort();

            return (UpdateLogEvent) serializers.get(typeId).readFrom(input);
        } catch (Throwable t) {
            throw new CatalogMarshallerException(t);
        }
    }
}
