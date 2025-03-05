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

import org.apache.ignite.internal.catalog.storage.UpdateLogEvent;
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
 *
 * <p>Implementation notes:
 * <ul>
 * <li>Each serializable object must implement the {@link MarshallableEntry} interface,
 *     which defines the type of object being serialized.</li>
 * <li>All serializable object types used in production must be listed in the {@link MarshallableEntryType} enumeration.</li>
 * <li>For each serializable object, there must be an external serializer that implements the {@link CatalogObjectSerializer}
 *     interface and is marked with the {@link CatalogSerializer} annotation.
 *     This annotation specifies the serializer version and is used to dynamically
 *     build a registry of all existing serializers.</li>
 * <li>When serializing an object, a header is written for it consisting of the
 *     object type (2 bytes) and the serializer version (1-3 bytes).</li>
 * </ul>
 *
 *<p>Basic format description:
 *<pre>
 * (size) | description
 * ------------------------------
 *     2  | data format version ({@link #PROTOCOL_VERSION})
 *     2  | entry type ({@link MarshallableEntryType})
 *   1-3  | entry serializer version ({@link CatalogSerializer#version()})
 *      ... (entry payload)
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

            serializers.get(1, update.typeId()).writeTo(update, output);

            return output.array();
        } catch (Throwable t) {
            throw new CatalogMarshallerException(t);
        }
    }

    @Override
    public UpdateLogEvent unmarshall(byte[] bytes) {
        try (IgniteUnsafeDataInput input = new IgniteUnsafeDataInput(bytes)) {
            int protoVersion = input.readShort();

            switch (protoVersion) {
                case 1:
                    int typeId = input.readShort();

                    return (UpdateLogEvent) serializers.get(1, typeId).readFrom(input);

                default:
                    throw new IllegalStateException(format("An object could not be deserialized because it was using a newer"
                            + " version of the serialization protocol [supported={}, actual={}].", PROTOCOL_VERSION, protoVersion));
            }
        } catch (Throwable t) {
            throw new CatalogMarshallerException(t);
        }
    }
}
