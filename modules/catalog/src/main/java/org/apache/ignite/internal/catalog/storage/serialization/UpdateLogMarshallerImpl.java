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
    private static final int PROTOCOL_VERSION = 2;

    /** Serializers provider. */
    private final CatalogEntrySerializerProvider serializers;

    private final int protocolVersion;

    @TestOnly
    public UpdateLogMarshallerImpl(CatalogEntrySerializerProvider serializers) {
        this(serializers, PROTOCOL_VERSION);
    }

    public UpdateLogMarshallerImpl(int protocolVersion) {
        this(CatalogEntrySerializerProvider.DEFAULT_PROVIDER, protocolVersion);
    }

    public UpdateLogMarshallerImpl(CatalogEntrySerializerProvider serializers, int protocolVersion) {
        this.serializers = serializers;
        this.protocolVersion = protocolVersion;
    }

    @Override
    public byte[] marshall(UpdateLogEvent update) {
        try (CatalogObjectDataOutput output = new CatalogObjectDataOutput(serializers)) {
            output.writeShort(protocolVersion);
            if (protocolVersion == 1) {
                output.writeShort(update.typeId());
                serializers.get(1, update.typeId()).writeTo(update, output);
            } else {
                output.writeEntry(update);
            }
            return output.array();
        } catch (Throwable t) {
            throw new CatalogMarshallerException(t);
        }
    }

    @Override
    public UpdateLogEvent unmarshall(byte[] bytes) {
        try (CatalogObjectDataInput input = new CatalogObjectDataInput(serializers, bytes)) {
            int protoVersion = input.readShort();

            switch (protoVersion) {
                case 1: {
                    int typeId = input.readShort();
                    return (UpdateLogEvent) serializers.get(1, typeId).readFrom(input);
                }
                case 2: {
                    return (UpdateLogEvent) input.readEntry();
                }
                default:
                    throw new IllegalStateException(format("An object could not be deserialized because it was using a newer"
                            + " version of the serialization protocol [supported={}, actual={}].", protocolVersion, protoVersion));
            }
        } catch (Throwable t) {
            throw new CatalogMarshallerException(t);
        }
    }
}
