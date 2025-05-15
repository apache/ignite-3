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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogEntrySerializerProvider;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntry;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.catalog.storage.serialization.UpdateLogMarshaller;
import org.apache.ignite.internal.catalog.storage.serialization.UpdateLogMarshallerImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.assertj.core.api.BDDAssertions;

final class CatalogSerializationChecker {

    private static final String UPDATE_TIMESTAMP_FIELD_REGEX = ".*updateTimestamp";

    private final Map<Integer, Integer> expectedEntryVersions = new HashMap<>();

    private boolean writeSnapshot;

    private final IgniteLogger log;

    private final String directory;

    private final int entryVersion;

    private final boolean expectExactProtocolVersion;

    private final int protocolVersion;

    CatalogSerializationChecker(
            IgniteLogger log,
            String directory,
            int entryVersion,
            boolean expectExactProtocolVersion,
            int protocolVersion
    ) {
        this.log = log;
        this.directory = directory;
        this.entryVersion = entryVersion;
        this.expectExactProtocolVersion = expectExactProtocolVersion;
        this.protocolVersion = protocolVersion;
    }

    void writeSnapshot(boolean value) {
        writeSnapshot = value;
    }

    void addExpectedVersion(int typeId, int entryVersion) {
        expectedEntryVersions.put(typeId, entryVersion);
    }

    void reset() {
        expectedEntryVersions.clear();
        writeSnapshot = false;
    }

    void compareSnapshotEntry(SnapshotEntry expectedEntry, String fileName, int version) {
        SnapshotEntry actualEntry = checkEntry(SnapshotEntry.class, fileName, version, expectedEntry);

        assertEquals(expectedEntry.typeId(), actualEntry.typeId());
        assertEquals(expectedEntry.activationTime(), actualEntry.activationTime(), "activationTime");
        assertEquals(expectedEntry.objectIdGenState(), actualEntry.objectIdGenState(), "objectIdGenState");
        assertEquals(expectedEntry.defaultZoneId(), actualEntry.defaultZoneId(), "defaultZoneId");

        var assertion = BDDAssertions.assertThat(expectedEntry.snapshot())
                .usingRecursiveComparison();

        if (entryVersion == 1) {
            // Ignoring update timestamp for version 1.
            assertion = assertion.ignoringFieldsMatchingRegexes(UPDATE_TIMESTAMP_FIELD_REGEX);
        }

        assertion.isEqualTo(actualEntry.snapshot());
    }

    void compareEntries(List<UpdateEntry> entries, String fileName, int version) {
        List<UpdateEntry> actual = checkEntries(entries, fileName, version);
        assertEquals(entries.size(), actual.size());

        for (int i = 0; i < entries.size(); i++) {
            UpdateEntry expectedEntry = entries.get(i);
            UpdateEntry actualEntry = actual.get(i);

            var assertion = BDDAssertions.assertThat(actualEntry).as("entry#" + i)
                    .usingRecursiveComparison();

            if (entryVersion == 1) {
                // Ignoring update timestamp for version 1.
                assertion = assertion.ignoringFieldsMatchingRegexes(UPDATE_TIMESTAMP_FIELD_REGEX);
            }

            assertion.isEqualTo(expectedEntry);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <T extends UpdateEntry> List<T> checkEntries(List<? extends T> entries, String fileName, int version) {
        VersionedUpdate update = new VersionedUpdate(1, 100L, (List<UpdateEntry>) entries);
        VersionedUpdate deserializedUpdate = checkEntry(VersionedUpdate.class, fileName, version, update);

        assertEquals(update.version(), deserializedUpdate.version());
        assertEquals(update.typeId(), deserializedUpdate.typeId());
        assertEquals(update.delayDurationMs(), deserializedUpdate.delayDurationMs());

        return (List) deserializedUpdate.entries();
    }

    private <T extends UpdateLogEvent> T checkEntry(Class<T> entryClass, String entryFileName, int entryVersion, UpdateLogEvent entry) {
        String fileName = format("{}_{}.bin", entryFileName, entryVersion);
        String resourceName = directory + "/" + fileName;

        CatalogEntrySerializerProvider defaultProvider = CatalogEntrySerializerProvider.DEFAULT_PROVIDER;
        CatalogEntrySerializerProvider provider;

        if (expectExactProtocolVersion) {
            provider = new VersionCheckingProvider(defaultProvider, protocolVersion, expectedEntryVersions);
        } else {
            provider = defaultProvider;
        }

        log.info("Read fileName: {}, class: {}, entryVersion: {}", fileName, entryClass.getSimpleName(), entryVersion);

        UpdateLogMarshaller marshaller = new UpdateLogMarshallerImpl(provider, protocolVersion);

        if (writeSnapshot) {
            writeEntry(entry, Path.of("src", "test", "resources", directory, fileName), marshaller);
        }

        byte[] srcBytes;

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourceName)) {
            assertNotNull(is, "Resource does not exist: " + resourceName);
            while (is.available() > 0) {
                bos.write(is.read());
            }
            srcBytes = bos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read resource " + resourceName, e);
        }

        return entryClass.cast(marshaller.unmarshall(srcBytes));
    }

    private void writeEntry(UpdateLogEvent entry, Path resourcePath, UpdateLogMarshaller marshaller) {
        log.info("Writing entry to {}", resourcePath);

        try {
            Files.write(resourcePath, marshaller.marshall(entry));
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to write resource", e);
        }
    }

    private static class VersionCheckingProvider implements CatalogEntrySerializerProvider {

        private final CatalogEntrySerializerProvider provider;

        private final int protocolVersion;

        private final Map<Integer, Integer> entryVersions = new HashMap<>();

        private VersionCheckingProvider(
                CatalogEntrySerializerProvider provider,
                int expectedProtocolVersion,
                Map<Integer, Integer> entryVersions
        ) {
            this.provider = provider;
            this.protocolVersion = expectedProtocolVersion;
            this.entryVersions.putAll(entryVersions);
        }

        @Override
        public <T extends MarshallableEntry> CatalogObjectSerializer<T> get(int version, int typeId) {
            CatalogObjectSerializer<MarshallableEntry> serializer = provider.get(version, typeId);

            checkVersion(typeId, version);

            return (CatalogObjectSerializer<T>) serializer;
        }

        @Override
        public int latestSerializerVersion(int typeId) {
            int latest = provider.latestSerializerVersion(typeId);

            checkVersion(typeId, latest);
            return latest;
        }

        private void checkVersion(int typeId, int entryVersion) {
            int expectedEntryVersion = entryVersions.getOrDefault(typeId, protocolVersion);
            if (entryVersion != expectedEntryVersion) {
                MarshallableEntryType type = null;

                for (MarshallableEntryType t : MarshallableEntryType.values()) {
                    if (t.id() == typeId) {
                        type = t;
                        break;
                    }
                }

                String message = format(
                        "Requested unexpected version for type {}[typeId={}] does not match. Expected {} but got {}",
                        type, typeId, expectedEntryVersion, entryVersion
                );
                fail(message);
            }
        }
    }
}
