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

import static org.junit.jupiter.api.Assertions.fail;

import java.util.Set;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptorSerializers;
import org.apache.ignite.internal.catalog.storage.CatalogSerializationChecker.SerializerClass;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.CollectionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Tests for catalog storage objects. Protocol version 1.
 *
 * <p>How to add a test case:
 * <ul>
 *     <li>Update descriptor generator(s) to return version(s) you want to test.
 *     See {@link TestTableDescriptors}, {@link TestZoneDescriptors}, and so on.</li>
 *     <li>Copy an existing test case.</li>
 *     <li>Set versions to those you what to check.</li>
 *     <li>Set {@link #WRITE_SNAPSHOT}  to {@code true} to generate a snapshot.</li>
 *     <li>Run test case to generate a snapshot, set {@link #WRITE_SNAPSHOT} back to false.</li>
 * </ul>
 * <pre>
 *     public void newTableV42() {
 *         int version = 42;
 *
 *         // Generates table descriptors for version 42.
 *         List&lt;UpdateEntry&gt; entries = TestTableDescriptors.tables(state, version)
 *                 .stream()
 *                 .map(NewTableEntry::new)
 *                 .collect(Collectors.toList());
 *
 *         // Use serializer version 42 for all CatalogTableDescriptors in this test case.
 *         checker.addExpectedVersion(MarshallableEntryType.DESCRIPTOR_TABLE.id(), version);
 *         checker.compareEntries(entries, "NewTableEntry", version);
 *     }
 * </pre>
 *
 * <p>Sometimes you may want to add a snapshot for a new version of a nested object. For this
 * you still need to add a new test for top level object nesting an object of interest. For
 * example, to add a snapshot for a {@link CatalogTableColumnDescriptorSerializers} of version 50,
 * we need to add a new test like this:
 * <pre>
 *     public void newTableV&lt;nextAvailableVersion&gt;() {
 *         int tableSerializerVersion = 2; // latest available version of top level object serializer
 *         int tableColumnSerializerVersion = 3; // the version we would like to create snapshot for
 *         int snapshotFileSuffix = 3; // next available version
 *
 *         List&lt;UpdateEntry&gt; entries = ... // create descriptor here
 *
 *         checker.addExpectedVersion(MarshallableEntryType.DESCRIPTOR_TABLE_COLUMN.id(), tableColumnSerializerVersion);
 *         checker.compareEntries(entries, "NewTableEntry", snapshotFileSuffix);
 *     }
 * </pre>
 */
public abstract class CatalogSerializationCompatibilityTest extends BaseIgniteAbstractTest {

    /** Whether to write entries to the test directory. */
    private static final boolean WRITE_SNAPSHOT = false;

    final TestDescriptorState state = new TestDescriptorState(42);

    CatalogSerializationChecker checker;

    @BeforeEach
    public void init() {
        checker = new CatalogSerializationChecker(
                log,
                dirName(),
                defaultEntryVersion(),
                expectExactVersion(),
                protocolVersion(),
                this::recordClass
        );
        checker.writeSnapshot(WRITE_SNAPSHOT);
    }

    @AfterEach
    public void reset() {
        checker.reset();
    }

    /** Serialization protocol version. */
    protected abstract int protocolVersion();

    /**
     * Default entry serializer version, This version is used for very object unless another version is added via
     * {@link CatalogSerializationChecker#addExpectedVersion(int, int)}.
     */
    protected abstract int defaultEntryVersion();

    protected abstract String dirName();

    /** Used for tests that check that protocol v2 serializers reads v1 objects. */
    protected boolean expectExactVersion() {
        return true;
    }

    protected void recordClass(SerializerClass clazz) {
        // does nothing
    }

    static void compareSerializers(Set<SerializerClass> expected, Set<SerializerClass> actual) {
        if (actual.equals(expected)) {
            return;
        }

        Set<SerializerClass> serializersWithoutTests = CollectionUtils.difference(expected, actual);
        if (!serializersWithoutTests.isEmpty()) {
            fail("There no tests for the following serializers: " + serializersWithoutTests);
        }

        Set<SerializerClass> testsWithoutSerializers = CollectionUtils.difference(actual, expected);
        if (!testsWithoutSerializers.isEmpty()) {
            fail("There are tests for these classes but their serializers are missing: " + testsWithoutSerializers);
        }
    }
}
