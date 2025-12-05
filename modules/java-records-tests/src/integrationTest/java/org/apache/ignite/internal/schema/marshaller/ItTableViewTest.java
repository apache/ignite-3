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

package org.apache.ignite.internal.schema.marshaller;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.schema.marshaller.AssertMarshaller.assertView;
import static org.apache.ignite.internal.schema.marshaller.AssertMarshaller.assertViewThrows;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Objects;
import java.util.stream.Stream;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.schema.marshaller.Records.ComponentsEmpty;
import org.apache.ignite.internal.schema.marshaller.Records.ComponentsExact;
import org.apache.ignite.internal.schema.marshaller.Records.ComponentsNarrow;
import org.apache.ignite.internal.schema.marshaller.Records.ComponentsReordered;
import org.apache.ignite.internal.schema.marshaller.Records.ComponentsWide;
import org.apache.ignite.internal.schema.marshaller.Records.ComponentsWrongTypes;
import org.apache.ignite.internal.schema.marshaller.Records.NotAnnotatedNotMapped;
import org.apache.ignite.lang.MarshallerException;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Ensures that records and classes behave the same way.
 */
class ItTableViewTest extends ClusterPerClassIntegrationTest {
    private static final String TBL = "TBL";
    private static IgniteClient CLIENT;
    private static Table TABLE_EMBEDDED;
    private static Table TABLE_CLIENT;

    private static Stream<Arguments> tableImpls() {
        return Stream.of(
                arguments(named("embedded", TABLE_EMBEDDED)),
                arguments(named("client", TABLE_CLIENT))
        );
    }

    @BeforeAll
    static void beforeAll() {
        CLIENT = IgniteClient.builder()
                .addresses("localhost:10800")
                .build();

        sql(format(Records.SQL_PATTERN, TBL));

        await().ignoreExceptions().untilAsserted(() -> {
            TABLE_EMBEDDED = Objects.requireNonNull(CLUSTER.aliveNode().tables().table(TBL));
            TABLE_CLIENT = Objects.requireNonNull(CLIENT.tables().table(TBL));
        });
    }

    @AfterEach
    void tearDown() {
        sql("DELETE FROM " + TBL + ";");
    }

    @AfterAll
    static void afterAll() {
        CLIENT.close();
    }

    @ParameterizedTest
    @MethodSource("tableImpls")
    void recordViewTest(Table t) {
        assertView(t, new ComponentsExact.Record(1, "a"));
        assertView(t, new ComponentsExact.Class(1, "a"));

        assertView(t, new ComponentsExact.ExplicitCanonical(1, "a"));
        assertView(t, new ComponentsExact.ExplicitCompact(1, "a"));
        assertView(t, new ComponentsExact.ExplicitMultiple(1, "a"));
        assertView(t, new ComponentsExact.ExplicitNoArgs(1, "a"));

        assertView(t, new ComponentsNarrow.Record(1));
        assertView(t, new ComponentsNarrow.Class(1));

        assertView(t, new ComponentsReordered.Record("a", 1));
        assertView(t, new ComponentsReordered.Class("a", 1));
    }

    @ParameterizedTest
    @MethodSource("tableImpls")
    void keyValueViewTest(Table t) {
        assertView(t, new ComponentsExact.RecordK(1), new ComponentsExact.RecordV("a"));
        assertView(t, new ComponentsExact.ClassK(1), new ComponentsExact.ClassV("a"));

        assertView(t, new ComponentsExact.RecordK(1), new ComponentsExact.ExplicitCanonicalV("a"));
        assertView(t, new ComponentsExact.RecordK(1), new ComponentsExact.ExplicitCompactV("a"));
        assertView(t, new ComponentsExact.RecordK(1), new ComponentsExact.ExplicitMultipleV("a"));
        assertView(t, new ComponentsExact.RecordK(1), new ComponentsExact.ExplicitNoArgsV());
    }

    @ParameterizedTest
    @MethodSource("tableImpls")
    void componentsWide(Table t) {
        String msgSubstring = "are not mapped to columns";

        assertViewThrows(MarshallerException.class, msgSubstring, t, new ComponentsWide.Record(1, "a", "b", "c"));
        assertViewThrows(MarshallerException.class, msgSubstring, t, new ComponentsWide.Class(1, "a", "b", "c"));

        assertViewThrows(MarshallerException.class, msgSubstring, t,
                new ComponentsExact.RecordK(1),
                new ComponentsWide.Record(1, "a", "b", "c")
        );
        assertViewThrows(MarshallerException.class, msgSubstring, t,
                new ComponentsExact.ClassK(1),
                new ComponentsWide.Class(1, "a", "b", "c")
        );
    }

    @ParameterizedTest
    @MethodSource("tableImpls")
    void componentsWrongTypesThrowsException(Table t) {
        String msgSubstring = "Column's type mismatch";

        assertViewThrows(ClassCastException.class, msgSubstring, t, new ComponentsWrongTypes.Record((short) 1, 2));
        assertViewThrows(ClassCastException.class, msgSubstring, t, new ComponentsWrongTypes.Class((short) 1, 2));

        assertViewThrows(ClassCastException.class, msgSubstring, t,
                new ComponentsWrongTypes.RecordK((short) 1),
                new ComponentsWrongTypes.RecordV(2)
        );
        assertViewThrows(ClassCastException.class, msgSubstring, t,
                new ComponentsWrongTypes.ClassK((short) 1),
                new ComponentsWrongTypes.ClassV(2)
        );
    }

    @ParameterizedTest
    @MethodSource("tableImpls")
    void componentsEmptyThrowsException(Table t) {
        String msgSubstring = "Empty mapping isn't allowed";

        assertViewThrows(IllegalArgumentException.class, msgSubstring, t, new ComponentsEmpty.Record());
        assertViewThrows(IllegalArgumentException.class, msgSubstring, t, new ComponentsEmpty.Class());

        assertViewThrows(IllegalArgumentException.class, msgSubstring, t,
                new ComponentsExact.RecordK(1),
                new ComponentsEmpty.Record()
        );
        assertViewThrows(IllegalArgumentException.class, msgSubstring, t,
                new ComponentsExact.ClassK(1),
                new ComponentsEmpty.Class()
        );
    }

    @ParameterizedTest
    @MethodSource("tableImpls")
    void notAnnotatedNotMappedThrowsException(Table t) {
        String msgSubstring = "No mapped object field found";

        assertViewThrows(MarshallerException.class, msgSubstring, t, new NotAnnotatedNotMapped.Record(1, "a"));
        assertViewThrows(MarshallerException.class, msgSubstring, t, new NotAnnotatedNotMapped.Class(1, "a"));

        msgSubstring = "are not mapped to columns";
        assertViewThrows(MarshallerException.class, msgSubstring, t,
                new ComponentsExact.RecordK(1),
                new NotAnnotatedNotMapped.Record(1, "a")
        );
        assertViewThrows(MarshallerException.class, msgSubstring, t,
                new ComponentsExact.ClassK(1),
                new NotAnnotatedNotMapped.Class(1, "a")
        );
    }
}
