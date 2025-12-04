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

package org.apache.ignite.internal.table;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.table.KeyValueTestUtils.ALL_TYPES_COLUMNS;
import static org.apache.ignite.internal.table.KeyValueTestUtils.newKey;
import static org.apache.ignite.internal.type.NativeTypes.INT64;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.marshaller.MarshallersProvider;
import org.apache.ignite.internal.marshaller.ReflectionMarshallersProvider;
import org.apache.ignite.internal.marshaller.testobjects.TestObjectWithAllTypes;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.partition.replicator.schemacompat.InternalSchemaVersionMismatchException;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.marshaller.reflection.KvMarshallerImpl;
import org.apache.ignite.internal.schema.marshaller.reflection.RecordMarshallerImpl;
import org.apache.ignite.internal.table.KeyValueTestUtils.TestKeyObject;
import org.apache.ignite.internal.table.distributed.schema.ConstantSchemaVersions;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.table.impl.DummySchemaManagerImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for key-value operations that uses mocked structures and internal API.
 *
 *<p>For public API tests use ItTableViewApiUnifiedBaseTest.
 */
@ExtendWith({MockitoExtension.class, ConfigurationExtension.class})
public class TableKvOperationsMockedTest extends BaseIgniteAbstractTest {
    private static final int SCHEMA_VERSION = 1;

    private final SchemaVersions schemaVersions = new ConstantSchemaVersions(SCHEMA_VERSION);

    @InjectConfiguration
    private TransactionConfiguration txConfiguration;

    @InjectConfiguration
    private ReplicationConfiguration replicationConfiguration;

    @InjectConfiguration
    private SystemDistributedConfiguration systemDistributedConfiguration;

    @Mock(answer = RETURNS_DEEP_STUBS)
    private ReplicaService replicaService;

    /** Simple schema with one key and one value column. */
    private final SchemaDescriptor simpleSchema = new SchemaDescriptor(
            SCHEMA_VERSION,
            new Column[]{new Column("id".toUpperCase(), INT64, false)},
            new Column[]{new Column("val".toUpperCase(), INT64, true)}
    );

    @Test
    void retriesOnInternalSchemaVersionMismatchExceptionKvView() {
        Random rnd = new Random();
        Mapper<TestKeyObject> keyMapper = Mapper.of(TestKeyObject.class);
        Mapper<TestObjectWithAllTypes> valMapper = Mapper.of(TestObjectWithAllTypes.class);

        SchemaDescriptor schema = new SchemaDescriptor(
                SCHEMA_VERSION,
                new Column[]{new Column("id".toUpperCase(), INT64, false)},
                ALL_TYPES_COLUMNS
        );
        InternalTable internalTable = spy(createInternalTable(schema));
        KeyValueViewImpl<TestKeyObject, TestObjectWithAllTypes> view = kvView(internalTable, schema, keyMapper, valMapper);

        TestKeyObject key = newKey(1);
        TestObjectWithAllTypes expectedValue = TestObjectWithAllTypes.randomObject(rnd);
        MarshallersProvider marshallers = new ReflectionMarshallersProvider();

        BinaryRow resultRow = new KvMarshallerImpl<>(schema, marshallers, keyMapper, valMapper)
                .marshal(key, expectedValue);

        doReturn(failedFuture(new InternalSchemaVersionMismatchException()))
                .doReturn(completedFuture(resultRow))
                .when(internalTable).get(any(), any());

        TestObjectWithAllTypes result = view.get(null, newKey(1L));

        assertThat(result, is(equalTo(expectedValue)));

        verify(internalTable, times(2)).get(any(), isNull());
    }

    @Test
    void retriesOnInternalSchemaVersionMismatchExceptionKvBinaryView() {
        InternalTable internalTable = spy(createInternalTable(simpleSchema));
        ReflectionMarshallersProvider marshallers = new ReflectionMarshallersProvider();

        KeyValueView<Tuple, Tuple> view = new KeyValueBinaryViewImpl(
                internalTable,
                new DummySchemaManagerImpl(simpleSchema),
                schemaVersions,
                mock(IgniteSql.class),
                marshallers
        );

        BinaryRow resultRow = new TupleMarshallerImpl(internalTable::name, simpleSchema)
                .marshal(Tuple.create().set("ID", 1L).set("VAL", 2L));

        doReturn(failedFuture(new InternalSchemaVersionMismatchException()))
                .doReturn(completedFuture(resultRow))
                .when(internalTable).get(any(), any());

        Tuple result = view.get(null, Tuple.create().set("ID", 1L));

        assertNotNull(result);

        assertThat(result.longValue("VAL"), is(2L));

        verify(internalTable, times(2)).get(any(), isNull());
    }

    @Test
    void retriesOnInternalSchemaVersionMismatchExceptionBinaryRecordView() {
        InternalTable internalTable = spy(createInternalTable(simpleSchema));
        ReflectionMarshallersProvider marshallers = new ReflectionMarshallersProvider();

        RecordView<Tuple> view = new RecordBinaryViewImpl(
                internalTable,
                new DummySchemaManagerImpl(simpleSchema),
                schemaVersions,
                mock(IgniteSql.class),
                marshallers
        );

        BinaryRow resultRow = new TupleMarshallerImpl(internalTable::name, simpleSchema)
                .marshal(Tuple.create().set("id", 1L).set("val", 2L));

        doReturn(failedFuture(new InternalSchemaVersionMismatchException()))
                .doReturn(completedFuture(resultRow))
                .when(internalTable).get(any(), any());

        Tuple result = view.get(null, Tuple.create().set("id", 1L));

        assertThat(result.longValue("val"), is(2L));

        verify(internalTable, times(2)).get(any(), isNull());
    }

    @Test
    void retriesOnInternalSchemaVersionMismatchExceptionRecordView() {
        Random rnd = new Random();

        String pkColumnName = "primitiveLongCol";

        Mapper<TestObjectWithAllTypes> recMapper = Mapper.of(TestObjectWithAllTypes.class);

        Column[] valCols = Arrays.stream(ALL_TYPES_COLUMNS)
                .filter(Predicate.not(col -> pkColumnName.equalsIgnoreCase(col.name())))
                .toArray(Column[]::new);

        SchemaDescriptor schema = new SchemaDescriptor(
                SCHEMA_VERSION,
                new Column[]{new Column(pkColumnName.toUpperCase(), INT64, false)},
                valCols
        );

        InternalTable internalTable = spy(createInternalTable(simpleSchema));

        RecordView<TestObjectWithAllTypes> view = recordView(internalTable, schema, valCols, recMapper);

        TestObjectWithAllTypes expectedRecord = TestObjectWithAllTypes.randomObject(rnd);
        ReflectionMarshallersProvider marshallers = new ReflectionMarshallersProvider();

        BinaryRow resultRow = new RecordMarshallerImpl<>(schema, marshallers, recMapper)
                .marshal(expectedRecord);

        doReturn(failedFuture(new InternalSchemaVersionMismatchException()))
                .doReturn(completedFuture(resultRow))
                .when(internalTable).get(any(), any());

        TestObjectWithAllTypes result = view.get(null, new TestObjectWithAllTypes());

        assertThat(result, is(equalTo(expectedRecord)));

        verify(internalTable, times(2)).get(any(), isNull());
    }

    /**
     * Creates key-value view.
     */
    private KeyValueViewImpl<TestKeyObject, TestObjectWithAllTypes> kvView(
            InternalTable internalTable,
            SchemaDescriptor schema,
            Mapper<TestKeyObject> keyMapper,
            Mapper<TestObjectWithAllTypes> valMapper
    ) {
        ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);
        when(clusterService.topologyService().localMember().address())
                .thenReturn(DummyInternalTableImpl.ADDR);

        when(clusterService.messagingService()).thenReturn(mock(MessagingService.class, RETURNS_DEEP_STUBS));

        // Validate all types are tested.
        Set<ColumnType> testedTypes = Arrays.stream(ALL_TYPES_COLUMNS).map(c -> c.type().spec())
                .collect(Collectors.toSet());

        Set<ColumnType> missedTypes = Arrays.stream(NativeType.nativeTypes())
                .filter(t -> !testedTypes.contains(t)).collect(Collectors.toSet());

        assertEquals(Collections.emptySet(), missedTypes);

        ReflectionMarshallersProvider marshallers = new ReflectionMarshallersProvider();
        return new KeyValueViewImpl<>(
                internalTable,
                new DummySchemaManagerImpl(schema),
                schemaVersions,
                mock(IgniteSql.class),
                marshallers,
                keyMapper,
                valMapper
        );
    }

    /**
     * Creates RecordView.
     */
    private RecordViewImpl<TestObjectWithAllTypes> recordView(
            InternalTable internalTable,
            SchemaDescriptor schema,
            Column[] valuesColumns,
            Mapper<TestObjectWithAllTypes> recMapper
    ) {
        ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);
        when(clusterService.topologyService().localMember().address())
                .thenReturn(DummyInternalTableImpl.ADDR);

        when(clusterService.messagingService()).thenReturn(mock(MessagingService.class, RETURNS_DEEP_STUBS));

        // Validate all types are tested.
        Set<ColumnType> testedTypes = Arrays.stream(valuesColumns)
                .map(c -> c.type().spec())
                .collect(Collectors.toSet());

        Set<ColumnType> missedTypes = Arrays.stream(NativeType.nativeTypes())
                .filter(t -> !testedTypes.contains(t)).collect(Collectors.toSet());

        assertEquals(Collections.emptySet(), missedTypes);

        ReflectionMarshallersProvider marshallers = new ReflectionMarshallersProvider();

        return new RecordViewImpl<>(
                internalTable,
                new DummySchemaManagerImpl(schema),
                schemaVersions,
                mock(IgniteSql.class),
                marshallers,
                recMapper
        );
    }

    private DummyInternalTableImpl createInternalTable(SchemaDescriptor schema) {
        return new DummyInternalTableImpl(replicaService, schema, txConfiguration, systemDistributedConfiguration,
                replicationConfiguration);
    }
}
