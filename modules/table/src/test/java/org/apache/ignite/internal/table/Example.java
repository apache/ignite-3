/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjects;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.storage.basic.ConcurrentHashMapPartitionStorage;
import org.apache.ignite.internal.table.distributed.storage.VersionedRowStore;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.mapper.TypeConverter;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Example.
 */
@SuppressWarnings({"PMD.EmptyLineSeparatorCheck", "emptylineseparator", "unused", "UnusedAssignment", "InstanceVariableMayNotBeInitialized",
        "JoinDeclarationAndAssignmentJava"})
public class Example {
    /**
     * Returns table implementation.
     */
    private static List<Table> tableFactory() {
        TxManagerImpl txManager = new TxManagerImpl(null, new HeapLockManager());

        return Collections.singletonList(new TableImpl(new DummyInternalTableImpl(new VersionedRowStore(
                new ConcurrentHashMapPartitionStorage(), txManager), txManager), null, null));
    }

    /**
     * Use case 1: a simple one. The table has the structure [ [id int, orgId int] // key [name varchar, lastName varchar, decimal salary,
     * int department] // value ] We show how to use the raw TableRow and a mapped class.
     */
    @Disabled
    @ParameterizedTest
    @MethodSource("tableFactory")
    public void useCase1(Table t) {
        // Search row will allow nulls even in non-null columns.
        Tuple res = t.recordView().get(Tuple.create().set("id", 1).set("orgId", 1));

        String name = res.value("name");
        String lastName = res.value("latName");
        BigDecimal salary = res.value("salary");
        Integer department = res.value("department");

        // We may have primitive-returning methods if needed.
        int departmentPrimitive = res.intValue("department");

        // Note that schema itself already defined which fields are key field.
        class Employee {
            final int id;
            final int orgId;

            String name;
            String lastName;
            BigDecimal salary;
            int department;

            Employee(int id, int orgId) {
                this.id = id;
                this.orgId = orgId;
            }
        }

        RecordView<Employee> employeeView = t.recordView(Employee.class);

        Employee e = employeeView.get(new Employee(1, 1));

        // As described in the IEP-54, we can have a truncated mapping.
        class TruncatedEmployee {
            final int id;
            final int orgId;

            String name;
            String lastName;

            TruncatedEmployee(int id, int orgId) {
                this.id = id;
                this.orgId = orgId;
            }
        }

        RecordView<TruncatedEmployee> truncatedEmployeeView = t.recordView(TruncatedEmployee.class);

        // salary and department will not be sent over the network during this call.
        TruncatedEmployee te = truncatedEmployeeView.get(new TruncatedEmployee(1, 1));
    }

    /**
     * Use case 2: using simple KV mappings The table has structure is [ [id int, orgId int] // key [name varchar, lastName varchar, decimal
     * salary, int department] // value ].
     */
    @Disabled
    @ParameterizedTest
    @MethodSource("tableFactory")
    public void useCase2(Table t) {
        class EmployeeKey {
            final int id;
            final int orgId;

            EmployeeKey(int id, int orgId) {
                this.id = id;
                this.orgId = orgId;
            }
        }

        class Employee {
            String name;
            String lastName;
            BigDecimal salary;
            int department;
        }

        KeyValueView<EmployeeKey, Employee> employeeKv = t.keyValueView(EmployeeKey.class, Employee.class);

        employeeKv.get(new EmployeeKey(1, 1));

        // As described in the IEP-54, we can have a truncated KV mapping.
        class TruncatedEmployee {
            String name;
            String lastName;
        }

        KeyValueView<EmployeeKey, TruncatedEmployee> truncatedEmployeeKv = t.keyValueView(EmployeeKey.class, TruncatedEmployee.class);

        TruncatedEmployee te = truncatedEmployeeKv.get(new EmployeeKey(1, 1));
    }

    /**
     * Use case 3: Single table strategy for inherited objects. The table has structure is [ [id long] // key [owner varchar, cardNumber
     * long, expYear int, expMonth int, accountNum long, bankName varchar] // value ]
     */
    @Disabled
    @ParameterizedTest
    @MethodSource("tableFactory")
    public void useCase3(Table t) {
        class BillingDetails {
            String owner;
        }

        class CreditCard extends BillingDetails {
            long cardNumber;
            int expYear;
            int expMonth;
        }

        class BankAccount extends BillingDetails {
            long account;
            String bankName;
        }

        KeyValueView<Long, CreditCard> credCardKvView = t.keyValueView(Long.class, CreditCard.class);
        CreditCard creditCard = credCardKvView.get(1L);

        KeyValueView<Long, BankAccount> backAccKvView = t.keyValueView(Long.class, BankAccount.class);
        BankAccount bankAccount = backAccKvView.get(2L);

        // Truncated view.
        KeyValueView<Long, BillingDetails> billingDetailsKvView = t.keyValueView(Long.class, BillingDetails.class);
        BillingDetails billingDetails = billingDetailsKvView.get(2L);

        // Without discriminator it is impossible to deserialize to correct type automatically.
        assert !(billingDetails instanceof CreditCard);
        assert !(billingDetails instanceof BankAccount);

        // Wide record.
        class BillingRecord {
            final long id;

            String owner;

            long cardNumber;
            int expYear;
            int expMonth;

            long account;
            String bankName;

            BillingRecord(long id) {
                this.id = id;
            }
        }

        final RecordView<BillingRecord> billingView = t.recordView(BillingRecord.class);

        final BillingRecord br = billingView.get(new BillingRecord(1));
    }

    /**
     * Use case 4: Conditional serialization. The table has structure is [ [id int, orgId int] // key [owner varchar, type int,
     * conditionalDetails byte[]] // value ]
     */
    @Disabled
    @ParameterizedTest
    @MethodSource("tableFactory")
    public void useCase4(Table t) {
        class OrderKey {
            final int id;
            final int orgId;

            OrderKey(int id, int orgId) {
                this.id = id;
                this.orgId = orgId;
            }
        }

        class OrderValue {
            String owner;
            int type; // Discriminator value.
            /* BillingDetails */ Object billingDetails;
        }

        class CreditCard /* extends BillingDetails */ {
            long cardNumber;
            int expYear;
            int expMonth;
        }

        class BankAccount /* extends BillingDetails */ {
            long account;
            String bankName;
        }

        KeyValueView<OrderKey, OrderValue> orderKvView = t
                .keyValueView(Mapper.of(OrderKey.class, "key"), Mapper.builder(OrderValue.class).map("billingDetails", (row) -> {
                    BinaryObject binObj = row.binaryObjectValue("conditionalDetails");
                    int type = row.intValue("type");

                    return type == 0
                            ? BinaryObjects.deserialize(binObj, CreditCard.class)
                            : BinaryObjects.deserialize(binObj, BankAccount.class);
                }).build());

        OrderValue ov = orderKvView.get(new OrderKey(1, 1));

        // Same with direct Row access and BinaryObject wrapper.
        Tuple res = t.recordView().get(Tuple.create().set("id", 1).set("orgId", 1));

        byte[] objData = res.value("billingDetails");
        BinaryObject binObj = BinaryObjects.wrap(objData);
        // Work with the binary object as in Ignite 2.x

        // Additionally, we may have a shortcut similar to primitive methods.
        binObj = res.binaryObjectValue("billingDetails");

        // Same with RecordAPI.
        class OrderRecord {
            final int id;
            final int orgId;

            String owner;
            int type;
            BinaryObject billingDetails;

            OrderRecord(int id, int orgId) {
                this.id = id;
                this.orgId = orgId;
            }
        }

        final RecordView<OrderRecord> orderRecView = t.recordView(OrderRecord.class);

        OrderRecord orderRecord = orderRecView.get(new OrderRecord(1, 1));
        binObj = orderRecord.billingDetails;

        // Manual deserialization is possible as well.
        Object billingDetails = orderRecord.type == 0
                ? BinaryObjects.deserialize(binObj, CreditCard.class)
                : BinaryObjects.deserialize(binObj, BankAccount.class);
    }

    /**
     * Use case 5: using byte[] and binary objects in columns. The table has structure [ [id int, orgId int] // key [originalObject byte[],
     * upgradedObject byte[], int department] // value ] Where {@code originalObject} is some value that was originally put to the column,
     * {@code upgradedObject} is a version 2 of the object, and department is an extracted field.
     */
    @Disabled
    @ParameterizedTest
    @MethodSource("tableFactory")
    public void useCase5(Table t) {
        Tuple res = t.recordView().get(Tuple.create().set("id", 1).set("orgId", 1));

        byte[] objData = res.value("originalObject");
        BinaryObject binObj = BinaryObjects.wrap(objData);
        // Work with the binary object as in Ignite 2.x

        // Additionally, we may have a shortcut similar to primitive methods.
        binObj = res.binaryObjectValue("upgradedObject");

        // Plain byte[] and BinaryObject fields in a class are straightforward.
        class Record {
            final int id;
            final int orgId;

            byte[] originalObject;
            Tuple upgradedObject;
            int department;

            Record(int id, int orgId) {
                this.id = id;
                this.orgId = orgId;
            }
        }

        RecordView<Record> recordView = t.recordView(Record.class);

        // Similarly work with the binary objects.
        Record rec = recordView.get(new Record(1, 1));

        // Now assume that we have some POJO classes to deserialize the binary objects.
        class JavaPerson {
            String name;
            String lastName;
        }

        class JavaPersonV2 extends JavaPerson {
            int department;
        }

        // We can have a compound record deserializing the whole tuple automatically.
        class JavaPersonRecord {
            JavaPerson originalObject;
            JavaPersonV2 upgradedObject;
            int department;
        }

        RecordView<JavaPersonRecord> personRecordView = t.recordView(JavaPersonRecord.class);

        // Or we can have an arbitrary record with custom class selection.
        class TruncatedRecord {
            JavaPerson upgradedObject;
            int department;
        }

        // Custom serializer.
        TypeConverter<JavaPerson, byte[]> serializer = new TypeConverter<>() {
            @Override
            public byte[] toColumnType(JavaPerson obj) throws Exception {
                return BinaryObjects.serialize(obj).bytes();
            }

            @Override
            public JavaPerson toObjectType(byte[] data) throws Exception {
                return BinaryObjects.deserialize(BinaryObjects.wrap(data), JavaPerson.class);
            }
        };

        RecordView<TruncatedRecord> truncatedView = t.recordView(
                Mapper.builder(TruncatedRecord.class)
                        .convert(serializer, "upgradedObject")
                        .map("updradedObject", "updradedObject")
                        .build());

        // Or we can have a custom conditional type selection.
        RecordView<TruncatedRecord> truncatedView2 = t.recordView(
                Mapper.builder(TruncatedRecord.class)
                        .map("upgradedObject", (row) -> {
                            BinaryObject binObj1 = row.binaryObjectValue("upgradedObject");
                            int dept = row.intValue("department");

                            return dept == 0 ? BinaryObjects.deserialize(binObj1, JavaPerson.class)
                                    : BinaryObjects.deserialize(binObj1, JavaPersonV2.class);
                        })
                        // TODO: But how to write the columns ??? There is no separate "write mapping" yet.
                        //                        .map("person", "colPersol", obj -> BinaryObjects.serialize(obj))
                        //                        .map("department", "colDepartment", obj -> obj instanceof JavaPerson ? 0 : 1)
                        .build());

    }

    /**
     * Use case 6: a simple one. The table has the structure [ [id long] // key [name varchar, lastName varchar, decimal salary, int
     * department] // value ] We show how to use the raw TableRow and a mapped class.
     */
    @Disabled
    @ParameterizedTest
    @MethodSource("tableFactory")
    public void useCase6(Table t) {
        // Search row will allow nulls even in non-null columns.
        Tuple res = t.recordView().get(Tuple.create().set("id", 1));

        String name = res.value("name");
        String lastName = res.value("latName");
        BigDecimal salary = res.value("salary");
        Integer department = res.value("department");

        // We may have primitive-returning methods if needed.
        int departmentPrimitive = res.intValue("department");

        // Note that schema itself already defined which fields are key field.
        class Employee {
            String name;
            String lastName;
            BigDecimal salary;
            int department;
        }

        class Key {
            long id;
        }

        KeyValueView<Long, Employee> employeeView = t.keyValueView(Long.class, Employee.class);

        Employee e = employeeView.get(1L);
    }

    /**
     * Use case 7: a simple one. The table has the structure [ [byte[]] // key [name varchar, lastName varchar, decimal salary, int
     * department] // value ] We show how to use the raw TableRow and a mapped class.
     */
    @Disabled
    @ParameterizedTest
    @MethodSource("tableFactory")
    public void useCase7(Table t) {
        // Note that schema itself already defined which fields are key field.
        class Employee {
            String name;
            String lastName;
            BigDecimal salary;
            int department;
        }

        KeyValueView<Long, BinaryObject> employeeView = t.keyValueView(Long.class, BinaryObject.class);

        employeeView.put(1L, BinaryObjects.wrap(new byte[0] /* serialized Employee */));

        t.keyValueView(Mapper.of(Long.class), Mapper.of(Employee.class, "value"));
    }

    /**
     * Use case 8: Here we show how to use mapper to represent the same data in different ways.
     */
    @Disabled
    @ParameterizedTest
    @MethodSource("tableFactory")
    public void useCase8(Table t) {
        new SchemaDescriptor(
                1,
                new Column[]{new Column("colId", NativeTypes.INT64, false)},
                new Column[]{new Column("colData", NativeTypes.BYTES, true)}
        );

        // Arbitrary user type.
        class UserObject {
        }

        // Domain class, which fields mapped to table columns.
        class Employee {
            UserObject fieldData;
        }

        // Domain class, which fields mapped to table columns.
        class Employee2 {
            byte[] fieldData;
        }

        // Actually, any bi-directional converter can be here instead.
        // Marshaller is a special case of "UserObject <--> byte[]" converter, just for example.
        TypeConverter<UserObject, byte[]> marsh = null; // here, create some marshaller for UserObject.class.

        // One-column keys only and only supported first-citizen types.
        Mapper.of(Long.class);
        Mapper.of(byte[].class);
        Mapper.of(UserObject.class); // Implicitly serialized to byte[].

        // LongMapper -> long - is it possible?

        // Shortcut (supported one-column key and value).
        Mapper.of(Long.class, "colId");

        // Shortcut (key and value represented by byte array)
        Mapper.of(UserObject.class, "colData"); // Does one-column record make sense ??? either one-column table ???

        // Keys, Values, and Records
        Mapper.builder(Employee.class)
                .map("fieldData", "colData")
                .map("fieldData2", "colData1")
                .build();

        Mapper.builder(Employee.class).map(
                "fieldData", "colData",
                "fieldData2", "colData1"
        ).build();

        // Shortcuts (supported keys and values and records).
        Mapper.of(Employee.class, "fieldData", "colData");
        Mapper.of(Employee.class, "fieldData", "colData", "fieldData1", "colData1");

        // Shortcut (supported one-column key and value) with additional transformation.
        Mapper.of(UserObject.class, "data", marsh);

        //  (supported one-column key and value and records) with additional transformation.
        Mapper.builder(Employee.class)
                .convert(marsh, "colData") //TODO: Will it be useful to set a bunch of columns that will use same converter (serializer) ???
                .map("fieldData", "colData")
                .build();
        // OR another way to do the same
        Mapper.builder(Employee.class)
                .convert(marsh, "fieldData", "colData")
                .build();

        // TODO: Does "marsh" is mandatory if field is a POJO and column is byte[] ??? Do we need any implicit serialization out-of-box ???

        // Next views shows different approaches to map user objects to columns.
        KeyValueView<Long, Employee> v1 = t.keyValueView(
                Mapper.of(Long.class),
                Mapper.of(Employee.class, "fieldData","colData"));

        KeyValueView<Long, Employee2> v2 = t.keyValueView(
                Mapper.of(Long.class),
                Mapper.builder(Employee2.class)
                        .convert(marsh, "colData")
                        .map("fieldData", "colData").build());

        KeyValueView<Long, Employee2> v3 = t.keyValueView(
                Mapper.of(Long.class, "colId"),
                Mapper.builder(Employee2.class).convert(marsh, "fieldData", "colData").build());

        KeyValueView<Long, UserObject> v4 = t.keyValueView(
                Mapper.of(Long.class, "colId"),
                Mapper.of(UserObject.class, "colData", marsh));

        KeyValueView<Long, byte[]> v5 = t.keyValueView(
                Mapper.of(Long.class, "colId"),
                Mapper.of(byte[].class, "colData"));

        // The values in next operations are equivalent, and lead to the same row value part content.
        v1.put(1L, new Employee());
        v2.put(2L, new Employee2());
        v3.put(3L, new Employee2());
        v4.put(4L, new UserObject());
        v5.put(5L, new byte[]{/* serialized UserObject bytes */});

        // Get operations return the same result for all keys for each of row.
        // for 1 in 1..5
        //      v1.get(iL) == v1.get(1L);

        // ============================  GET  ===============================================

        UserObject obj = v4.get(1L); // indistinguishable absent value and null column

        // Optional way
        //        Optional<UserObject> optionalObj = v4.get(1L); // abuse of Optional type

        // NullableValue way
        //        NullableValue<UserObject> nullableValue = v4.getNullable(1L);

        UserObject userObject = v4.get(1L); // what if user uses this syntax for nullable column?
        // 1. Exception always
        // 2. Exception if column value is null (use getNullable)

        // ============================  PUT  ===============================================

        v4.put(1L, null);
        v4.remove(1L, null);
    }

    interface NullableValue<T> {
        T get();
    }


    /**
     * Fully manual mapping case.
     * Allows users to write powerful functions that will convert an object to a row and vice versa.
     *
     * For now, it is the only case where conditional mapping (condition on another field) is possible.
     * This case widely used in ORM (e.g. Hibernate) to store inherited objects in same table using a condition on special-purpose "discriminator" column.
     *
     * TODO: Maybe we can produce design with other approaches ??? Do we want this feature ???
     *
     * @param t Table.
     */
    public void useCase9(Table t) {
        // Now assume that we have some POJO classes to deserialize the binary objects.
        class Emploee {
            String name;
            String lastName;
        }

        // Here we
        class EmploeeV2 extends Emploee {
            int department;
        }

        // Or we can have an arbitrary record with custom class selection.
        class UserRecord {
            Emploee person;
            int department; // Discriminator column.
        }

        RecordView<UserRecord> truncatedView2 = t.recordView(
                Mapper.builder(UserRecord.class)
                        // Next two functions of compatible interfaces parametrized with compatible generic types.
                        // TODO: Do we want to expose such powerfull and potetially harmful feature ??? or let's try another approach ???
                        // TODO: Do we require top-level class with a certain interface here ???
                        .map(
                                (obj) -> obj == null
                                        ? null
                                        : Tuple.create(Map.of(
                                                "colPerson", BinaryObjects.serialize(obj),
                                                "colDepartment", (obj.person instanceof EmploeeV2) ? 1 : 0
                                        )),

                                (row) -> {
                                    if (row == null) {
                                        return null;
                                    }

                                    UserRecord rec = new UserRecord();
                                    int dep = row.intValue("colDepartment");

                                    rec.department = dep;
                                    rec.person = dep == 0 ? BinaryObjects.deserialize(row.binaryObjectValue("colPerson"), Emploee.class)
                                            : BinaryObjects.deserialize(row.binaryObjectValue("colPerson"), EmploeeV2.class);

                                    return rec;
                                })
                        .build());

    }

}
