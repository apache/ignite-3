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

package org.apache.ignite.storage;

import java.math.BigDecimal;
import org.apache.ignite.storage.binary.BinaryObject;
import org.apache.ignite.storage.binary.BinaryObjects;
import org.apache.ignite.storage.mapper.Mappers;

/**
 *
 */
@SuppressWarnings({"unused", "UnusedAssignment"})
public class Example {
    /**
     * Use case 1: a simple one. The table has the structure
     * [
     * [id int, orgId int] // key
     * [name varchar, lastName varchar, decimal salary, int department] // value
     * ]
     * We show how to use the raw TableRow and a mapped class.
     */
    public void useCase1(Table t) {
        // Search row will allow nulls even in non-null columns.
        Row res = t.get(t.createSearchRow(1, 1));

        String name = res.field("name");
        String lastName = res.field("latName");
        BigDecimal salary = res.field("salary");
        Integer department = res.field("department");

        // We may have primitive-returning methods if needed.
        int departmentPrimitive = res.intField("department");

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

        TableView<Employee> employeeView = t.tableView(Employee.class);

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

        TableView<TruncatedEmployee> truncatedEmployeeView = t.tableView(TruncatedEmployee.class);

        // salary and department will not be sent over the network during this call.
        TruncatedEmployee te = truncatedEmployeeView.get(new TruncatedEmployee(1, 1));
    }

    /**
     * Use case 2: using simple KV mappings
     * The table has structure is
     * [
     * [id int, orgId int] // key
     * [name varchar, lastName varchar, decimal salary, int department] // value
     * ]
     */
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

        KVView<EmployeeKey, Employee> employeeKv = t.kvView(EmployeeKey.class, Employee.class);

        employeeKv.get(new EmployeeKey(1, 1));

        // As described in the IEP-54, we can have a truncated KV mapping.
        class TruncatedEmployee {
            String name;
            String lastName;
        }

        KVView<EmployeeKey, TruncatedEmployee> truncatedEmployeeKv = t.kvView(EmployeeKey.class, TruncatedEmployee.class);

        TruncatedEmployee te = truncatedEmployeeKv.get(new EmployeeKey(1, 1));
    }

    /**
     * Use case 3: Single table strategy for inherited objects.
     * The table has structure is
     * [
     * [id long] // key
     * [owner varchar, cardNumber long, expYear int, expMonth int, accountNum long, bankName varchar] // value
     * ]
     */
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

        KVView<Long, CreditCard> credCardKvView = t.kvView(Long.class, CreditCard.class);
        CreditCard creditCard = credCardKvView.get(1L);

        KVView<Long, BankAccount> backAccKvView = t.kvView(Long.class, BankAccount.class);
        BankAccount bankAccount = backAccKvView.get(2L);

        // Truncated view.
        KVView<Long, BillingDetails> billingDetailsKVView = t.kvView(Long.class, BillingDetails.class);
        BillingDetails billingDetails = billingDetailsKVView.get(2L);

        // Without discriminator it is impossible to deserialize to correct type automatically.
        assert !(billingDetails instanceof CreditCard);
        assert !(billingDetails instanceof BankAccount);
    }

    /**
     * Use case 4: Conditional serialization.
     * The table has structure is
     * [
     * [id int, orgId int] // key
     * [owner varchar, type int, conditionalDetails byte[]] // value
     * ]
     */
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

        class CreditCard /* extends BillingDetails */{
            long cardNumber;
            int expYear;
            int expMonth;
        }

        class BankAccount /* extends BillingDetails */ {
            long account;
            String bankName;
        }

        KVView<OrderKey, OrderValue> orderKvView = t.kvView(Mappers.ofKeyClass(OrderKey.class),
            Mappers.ofValueClassBuilder(OrderValue.class)
                .map("billingDetails", (row) -> {
                    BinaryObject bObj = row.binaryObjectField("conditionalDetails");
                    int type = row.intField("type");

                    return bObj.deserialize(type == 0 ? CreditCard.class : BankAccount.class);
                }).build());

        OrderValue ov = orderKvView.get(new OrderKey(1, 1));

        // Same with direct Row access and BinaryObject wrapper.
        Row res = t.get(t.createSearchRow(1, 1));

        byte[] objData = res.field("billingDetails");
        BinaryObject binObj = BinaryObjects.wrap(objData);
        // Work with the binary object as in Ignite 2.x

        // Additionally, we may have a shortcut similar to primitive methods.
        binObj = res.binaryObjectField("billingDetails");

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

        final TableView<OrderRecord> orderRecView = t.tableView(Mappers.ofRowClass(OrderRecord.class));

        OrderRecord orderRecord = orderRecView.get(new OrderRecord(1, 1));
        binObj = orderRecord.billingDetails;

        // Manual deserialization is possible as well.
        Object billingDetails = orderRecord.type == 0 ?
            binObj.deserialize(CreditCard.class) :
            binObj.deserialize(BankAccount.class);
    }

    /**
     * Use case 5: using byte[] and binary objects in columns.
     * The table has structure
     * [
     * [id int, orgId int] // key
     * [originalObject byte[], upgradedObject byte[], int department] // value
     * ]
     * Where {@code originalObject} is some value that was originally put to the column,
     * {@code upgradedObject} is a version 2 of the object, and department is extracted field.
     */
    public void useCase5(Table t) {
        Row res = t.get(t.createSearchRow(1, 1));

        byte[] objData = res.field("originalObject");
        BinaryObject binObj = BinaryObjects.wrap(objData);
        // Work with the binary object as in Ignite 2.x

        // Additionally, we may have a shortcut similar to primitive methods.
        binObj = res.binaryObjectField("upgradedObject");

        // Plain byte[] and BinaryObject fields in a class are straightforward.
        class Record {
            final int id;
            final int orgId;

            byte[] originalObject;
            BinaryObject upgradedObject;
            int department;

            Record(int id, int orgId) {
                this.id = id;
                this.orgId = orgId;
            }
        }

        TableView<Record> recordView = t.tableView(Mappers.ofRowClass(Record.class));

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

        TableView<JavaPersonRecord> personRecordView = t.tableView(Mappers.ofRowClass(JavaPersonRecord.class));

        // Or we can have an arbitrary record with custom class selection.
        class TruncatedRecord {
            JavaPerson upgradedObject;
            int department;
        }

        TableView<TruncatedRecord> truncatedView = t.tableView(
            Mappers.ofRowClassBuilder(TruncatedRecord.class)
                .deserializing("upgradedObject", JavaPersonV2.class).build());

        // Or we can have a custom conditional type selection.
        TableView<TruncatedRecord> truncatedView2 = t.tableView(
            Mappers.ofRowClassBuilder(TruncatedRecord.class)
                .map("upgradedObject", (row) -> {
                    BinaryObject bObj = row.binaryObjectField("upgradedObject");
                    int dept = row.intField("department");

                    return dept == 0 ? bObj.deserialize(JavaPerson.class) : bObj.deserialize(JavaPersonV2.class);
                }).build());
    }
}


