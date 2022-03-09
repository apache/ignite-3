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

package org.apache.ignite.internal.sql.engine;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Test;

/**
 * Integration test for set op (EXCEPT, INTERSECT).
 */
public class ItCreateTableDdlTest extends AbstractBasicIntegrationTest {
    @Test
    public void pkWithNullableColumns() {
        assertThrows(
                IgniteException.class,
                () -> sql("CREATE TABLE T0(ID0 INT NULL, ID1 INT NOT NULL, VAL INT, PRIMARY KEY (ID1, ID0))"),
                "Primary key cannot contain nullable column [col=ID0]"
        );
        assertThrows(
                IgniteException.class,
                () -> sql("CREATE TABLE T0(ID INT NULL PRIMARY KEY, VAL INT)"),
                "Primary key cannot contain nullable column [col=ID]"
        );
    }

    @Test
    public void undefinedColumnsInPrimaryKey() {
        assertThrows(
                IgniteException.class,
                () -> sql("CREATE TABLE T0(ID INT, VAL INT, PRIMARY KEY (ID1, ID0, ID2))"),
                " Primary key constrain contains undefined columns: [cols=[ID0, ID2, ID1]]"
        );
    }

    @Test
    public void dbg() {
        sql("CREATE TABLE T0(ID INT, VAL INT, PRIMARY KEY (ID))");
    }

}
