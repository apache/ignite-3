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

/** Check correctness of query validations. */
package org.apache.ignite.internal.sql.engine.sql;

public class IgniteSqlValidatorTest {
    
    /*@Test
    public void testMergeKeysConflict() {
        sql("DROP TABLE IF EXISTS test1 ");
        sql("DROP TABLE IF EXISTS test2 ");
        sql("CREATE TABLE test1 (k1 int, k2 int, a int, b int, CONSTRAINT PK PRIMARY KEY (k1, k2))");
        sql("CREATE TABLE test2 (k1 int, k2 int, a int, b int, CONSTRAINT PK PRIMARY KEY (k1, k2))");

        IgniteInternalException exi = assertThrows(IgniteInternalException.class, () -> sql(
                "MERGE INTO test2 USING test1 ON test1.a = test2.a "
                        + "WHEN MATCHED THEN UPDATE SET b = test1.b + 1 "
                        + "WHEN NOT MATCHED THEN INSERT (k1, a, b) VALUES (0, a, b)"));

        assertTrue(exi.getCause().getMessage().contains("Attempt to update primary keys partially."));
    }*/
}
