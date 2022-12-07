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

package org.apache.ignite.internal.sql.engine;

import java.math.BigDecimal;
import java.sql.Date;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ItDynamicParameterTest extends AbstractBasicIntegrationTest {
    @Test
    public void testDynamicParameters() {
        assertQuery("SELECT LOWER(?)").withParams("ASD").returns("asd").check();
        assertQuery("SELECT ?").withParams("asd").returns("asd").check();
        assertQuery("SELECT COALESCE(?, ?)").withParams("a", 10).returns("a").check();

        createAndPopulateTable();
        assertQuery("SELECT name LIKE '%' || ? || '%' FROM person where name is not null").withParams("go")
                .returns(true)
                .returns(false)
                .returns(false)
                .returns(false)
                .check();
    }

    // After fix the mute reason need to merge the test with above testDynamicParameters
    @Disabled("")
    @Test
    public void testDynamicParameters2() {
        assertQuery("SELECT POWER(?, ?)").withParams(2, 3).returns(8).check();
        assertQuery("SELECT SQRT(?)").withParams(4d).returns(2d).check();
        assertQuery("SELECT ? % ?").withParams(11, 10).returns(BigDecimal.valueOf(1)).check();
    }

    // After fix the mute reason need to merge the test with above testDynamicParameters
    @Disabled("")
    @Test
    public void testDynamicParameters3() {
        assertQuery("SELECT LAST_DAY(?)").withParams(Date.valueOf("2022-01-01"))
                .returns(Date.valueOf("2022-01-31")).check();
    }
}
