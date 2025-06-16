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

package org.apache.ignite.internal;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import java.util.List;
import org.apache.ignite.Ignite;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

@ParameterizedClass
@MethodSource("baseVersions")
@Disabled("https://issues.apache.org/jira/browse/IGNITE-25647")
class ItCompatibilityTest extends CompatibilityTestBase {
    @Override
    protected void setupBaseVersion(Ignite baseIgnite) {
        baseIgnite.sql().execute(null, "CREATE TABLE TEST(ID INT PRIMARY KEY, VAL VARCHAR)");
        baseIgnite.sql().execute(null, "INSERT INTO TEST VALUES (1, 'str')");
    }

    @Test
    void testCompatibility() {
        List<List<Object>> result = sql("SELECT * FROM TEST");
        assertThat(result, contains(contains(1, "str")));
    }

    private static List<String> baseVersions() {
        return baseVersions(2);
    }
}
