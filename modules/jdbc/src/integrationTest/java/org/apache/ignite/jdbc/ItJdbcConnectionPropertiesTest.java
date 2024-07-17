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

package org.apache.ignite.jdbc;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.DriverPropertyInfo;
import org.apache.ignite.internal.jdbc.ConnectionPropertiesImpl;
import org.junit.jupiter.api.Test;

/**
 * {@link ConnectionPropertiesImpl} unit tests.
 */
public class ItJdbcConnectionPropertiesTest {
    /**
     * Test check the {@link ConnectionPropertiesImpl#getDriverPropertyInfo()} return properties with prefix {@link
     * ConnectionPropertiesImpl#PROP_PREFIX}.
     */
    @Test
    public void testNamePrefixDriverPropertyInfo() {
        ConnectionPropertiesImpl connProps = new ConnectionPropertiesImpl();
        DriverPropertyInfo[] propsInfo = connProps.getDriverPropertyInfo();

        assertNotEquals(0, propsInfo.length);

        for (DriverPropertyInfo info : propsInfo) {
            assertTrue(info.name.startsWith(ConnectionPropertiesImpl.PROP_PREFIX));
        }
    }
}
