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

package org.apache.ignite.internal.sql.engine.datatypes.uuid;

import java.util.UUID;

import org.apache.ignite.internal.sql.engine.datatypes.CustomDataTypeTestSpecs;
import org.apache.ignite.internal.sql.engine.datatypes.tests.BaseIndexCustomDataTypeTest;
import org.apache.ignite.internal.sql.engine.datatypes.tests.CustomDataTypeTestSpec;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.junit.jupiter.api.Disabled;

/**
 * Tests for queries that use indexes with {@link UuidType UUID data type}.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-19128")
public class ItUuidIndexTest extends BaseIndexCustomDataTypeTest<UUID> {

    @Override
    protected CustomDataTypeTestSpec<UUID> getTypeSpec() {
        return CustomDataTypeTestSpecs.UUID_TYPE;
    }

}
