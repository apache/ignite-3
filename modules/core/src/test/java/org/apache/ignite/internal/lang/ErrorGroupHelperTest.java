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

package org.apache.ignite.internal.lang;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.lang.ErrorGroup;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.lang.ErrorGroups.Client;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.ErrorGroups.MetaStorage;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.ErrorGroups.Table;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ErrorGroupHelperTest {
    /**
     * We must have identical bahavior for two methods {@link ErrorGroupHelper#errorMessage(String, UUID, String, int, String)} and
     * {@link ErrorGroup#errorMessage(String, UUID, String, int, String)}. The second method has privite access so we can check it
     * transitively.
     *
     * @param fullErrorCode Full error code to test error messages.
     */
    @ParameterizedTest
    @MethodSource("someErrorsCodes")
    void errorMessageIsIdenticalToMethodInApiModule(int fullErrorCode) {
        UUID uuid = UUID.randomUUID();
        String message = IgniteTestUtils.randomString(ThreadLocalRandom.current(), 10);
        ErrorGroup errorGroup = ErrorGroups.errorGroupByCode(fullErrorCode);

        String errMessageFromErrorGroup = ErrorGroup.errorMessage(uuid, fullErrorCode, message);
        String errMessageFromErrorGroupHelper =
                ErrorGroupHelper.errorMessage(errorGroup.errorPrefix(), uuid, errorGroup.name(), fullErrorCode, message);

        assertEquals(errMessageFromErrorGroupHelper, errMessageFromErrorGroup);
    }

    private static Stream<Arguments> someErrorsCodes() {
        return Stream.of(
                Common.INTERNAL_ERR, Common.ILLEGAL_ARGUMENT_ERR, Common.NULLABLE_VALUE_ERR,
                Table.SCHEMA_VERSION_MISMATCH_ERR, Table.UNSUPPORTED_PARTITION_TYPE_ERR,
                Client.CONNECTION_ERR, Client.CLUSTER_ID_MISMATCH_ERR, Client.PROTOCOL_ERR,
                Sql.MAPPING_ERR, Sql.RUNTIME_ERR,
                MetaStorage.COMPACTED_ERR, MetaStorage.DIVERGED_ERR, MetaStorage.OP_EXECUTION_TIMEOUT_ERR).map(Arguments::of);
    }
}
