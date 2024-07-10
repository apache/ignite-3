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

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.IncompatibleSchemaException;
import org.apache.ignite.internal.lang.IgniteExceptionMapper;
import org.apache.ignite.internal.table.distributed.replicator.IncompatibleSchemaVersionException;
import org.junit.jupiter.api.Test;

class TableExceptionMapperProviderTest {
    private final TableExceptionMapperProvider provider = new TableExceptionMapperProvider();

    @Test
    void translatesIncompatibleSchemaVersionException() {
        Collection<IgniteExceptionMapper<?, ?>> mappers = provider.mappers();

        List<IgniteExceptionMapper<?, ?>> matchingMappers = mappers.stream()
                .filter(mapper -> IncompatibleSchemaVersionException.class.equals(mapper.mappingFrom()))
                .collect(toList());

        assertThat(matchingMappers, hasSize(1));

        var mapper = (IgniteExceptionMapper<IncompatibleSchemaVersionException, IncompatibleSchemaException>) matchingMappers.get(0);

        IncompatibleSchemaVersionException originalException = new IncompatibleSchemaVersionException("Oops");
        IncompatibleSchemaException translationResult = mapper.map(originalException);

        assertThat(translationResult.traceId(), is(originalException.traceId()));
        assertThat(translationResult.code(), is(originalException.code()));
        assertThat(translationResult.getMessage(), is(originalException.getMessage()));
    }
}
