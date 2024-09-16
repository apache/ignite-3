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

package org.apache.ignite.table;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.junit.jupiter.api.Test;

/**
 * Tests to validate {@link TupleHelper}.
 */
class TupleHelperTest extends IgniteAbstractTest {

    @Test
    void valueOrDefault() {
        String columnName = "col_1";
        String defaultValue = "default";

        {
            TupleImpl tuple = mock(TupleImpl.class);

            TupleHelper.valueOrDefault(tuple, columnName, defaultValue);

            // in case of TupleImpl optimized method avoiding normalization must be invoked
            verify(tuple).valueOrDefaultSkipNormalization(eq(columnName), eq(defaultValue));
            verifyNoMoreInteractions(tuple);
        }

        {
            Tuple tuple = mock(Tuple.class);

            TupleHelper.valueOrDefault(tuple, columnName, defaultValue);

            // in case of other implementations regular method must be invoked, but column name must be quoted
            verify(tuple).valueOrDefault(eq(IgniteNameUtils.quote(columnName)), eq(defaultValue));
            verifyNoMoreInteractions(tuple);
        }
    }
}
