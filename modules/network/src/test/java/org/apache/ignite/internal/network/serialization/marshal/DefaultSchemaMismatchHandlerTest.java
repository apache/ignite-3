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

package org.apache.ignite.internal.network.serialization.marshal;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.ObjectInput;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

class DefaultSchemaMismatchHandlerTest extends BaseIgniteAbstractTest {
    private final DefaultSchemaMismatchHandler handler = new DefaultSchemaMismatchHandler();

    @Test
    void doesNothingOnFieldIgnored() {
        assertDoesNotThrow(() -> handler.onFieldIgnored(new Object(), "field", "value"));
    }

    @Test
    void doesNothingOnFieldMissed() {
        assertDoesNotThrow(() -> handler.onFieldMissed(new Object(), "field"));
    }

    @Test
    void throwsOnFieldTypeChanged() {
        var ex = assertThrows(SchemaMismatchException.class, () -> handler.onFieldTypeChanged(new Object(), "field", int.class, "value"));
        assertThat(ex.getMessage(), containsString("field type changed, serialized as int, value value of type java.lang.String"));
    }

    @Test
    void throwsOnExternalizableIgnored() {
        var ex = assertThrows(SchemaMismatchException.class,
                () -> handler.onExternalizableIgnored(new Object(), mock(ObjectInput.class))
        );
        assertThat(ex.getMessage(),
                containsString("Class java.lang.Object was serialized as an Externalizable remotely,"
                        + " but locally it is not an Externalizable")
        );
    }

    @Test
    void doesNothingOnExternalizableMissed() {
        assertDoesNotThrow(() -> handler.onExternalizableMissed(new Object()));
    }

    @Test
    void doesNothingOnReadResolveDisappeared() {
        assertDoesNotThrow(() -> handler.onReadResolveDisappeared(new Object()));
    }

    @Test
    void returnsTrueOnReadResolveAppeared() throws Exception {
        assertTrue(handler.onReadResolveAppeared(new Object()));
    }
}
