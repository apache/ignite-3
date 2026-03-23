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

package org.apache.ignite.internal.client.proto.pojo;

import static org.apache.ignite.internal.compute.PojoConverter.fromTuple;
import static org.apache.ignite.internal.compute.PojoConverter.toTuple;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.ignite.internal.compute.PojoConversionException;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ThrowableNotThrown")
class PojoConverterTest {
    @Test
    void allTypes() {
        Pojo src = Pojo.generateTestPojo(true);
        Tuple tuple = toTuple(src);
        Pojo dst = new Pojo();
        fromTuple(dst, tuple);
        assertThat(dst, is(src));
    }

    /*
     * Nested Pojo is null in this case.
     */
    @Test
    void allTypesWithoutNested() {
        Pojo src = Pojo.generateTestPojo(false);
        Tuple tuple = toTuple(src);
        Pojo dst = new Pojo();
        fromTuple(dst, tuple);
        assertThat(dst, is(src));
    }

    @Test
    void staticField() {
        assertThrows(
                PojoConversionException.class,
                () -> toTuple(new StaticFieldPojo()),
                "Class " + StaticFieldPojo.class.getName() + " doesn't contain any marshallable fields"
        );
    }

    @Test
    void finalFieldPojo() {
        Tuple tuple = toTuple(new FinalFieldPojo());

        assertThrows(
                PojoConversionException.class,
                () -> fromTuple(new FinalFieldPojo(), tuple),
                "Field for the column `longField` is final"
        );
    }

    @SuppressWarnings("PMD.UnusedPrivateField")
    public static class IntPojo {
        private int value;
    }

    @SuppressWarnings("PMD.UnusedPrivateField")
    public static class LongPojo {
        private Long value;
    }

    @Test
    void incompatibleTypes() {
        IntPojo intPojo = new IntPojo();
        intPojo.value = 1;

        Tuple tuple = toTuple(intPojo);

        assertThrows(
                PojoConversionException.class,
                () -> fromTuple(new LongPojo(), tuple),
                "Incompatible types: Field `value` has a type class java.lang.Long while deserializing type class java.lang.Integer"
        );
    }
}
