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

package org.apache.ignite.internal.compute;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import java.util.Collection;
import java.util.stream.Stream;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.network.ClusterService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit tests for compute implementation's bare execution of compute tasks.
 */
class IgniteComputeImplBareExecutionTest {
    /** Compute instance. */
    private final IgniteComputeImpl compute = new IgniteComputeImpl(mock(ClusterService.class));

    /**
     * Tests different compute tasks that should be able to be executed by design.
     *
     * @param clazz Compute class.
     */
    @ParameterizedTest
    @MethodSource("computeTasks")
    void testComputes(Class<? extends ComputeTask<?, ?, ?>> clazz) {
        Object res = compute.execute0(clazz.getName(), 1);

        assertInstanceOf(Integer.class, res);

        assertEquals(2, res);
    }

    private interface SimpleTask<T, R> extends ComputeTask<T, R, Void> {
        @Override
        default Void reduce(Collection<R> results) {
            return null;
        }
    }

    private static class SimpleCompute implements SimpleTask<Integer, Integer> {
        @Override
        public Integer execute(Integer argument) {
            return argument + 1;
        }
    }

    private static class ComputeInexactParam implements SimpleTask<Number, Integer> {
        @Override
        public Integer execute(Number argument) {
            return argument.intValue() + 1;
        }
    }

    private static class ComputeInexactReturn implements SimpleTask<Integer, Number> {
        @Override
        public Number execute(Integer argument) {
            return argument + 1;
        }
    }

    private static class ComputeInexactParamAndReturn implements SimpleTask<Number, Number> {
        @Override
        public Number execute(Number argument) {
            return argument.intValue() + 1;
        }
    }

    @SuppressWarnings("rawtypes")
    private static class ComputeRaw implements SimpleTask {
        @Override
        public Object execute(Object argument) {
            Integer intArg = (Integer) argument;
            return intArg + 1;
        }
    }

    private static class ComputeSuperClass implements SimpleTask<Integer, Integer> {
        @Override
        public Integer execute(Integer argument) {
            return argument + 1;
        }
    }

    private static class ComputeInheritor extends ComputeSuperClass {

    }

    private static class ComputeSuperClassWithBounds<A extends Number, B extends Number> implements SimpleTask<A, B> {
        @Override
        public B execute(A argument) {
            Integer i = argument.intValue() + 1;
            return (B) i;
        }
    }

    private static class ComputeBoundedInheritor extends ComputeSuperClassWithBounds<Integer, Integer> {
    }

    private static class ComputeUnboundedInheritor extends ComputeSuperClassWithBounds {
    }

    private abstract static class ComputeAbstract<X extends Number, Y extends Number> extends ComputeSuperClassWithBounds<X, Y> {
    }

    private static class ComputeAbstractBoundedInheritor extends ComputeAbstract<Integer, Integer> {
    }

    private static class ComputeAbstractUnoundedInheritor extends ComputeAbstract {
    }

    @SuppressWarnings("rawtypes")
    private static Stream<Class<? extends ComputeTask>> computeTasks() {
        return Stream.of(
            SimpleCompute.class,
            ComputeInexactParam.class,
            ComputeInexactReturn.class,
            ComputeInexactParamAndReturn.class,
            ComputeRaw.class,
            ComputeInheritor.class,
            ComputeBoundedInheritor.class,
            ComputeUnboundedInheritor.class,
            ComputeAbstractBoundedInheritor.class,
            ComputeAbstractUnoundedInheritor.class
        );
    }

    private static class ComputeMismatchParameter implements SimpleTask<Float, Float> {

        @Override
        public Float execute(Float argument) {
            fail();

            return Float.MAX_VALUE;
        }
    }

    /**
     * Tests that compute task fails to execute if an argument is of the wrong type.
     */
    @Test
    void testParameterMismatch() {
        ComputeException computeException = assertThrows(ComputeException.class,
                () -> compute.execute0(ComputeMismatchParameter.class.getName(), 1));

        assertInstanceOf(IllegalArgumentException.class, computeException.getCause());
    }

    /**
     * Tests that execution fails if class name passed to the {@link org.apache.ignite.compute.IgniteCompute#execute(Class, Object)}
     * does not implement {@link ComputeTask}.
     */
    @Test
    void testWrongClass() {
        assertThrows(IllegalArgumentException.class, () -> compute.execute0(Object.class.getName(), 1));
    }

    private static class ComputeWithNullArgument implements SimpleTask<Integer, Integer> {
        @Override
        public Integer execute(Integer argument) {
            return 2;
        }
    }

    /**
     * Tests that null argument can be passed to the execute method.
     */
    @Test
    void testComputeWithNullArgument() {
        Object res = compute.execute0(ComputeWithNullArgument.class.getName(), null);

        assertInstanceOf(Integer.class, res);

        assertEquals(2, res);
    }
}