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

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

/**
 * Test class to verify various requirements to Table API.
 */
public class TableApiConsistencyTest {

    Set<Method> noTxParamExclusions = Set.of(
            KeyValueView.class.getDeclaredMethod("removeAll", Transaction.class, Collection.class),
            KeyValueView.class.getDeclaredMethod("removeAllAsync", Transaction.class, Collection.class),
            RecordView.class.getDeclaredMethod("deleteAll", Transaction.class, Collection.class),
            RecordView.class.getDeclaredMethod("deleteAllAsync", Transaction.class, Collection.class)
    );

    private KeyValueView<Object, Object> kvViewMock;
    private RecordView<Object> recViewMock;

    public TableApiConsistencyTest() throws NoSuchMethodException {
    }

    @BeforeEach
    public void setUp() {
        kvViewMock = mock(KeyValueView.class, Mockito.CALLS_REAL_METHODS);
        recViewMock = mock(RecordView.class, Mockito.CALLS_REAL_METHODS);
    }

    /**
     * Test validates that if method requires {@link Transaction} parameter then overloaded method without such parameter should exist.
     */
    @ParameterizedTest
    @ValueSource(classes = {KeyValueView.class, RecordView.class})
    public void methodsWithOptionalTxShouldHaveSimpleOverload(Class<?> clazz) {
        for (Method mtd : clazz.getDeclaredMethods()) {
            var params = mtd.getParameters();

            int txParIdx = txParamIdx(params);
            boolean deprecated = mtd.isAnnotationPresent(Deprecated.class);

            if (txParIdx != -1 && !deprecated) {
                Class<?>[] altParams = Arrays.stream(params)
                        .filter(p -> p != params[txParIdx])
                        .map(Parameter::getType)
                        .toArray(Class[]::new);

                Method altMtd;
                try {
                    altMtd = clazz.getDeclaredMethod(mtd.getName(), altParams);
                } catch (NoSuchMethodException e) {
                    altMtd = null;
                }

                if (altMtd == null && !noTxParamExclusions.contains(mtd)) {
                    fail("Method " + mtd + " should have overload without transaction parameter");
                }

                if (altMtd != null && noTxParamExclusions.contains(mtd)) {
                    fail("Method " + mtd + " has overload without transaction parameter but listed within exclusions");
                }
            }
        }
    }

    private int txParamIdx(Parameter[] params) {
        for (int i = 0; i < params.length; i++) {
            if (params[i].getType() == Transaction.class) {
                return i;
            }
        }

        return -1;
    }

    @Test
    public void testKvGet() {
        Object key = new Object();
        kvViewMock.get(key);

        verify(kvViewMock).get(eq(null), eq(key));
    }

    @Test
    void testGetAsync() {
        Object key = new Object();

        kvViewMock.getAsync(key);

        verify(kvViewMock).getAsync(eq(null), eq(key));
    }

    @Test
    void testGetNullable() {
        Object key = new Object();

        kvViewMock.getNullable(key);

        verify(kvViewMock).getNullable(eq(null), eq(key));
    }

    @Test
    void testGetNullableAsync() {
        Object key = new Object();

        kvViewMock.getNullableAsync(key);

        verify(kvViewMock).getNullableAsync(eq(null), eq(key));
    }

    @Test
    void testGetOrDefault() {
        Object key = new Object();
        Object def = new Object();

        kvViewMock.getOrDefault(key, def);

        verify(kvViewMock).getOrDefault(eq(null), eq(key), eq(def));
    }

    @Test
    void testGetOrDefaultAsync() {
        Object key = new Object();
        Object def = new Object();

        kvViewMock.getOrDefaultAsync(key, def);

        verify(kvViewMock).getOrDefaultAsync(eq(null), eq(key), eq(def));
    }

    @Test
    void testGetAll() {
        Collection<Object> keys = new ArrayList<>();

        kvViewMock.getAll(keys);

        verify(kvViewMock).getAll(eq(null), eq(keys));
    }

    @Test
    void testGetAllAsync() {
        Collection<Object> keys = new ArrayList<>();

        kvViewMock.getAllAsync(keys);

        verify(kvViewMock).getAllAsync(eq(null), eq(keys));
    }

    @Test
    void testContains() {
        Object key = new Object();

        kvViewMock.contains(key);

        verify(kvViewMock).contains(eq(null), eq(key));
    }

    @Test
    void testContainsAsync() {
        Object key = new Object();

        kvViewMock.containsAsync(key);

        verify(kvViewMock).containsAsync(eq(null), eq(key));
    }

    @Test
    void testContainsAll() {
        Collection<Object> keys = new ArrayList<>();

        kvViewMock.containsAll(keys);

        verify(kvViewMock).containsAll(eq(null), eq(keys));
    }

    @Test
    void testContainsAllAsync() {
        Collection<Object> keys = new ArrayList<>();

        kvViewMock.containsAllAsync(keys);

        verify(kvViewMock).containsAllAsync(eq(null), eq(keys));
    }

    @Test
    void testPut() {
        Object key = new Object();
        Object val = new Object();

        kvViewMock.put(key, val);

        verify(kvViewMock).put(eq(null), eq(key), eq(val));
    }

    @Test
    void testPutAsync() {
        Object key = new Object();
        Object val = new Object();

        kvViewMock.putAsync(key, val);

        verify(kvViewMock).putAsync(eq(null), eq(key), eq(val));
    }

    @Test
    void testPutAll() {
        Map<Object, Object> map = new HashMap<>();

        kvViewMock.putAll(map);

        verify(kvViewMock).putAll(eq(null), eq(map));
    }

    @Test
    void testPutAllAsync() {
        Map<Object, Object> map = new HashMap<>();

        kvViewMock.putAllAsync(map);

        verify(kvViewMock).putAllAsync(eq(null), eq(map));
    }

    @Test
    void testRemove() {
        Object key = new Object();

        kvViewMock.remove(key);

        verify(kvViewMock).remove(eq(null), eq(key));
    }

    @Test
    void testRemoveAsync() {
        Object key = new Object();

        kvViewMock.removeAsync(key);

        verify(kvViewMock).removeAsync(eq(null), eq(key));
    }

    @Test
    void testRemoveExact() {
        Object key = new Object();
        Object val = new Object();

        kvViewMock.removeExact(key, val);

        verify(kvViewMock).removeExact(eq(null), eq(key), eq(val));
    }

    @Test
    void testRemoveExactAsync() {
        Object key = new Object();
        Object val = new Object();

        kvViewMock.removeExactAsync(key, val);

        verify(kvViewMock).removeExactAsync(eq(null), eq(key), eq(val));
    }

    @Test
    void testRemoveAll() {
        kvViewMock.removeAll();

        verify(kvViewMock).removeAll(eq(null));
    }

    @Test
    void testRemoveAllAsync() {
        kvViewMock.removeAllAsync();

        verify(kvViewMock).removeAllAsync(eq(null));
    }

    @Test
    void testGetAndRemove() {
        Object key = new Object();

        kvViewMock.getAndRemove(key);

        verify(kvViewMock).getAndRemove(eq(null), eq(key));
    }

    @Test
    void testGetAndRemoveAsync() {
        Object key = new Object();

        kvViewMock.getAndRemoveAsync(key);

        verify(kvViewMock).getAndRemoveAsync(eq(null), eq(key));
    }

    @Test
    void testReplace() {
        Object key = new Object();
        Object val = new Object();

        kvViewMock.replace(key, val);

        verify(kvViewMock).replace(eq(null), eq(key), eq(val));
    }

    @Test
    void testReplaceAsync() {
        Object key = new Object();
        Object val = new Object();

        kvViewMock.replaceAsync(key, val);

        verify(kvViewMock).replaceAsync(eq(null), eq(key), eq(val));
    }

    @Test
    void testReplaceExact() {
        Object key = new Object();
        Object oldVal = new Object();
        Object newVal = new Object();

        kvViewMock.replaceExact(key, oldVal, newVal);

        verify(kvViewMock).replaceExact(eq(null), eq(key), eq(oldVal), eq(newVal));
    }

    @Test
    void testReplaceExactAsync() {
        Object key = new Object();
        Object oldVal = new Object();
        Object newVal = new Object();

        kvViewMock.replaceExactAsync(key, oldVal, newVal);

        verify(kvViewMock).replaceExactAsync(eq(null), eq(key), eq(oldVal), eq(newVal));
    }
}
