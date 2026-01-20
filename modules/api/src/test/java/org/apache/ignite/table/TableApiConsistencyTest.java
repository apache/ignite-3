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
    void testKvGetAsync() {
        Object key = new Object();

        kvViewMock.getAsync(key);

        verify(kvViewMock).getAsync(eq(null), eq(key));
    }

    @Test
    void testKvGetNullable() {
        Object key = new Object();

        kvViewMock.getNullable(key);

        verify(kvViewMock).getNullable(eq(null), eq(key));
    }

    @Test
    void testKvGetNullableAsync() {
        Object key = new Object();

        kvViewMock.getNullableAsync(key);

        verify(kvViewMock).getNullableAsync(eq(null), eq(key));
    }

    @Test
    void testKvGetOrDefault() {
        Object key = new Object();
        Object def = new Object();

        kvViewMock.getOrDefault(key, def);

        verify(kvViewMock).getOrDefault(eq(null), eq(key), eq(def));
    }

    @Test
    void testKvGetOrDefaultAsync() {
        Object key = new Object();
        Object def = new Object();

        kvViewMock.getOrDefaultAsync(key, def);

        verify(kvViewMock).getOrDefaultAsync(eq(null), eq(key), eq(def));
    }

    @Test
    void testKvGetAll() {
        Collection<Object> keys = new ArrayList<>();

        kvViewMock.getAll(keys);

        verify(kvViewMock).getAll(eq(null), eq(keys));
    }

    @Test
    void testKvGetAllAsync() {
        Collection<Object> keys = new ArrayList<>();

        kvViewMock.getAllAsync(keys);

        verify(kvViewMock).getAllAsync(eq(null), eq(keys));
    }

    @Test
    void testKvContains() {
        Object key = new Object();

        kvViewMock.contains(key);

        verify(kvViewMock).contains(eq(null), eq(key));
    }

    @Test
    void testKvContainsAsync() {
        Object key = new Object();

        kvViewMock.containsAsync(key);

        verify(kvViewMock).containsAsync(eq(null), eq(key));
    }

    @Test
    void testKvContainsAll() {
        Collection<Object> keys = new ArrayList<>();

        kvViewMock.containsAll(keys);

        verify(kvViewMock).containsAll(eq(null), eq(keys));
    }

    @Test
    void testKvContainsAllAsync() {
        Collection<Object> keys = new ArrayList<>();

        kvViewMock.containsAllAsync(keys);

        verify(kvViewMock).containsAllAsync(eq(null), eq(keys));
    }

    @Test
    void testKvPut() {
        Object key = new Object();
        Object val = new Object();

        kvViewMock.put(key, val);

        verify(kvViewMock).put(eq(null), eq(key), eq(val));
    }

    @Test
    void testKvPutAsync() {
        Object key = new Object();
        Object val = new Object();

        kvViewMock.putAsync(key, val);

        verify(kvViewMock).putAsync(eq(null), eq(key), eq(val));
    }

    @Test
    void testKvPutAll() {
        Map<Object, Object> map = new HashMap<>();

        kvViewMock.putAll(map);

        verify(kvViewMock).putAll(eq(null), eq(map));
    }

    @Test
    void testKvPutAllAsync() {
        Map<Object, Object> map = new HashMap<>();

        kvViewMock.putAllAsync(map);

        verify(kvViewMock).putAllAsync(eq(null), eq(map));
    }

    @Test
    void testKvRemove() {
        Object key = new Object();

        kvViewMock.remove(key);

        verify(kvViewMock).remove(eq(null), eq(key));
    }

    @Test
    void testKvRemoveAsync() {
        Object key = new Object();

        kvViewMock.removeAsync(key);

        verify(kvViewMock).removeAsync(eq(null), eq(key));
    }

    @Test
    void testKvRemoveExact() {
        Object key = new Object();
        Object val = new Object();

        kvViewMock.removeExact(key, val);

        verify(kvViewMock).removeExact(eq(null), eq(key), eq(val));
    }

    @Test
    void testKvRemoveExactAsync() {
        Object key = new Object();
        Object val = new Object();

        kvViewMock.removeExactAsync(key, val);

        verify(kvViewMock).removeExactAsync(eq(null), eq(key), eq(val));
    }

    @Test
    void testKvRemoveAll() {
        kvViewMock.removeAll();

        verify(kvViewMock).removeAll(eq(null));
    }

    @Test
    void testKvRemoveAllAsync() {
        kvViewMock.removeAllAsync();

        verify(kvViewMock).removeAllAsync(eq(null));
    }

    @Test
    void testKvGetAndRemove() {
        Object key = new Object();

        kvViewMock.getAndRemove(key);

        verify(kvViewMock).getAndRemove(eq(null), eq(key));
    }

    @Test
    void testKvGetAndRemoveAsync() {
        Object key = new Object();

        kvViewMock.getAndRemoveAsync(key);

        verify(kvViewMock).getAndRemoveAsync(eq(null), eq(key));
    }

    @Test
    void testKvReplace() {
        Object key = new Object();
        Object val = new Object();

        kvViewMock.replace(key, val);

        verify(kvViewMock).replace(eq(null), eq(key), eq(val));
    }

    @Test
    void testKvReplaceAsync() {
        Object key = new Object();
        Object val = new Object();

        kvViewMock.replaceAsync(key, val);

        verify(kvViewMock).replaceAsync(eq(null), eq(key), eq(val));
    }

    @Test
    void testKvReplaceExact() {
        Object key = new Object();
        Object oldVal = new Object();
        Object newVal = new Object();

        kvViewMock.replaceExact(key, oldVal, newVal);

        verify(kvViewMock).replaceExact(eq(null), eq(key), eq(oldVal), eq(newVal));
    }

    @Test
    void testKvReplaceExactAsync() {
        Object key = new Object();
        Object oldVal = new Object();
        Object newVal = new Object();

        kvViewMock.replaceExactAsync(key, oldVal, newVal);

        verify(kvViewMock).replaceExactAsync(eq(null), eq(key), eq(oldVal), eq(newVal));
    }

    @Test
    void testRecGet() {
        Object rec = new Object();

        recViewMock.get(rec);

        verify(recViewMock).get(eq(null), eq(rec));
    }

    @Test
    void testRecGetAsync() {
        Object rec = new Object();

        recViewMock.getAsync(rec);

        verify(recViewMock).getAsync(eq(null), eq(rec));
    }

    @Test
    void testRecGetAll() {
        Collection<Object> recs = mock(Collection.class);

        recViewMock.getAll(recs);

        verify(recViewMock).getAll(eq(null), eq(recs));
    }

    @Test
    void testRecGetAllAsync() {
        Collection<Object> recs = mock(Collection.class);

        recViewMock.getAllAsync(recs);

        verify(recViewMock).getAllAsync(eq(null), eq(recs));
    }

    @Test
    void testRecContains() {
        Object rec = new Object();

        recViewMock.contains(rec);

        verify(recViewMock).contains(eq(null), eq(rec));
    }

    @Test
    void testRecContainsAsync() {
        Object rec = new Object();

        recViewMock.containsAsync(rec);

        verify(recViewMock).containsAsync(eq(null), eq(rec));
    }

    @Test
    void testRecContainsAll() {
        Collection<Object> recs = mock(Collection.class);

        recViewMock.containsAll(recs);

        verify(recViewMock).containsAll(eq(null), eq(recs));
    }

    @Test
    void testRecContainsAllAsync() {
        Collection<Object> recs = mock(Collection.class);

        recViewMock.containsAllAsync(recs);

        verify(recViewMock).containsAllAsync(eq(null), eq(recs));
    }

    @Test
    void testRecUpsert() {
        Object rec = new Object();

        recViewMock.upsert(rec);

        verify(recViewMock).upsert(eq(null), eq(rec));
    }

    @Test
    void testRecUpsertAsync() {
        Object rec = new Object();

        recViewMock.upsertAsync(rec);

        verify(recViewMock).upsertAsync(eq(null), eq(rec));
    }

    @Test
    void testRecUpsertAll() {
        Collection<Object> recs = mock(Collection.class);

        recViewMock.upsertAll(recs);

        verify(recViewMock).upsertAll(eq(null), eq(recs));
    }

    @Test
    void testRecUpsertAllAsync() {
        Collection<Object> recs = mock(Collection.class);

        recViewMock.upsertAllAsync(recs);

        verify(recViewMock).upsertAllAsync(eq(null), eq(recs));
    }

    @Test
    void testRecGetAndUpsert() {
        Object rec = new Object();

        recViewMock.getAndUpsert(rec);

        verify(recViewMock).getAndUpsert(eq(null), eq(rec));
    }

    @Test
    void testRecGetAndUpsertAsync() {
        Object rec = new Object();

        recViewMock.getAndUpsertAsync(rec);

        verify(recViewMock).getAndUpsertAsync(eq(null), eq(rec));
    }

    @Test
    void testRecInsert() {
        Object rec = new Object();

        recViewMock.insert(rec);

        verify(recViewMock).insert(eq(null), eq(rec));
    }

    @Test
    void testRecInsertAsync() {
        Object rec = new Object();

        recViewMock.insertAsync(rec);

        verify(recViewMock).insertAsync(eq(null), eq(rec));
    }

    @Test
    void testRecInsertAll() {
        Collection<Object> recs = mock(Collection.class);

        recViewMock.insertAll(recs);

        verify(recViewMock).insertAll(eq(null), eq(recs));
    }

    @Test
    void testRecInsertAllAsync() {
        Collection<Object> recs = mock(Collection.class);

        recViewMock.insertAllAsync(recs);

        verify(recViewMock).insertAllAsync(eq(null), eq(recs));
    }

    @Test
    void testRecReplace() {
        Object rec = new Object();

        recViewMock.replace(rec);

        verify(recViewMock).replace(eq(null), eq(rec));
    }

    @Test
    void testRecReplaceExact() {
        Object oldRec = new Object();
        Object newRec = new Object();

        recViewMock.replaceExact(oldRec, newRec);

        verify(recViewMock).replaceExact(eq(null), eq(oldRec), eq(newRec));
    }

    @Test
    void testRecReplaceAsync() {
        Object rec = new Object();

        recViewMock.replaceAsync(rec);

        verify(recViewMock).replaceAsync(eq(null), eq(rec));
    }

    @Test
    void testRecReplaceExactAsync() {
        Object oldRec = new Object();
        Object newRec = new Object();

        recViewMock.replaceExactAsync(oldRec, newRec);

        verify(recViewMock).replaceExactAsync(eq(null), eq(oldRec), eq(newRec));
    }

    @Test
    void testRecGetAndReplace() {
        Object rec = new Object();

        recViewMock.getAndReplace(rec);

        verify(recViewMock).getAndReplace(eq(null), eq(rec));
    }

    @Test
    void testRecGetAndReplaceAsync() {
        Object rec = new Object();

        recViewMock.getAndReplaceAsync(rec);

        verify(recViewMock).getAndReplaceAsync(eq(null), eq(rec));
    }

    @Test
    void testRecDelete() {
        Object rec = new Object();

        recViewMock.delete(rec);

        verify(recViewMock).delete(eq(null), eq(rec));
    }

    @Test
    void testRecDeleteAsync() {
        Object rec = new Object();

        recViewMock.deleteAsync(rec);

        verify(recViewMock).deleteAsync(eq(null), eq(rec));
    }

    @Test
    void testRecDeleteExact() {
        Object rec = new Object();

        recViewMock.deleteExact(rec);

        verify(recViewMock).deleteExact(eq(null), eq(rec));
    }

    @Test
    void testRecDeleteExactAsync() {
        Object rec = new Object();

        recViewMock.deleteExactAsync(rec);

        verify(recViewMock).deleteExactAsync(eq(null), eq(rec));
    }

    @Test
    void testRecGetAndDelete() {
        Object rec = new Object();

        recViewMock.getAndDelete(rec);

        verify(recViewMock).getAndDelete(eq(null), eq(rec));
    }

    @Test
    void testRecGetAndDeleteAsync() {
        Object rec = new Object();

        recViewMock.getAndDeleteAsync(rec);

        verify(recViewMock).getAndDeleteAsync(eq(null), eq(rec));
    }

    @Test
    void testRecDeleteAll() {
        recViewMock.deleteAll();

        verify(recViewMock).deleteAll(eq(null));
    }

    @Test
    void testRecDeleteAllAsync() {
        recViewMock.deleteAllAsync();

        verify(recViewMock).deleteAllAsync(eq(null));
    }

    @Test
    void testRecDeleteAllExact() {
        Collection<Object> recs = mock(Collection.class);

        recViewMock.deleteAllExact(recs);

        verify(recViewMock).deleteAllExact(eq(null), eq(recs));
    }

    @Test
    void testRecDeleteAllExactAsync() {
        Collection<Object> recs = mock(Collection.class);

        recViewMock.deleteAllExactAsync(recs);

        verify(recViewMock).deleteAllExactAsync(eq(null), eq(recs));
    }
}
