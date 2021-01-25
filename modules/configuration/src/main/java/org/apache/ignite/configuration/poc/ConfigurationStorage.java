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

package org.apache.ignite.configuration.poc;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/** */
public interface ConfigurationStorage {
//    /** */
//    ReentrantReadWriteLock.ReadLock lock();

    /** */
    Iterable<Pair<String, Serializable>> iterate(String prefix); // Map?

    /** */
    default Stream<Pair<String, Serializable>> stream(String prefix) {
        return StreamSupport.stream(iterate(prefix).spliterator(), false);
    }

    /** */
    Serializable get(String name);

    /** */
    void write(Map<String, Serializable> values);

    /** */
    void listen(String prefix, Listener lsnr);

    interface Listener {
//        void before();
//
//        void after();
//
//        void onUpdate(String key, Serializable value);

        void onUpdate(Map<String, Serializable> values);
    }

    @SuppressWarnings("rawtypes")
    private static <T extends Map<String, Either<T, Serializable>>> T createSuffixMap() {
        return (T)new HashMap();
    }

    static <T extends Map<String, Either<T, Serializable>>> T toSuffixMap(Map<String, Serializable> map) {
        T res = createSuffixMap();

        for (Map.Entry<String, Serializable> entry : map.entrySet()) {
            List<String> keys = Arrays.asList(entry.getKey().split("[.]"));

            insert(res, keys, entry.getValue());
        }

        return null;
    }

    static <T extends Map<String, Either<T, Serializable>>> void insert(T map, List<String> keys, Serializable val) {
        if (keys.isEmpty())
            return;

        String key = keys.get(0);

        if (keys.size() == 1) {
            assert map.get(key) == null;

            map.put(key, new EitherB<>(val));
        }
        else {
            Either<T, Serializable> either = map.get(key);

            T submap;

            if (either == null) {
                either = new EitherA<>(submap = createSuffixMap());

                map.put(key, either);
            }
            else {
                assert either.isA();

                submap = either.a();
            }

            insert(submap, keys.subList(1, keys.size()), val);
        }
    }

    interface Either<A, B> {
        boolean isA(); // I know that this is not the right way. It only a prototype after all.
        A a();
        B b();
    }

    class EitherA<A, B> implements Either<A, B> {
        private final A a;

        public EitherA(A a) {
            this.a = a;
        }

        @Override public boolean isA() {
            return true;
        }

        @Override public A a() {
            return a;
        }

        @Override public B b() {
            throw new IllegalStateException();
        }
    }

    class EitherB<A, B> implements Either<A, B> {
        private final B b;

        public EitherB(B b) {
            this.b = b;
        }

        @Override public boolean isA() {
            return false;
        }

        @Override public A a() {
            throw new IllegalStateException();
        }

        @Override public B b() {
            return b;
        }
    }
}
