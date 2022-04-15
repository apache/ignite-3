/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.lang.IgniteStringFormatter;

public class DbCounter {
    public static final Map<String, Counter> COUNTERS = new ConcurrentHashMap<>();

    static {
        addCntr("clusterState");
        addCntr("kv");
        addCntr("log");
        addCntr("table");
        addCntr("vault");
    }

    private static void addCntr(String name) {
        COUNTERS.put(name, new Counter(name));
    }

    public static void incCluster() {
        COUNTERS.get("clusterState").inc();
    }

    public static void decCluster() {
        COUNTERS.get("clusterState").dec();
    }

    public static void incKv() {
        COUNTERS.get("kv").inc();
    }

    public static void decKv() {
        COUNTERS.get("kv").dec();
    }

    public static void incLog() {
        COUNTERS.get("log").inc();
    }

    public static void decLog() {
        COUNTERS.get("log").dec();
    }

    public static void incTable() {
        COUNTERS.get("table").inc();
    }

    public static void decTable() {
        COUNTERS.get("table").dec();
    }

    public static void incVault() {
        COUNTERS.get("vault").inc();
    }

    public static void decVault() {
        COUNTERS.get("vault").dec();
    }

    public static String each() {
        StringBuilder sb = new StringBuilder();
        COUNTERS.forEach((k,v) -> {
            sb
                .append(k).append("=").append(v.getEach()).append(", ")
                .append(k).append("Total=").append(v.getTotalEach()).append(", ");
        });
        return sb.toString();
    }

    public static String all() {
        StringBuilder sb = new StringBuilder();
        COUNTERS.forEach((k,v) -> {
            sb.append(k).append("=").append(v.getAll()).append(", ");
        });
        return sb.toString();
    }

    public static class Counter {
        public final String name;
        final AtomicInteger each = new AtomicInteger();
        final AtomicInteger totalEach = new AtomicInteger();
        final AtomicInteger all = new AtomicInteger();

        public Counter(String name) {
            this.name = name;
        }

        public void inc() {
            each.incrementAndGet();
            totalEach.incrementAndGet();
            all.incrementAndGet();
        }

        public void dec() {
            each.decrementAndGet();
            all.decrementAndGet();
        }

        public int getEach() {
            return each.getAndSet(0);
        }

        public int getTotalEach() {
            return totalEach.get();
        }

        public int getAll() {
            return all.getAndSet(0);
        }
    }
}
