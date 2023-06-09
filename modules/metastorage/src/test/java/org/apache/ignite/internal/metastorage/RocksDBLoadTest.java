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

package org.apache.ignite.internal.metastorage;

import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.server.Value.TOMBSTONE;

import java.io.ObjectStreamException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.dsl.CompoundCondition;
import org.apache.ignite.internal.metastorage.dsl.ConditionType;
import org.apache.ignite.internal.metastorage.dsl.SimpleCondition;
import org.apache.ignite.internal.metastorage.server.AndCondition;
import org.apache.ignite.internal.metastorage.server.Condition;
import org.apache.ignite.internal.metastorage.server.ExistenceCondition;
import org.apache.ignite.internal.metastorage.server.OrCondition;
import org.apache.ignite.internal.metastorage.server.RevisionCondition;
import org.apache.ignite.internal.metastorage.server.TombstoneCondition;
import org.apache.ignite.internal.metastorage.server.ValueCondition;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
public class RocksDBLoadTest {
    private static final IgniteLogger LOG = Loggers.forClass(RocksDBLoadTest.class);

    private byte[] randomBytes() {
        return UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
    }

    private byte[] randomBytes(String prefix) {
        return (prefix + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Increments the last character of the given string.
     */
    private static String incrementLastChar(String str) {
        char lastChar = str.charAt(str.length() - 1);

        return str.substring(0, str.length() - 1) + (char) (lastChar + 1);
    }


    @Test
    public void test(@WorkDirectory Path path) throws InterruptedException {
        System.out.println("start");
        HybridClock clock = new HybridClockImpl();
        RocksDbKeyValueStorage storage = new RocksDbKeyValueStorage("asd", path.resolve("rocksdbtest"));
        storage.start();
        for (int i = 0; i < 1000; i++) {
            storage.put(randomBytes(), randomBytes(), clock.now());
        }
        for (int i = 0; i < 5000; i++) {
            storage.put(randomBytes(), TOMBSTONE, clock.now());
        }
        for (int i = 0; i < 100; i++) {
            storage.put(randomBytes("tables"), randomBytes(), clock.now());
        }
        for (int i = 0; i < 5000; i++) {
            storage.put(randomBytes("tables"), TOMBSTONE, clock.now());
        }

        ByteArray leaseKey = ByteArray.fromString("placementdriver.leases");
        AtomicBoolean leasesStopped = new AtomicBoolean();
        AtomicBoolean rangeStopped = new AtomicBoolean();
        Thread leases = new Thread(() -> {
            byte[] leaseRaw = new byte[500_000];
            byte a = 0;
            while (!leasesStopped.get()) {
                byte[] renewedLease = new byte[500_000];
                renewedLease[0] = ++a;
                storage.invoke(
                        toCondition(or(notExists(leaseKey), value(leaseKey).eq(leaseRaw))),
                        List.of(put(leaseKey, renewedLease)),
                        List.of(noop()),
                        clock.now()
                );
                leaseRaw = renewedLease;

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        leases.start();

        Thread range = new Thread(() -> {
            while (!rangeStopped.get()) {
                long start = System.currentTimeMillis();
                Cursor<Entry> cursor =
                        storage.range("tables".getBytes(StandardCharsets.UTF_8), incrementLastChar("tables").getBytes(StandardCharsets.UTF_8));
                List<Object> list = new ArrayList<>();
                for(Entry e : cursor) {
                    if (!e.tombstone()) {
                        list.add(e.value());
                    }
                }
                LOG.info("time " + (System.currentTimeMillis() - start) + ", size " + list.size());
                cursor.close();

                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        range.start();

        for (int i = 0; i < 180_000; i++) {
            storage.get(randomBytes());
            Thread.sleep(3);
        }

        leasesStopped.set(true);
        rangeStopped.set(true);
        leases.join();
        range.join();
    }



    private static Condition toCondition(org.apache.ignite.internal.metastorage.dsl.Condition condition) {
        if (condition instanceof SimpleCondition.ValueCondition) {
            var valueCondition = (SimpleCondition.ValueCondition) condition;

            return new ValueCondition(
                    toValueConditionType(valueCondition.type()),
                    valueCondition.key(),
                    valueCondition.value()
            );
        } else if (condition instanceof SimpleCondition.RevisionCondition) {
            var revisionCondition = (SimpleCondition.RevisionCondition) condition;

            return new RevisionCondition(
                    toRevisionConditionType(revisionCondition.type()),
                    revisionCondition.key(),
                    revisionCondition.revision()
            );
        } else if (condition instanceof SimpleCondition) {
            var simpleCondition = (SimpleCondition) condition;

            switch (simpleCondition.type()) {
                case KEY_EXISTS:
                    return new ExistenceCondition(ExistenceCondition.Type.EXISTS, simpleCondition.key());

                case KEY_NOT_EXISTS:
                    return new ExistenceCondition(ExistenceCondition.Type.NOT_EXISTS, simpleCondition.key());

                case TOMBSTONE:
                    return new TombstoneCondition(simpleCondition.key());

                default:
                    throw new IllegalArgumentException("Unexpected simple condition type " + simpleCondition.type());
            }
        } else if (condition instanceof CompoundCondition) {
            CompoundCondition compoundCondition = (CompoundCondition) condition;

            Condition leftCondition = toCondition(compoundCondition.leftCondition());
            Condition rightCondition = toCondition(compoundCondition.rightCondition());

            switch (compoundCondition.type()) {
                case AND:
                    return new AndCondition(leftCondition, rightCondition);

                case OR:
                    return new OrCondition(leftCondition, rightCondition);

                default:
                    throw new IllegalArgumentException("Unexpected compound condition type " + compoundCondition.type());
            }
        } else {
            throw new IllegalArgumentException("Unknown condition " + condition);
        }
    }

    private static ValueCondition.Type toValueConditionType(ConditionType type) {
        switch (type) {
            case VAL_EQUAL:
                return ValueCondition.Type.EQUAL;
            case VAL_NOT_EQUAL:
                return ValueCondition.Type.NOT_EQUAL;
            case VAL_GREATER:
                return ValueCondition.Type.GREATER;
            case VAL_GREATER_OR_EQUAL:
                return ValueCondition.Type.GREATER_OR_EQUAL;
            case VAL_LESS:
                return ValueCondition.Type.LESS;
            case VAL_LESS_OR_EQUAL:
                return ValueCondition.Type.LESS_OR_EQUAL;
            default:
                throw new IllegalArgumentException("Unexpected value condition type " + type);
        }
    }

    private static RevisionCondition.Type toRevisionConditionType(ConditionType type) {
        switch (type) {
            case REV_EQUAL:
                return RevisionCondition.Type.EQUAL;
            case REV_NOT_EQUAL:
                return RevisionCondition.Type.NOT_EQUAL;
            case REV_GREATER:
                return RevisionCondition.Type.GREATER;
            case REV_GREATER_OR_EQUAL:
                return RevisionCondition.Type.GREATER_OR_EQUAL;
            case REV_LESS:
                return RevisionCondition.Type.LESS;
            case REV_LESS_OR_EQUAL:
                return RevisionCondition.Type.LESS_OR_EQUAL;
            default:
                throw new IllegalArgumentException("Unexpected revision condition type " + type);
        }
    }
}
