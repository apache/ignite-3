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

package org.apache.ignite.internal.tx;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/** */
public class TestDeadlock {
    @Test
    public void test() throws ExecutionException, InterruptedException {
        ReadWriteLock lock1 = new ReentrantReadWriteLock();
        ReadWriteLock lock2 = new ReentrantReadWriteLock();

        CountDownLatch l = new CountDownLatch(2);

        Thread t1 = new Thread(new Runnable() {
            @Override public void run() {
                lock1.readLock().lock();

                l.countDown();
                try {
                    l.await();
                }
                catch (InterruptedException e) {
                    return;
                }

                lock2.writeLock().lock();
                lock2.writeLock().unlock();

                lock1.readLock().unlock();
            }
        });

        Thread t2 = new Thread(new Runnable() {
            @Override public void run() {
                lock2.readLock().lock();

                l.countDown();
                try {
                    l.await();
                }
                catch (InterruptedException e) {
                    return;
                }

                lock1.writeLock().lock();
                lock1.writeLock().unlock();

                lock2.readLock().unlock();
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();
    }
}
