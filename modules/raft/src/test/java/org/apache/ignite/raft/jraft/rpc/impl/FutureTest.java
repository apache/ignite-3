/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.rpc.impl;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FutureTest {
    private static final Logger log = LoggerFactory.getLogger(FutureImpl.class);

    private static final class NotifyFutureRunner implements Runnable {
        FutureImpl<Boolean> future;
        long sleepTime;
        Throwable throwable;

        NotifyFutureRunner(FutureImpl<Boolean> future, long sleepTime, Throwable throwable) {
            super();
            this.future = future;
            this.sleepTime = sleepTime;
            this.throwable = throwable;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(this.sleepTime);
                if (this.throwable != null) {
                    this.future.completeExceptionally(this.throwable);
                }
                else {
                    this.future.complete(true);
                }
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void testGet() throws Exception {
        FutureImpl<Boolean> future = new FutureImpl<Boolean>();
        new Thread(new NotifyFutureRunner(future, 2000, null)).start();
        boolean result = future.get();
        assertTrue(result);
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
    }

    @Test
    public void testGetImmediately() throws Exception {
        FutureImpl<Boolean> future = new FutureImpl<Boolean>();
        future.complete(true);
        boolean result = future.get();
        assertTrue(result);
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
    }

    @Test
    public void testGetException() throws Exception {
        FutureImpl<Boolean> future = new FutureImpl<Boolean>();
        new Thread(new NotifyFutureRunner(future, 2000, new IOException("hello"))).start();
        try {
            future.get();
            fail();
        }
        catch (ExecutionException e) {
            assertEquals("hello", e.getCause().getMessage());

        }
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());

    }

    @Test
    public void testCancel() throws Exception {
        final FutureImpl<Boolean> future = new FutureImpl<Boolean>();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(3000);
                    future.cancel(true);
                }
                catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }).start();
        try {
            future.get();
            fail();
        }
        catch (CancellationException e) {
            assertTrue(true);

        }
        assertTrue(future.isDone());
        assertTrue(future.isCancelled());
    }

    @Test
    public void testGetTimeout() throws Exception {
        FutureImpl<Boolean> future = new FutureImpl<Boolean>();
        try {
            future.get(1000, TimeUnit.MILLISECONDS);
            fail();
        }
        catch (TimeoutException e) {
            assertTrue(true);
        }
    }
}