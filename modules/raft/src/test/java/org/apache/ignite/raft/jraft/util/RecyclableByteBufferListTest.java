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
package org.apache.ignite.raft.jraft.util;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertSame;

/**
 *
 */
public class RecyclableByteBufferListTest {

    @Test(expected = IllegalStateException.class)
    public void testMultipleRecycle() {
        final RecyclableByteBufferList object = RecyclableByteBufferList.newInstance();
        object.recycle();
        object.recycle();
    }

    @Test
    public void testMultipleRecycleAtDifferentThread() throws InterruptedException {
        final RecyclableByteBufferList object = RecyclableByteBufferList.newInstance();
        final Thread thread1 = new Thread(object::recycle);
        thread1.start();
        thread1.join();
        assertSame(object, RecyclableByteBufferList.newInstance());
    }

    @Test
    public void testRecycle() {
        final RecyclableByteBufferList object = RecyclableByteBufferList.newInstance();
        object.recycle();
        final RecyclableByteBufferList object2 = RecyclableByteBufferList.newInstance();
        Assert.assertSame(object, object2);
        object2.recycle();
    }
}
