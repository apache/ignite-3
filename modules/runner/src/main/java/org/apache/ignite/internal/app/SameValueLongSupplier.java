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

package org.apache.ignite.internal.app;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;

/**
 * {@link LongSupplier} that fails an assertion if a wrapped supplier returns a value different
 * from the one returned on the previous call.
 */
class SameValueLongSupplier implements LongSupplier {
    private static final long NO_VALUE = Long.MIN_VALUE;

    private final AtomicLong previousValue = new AtomicLong(NO_VALUE);

    private final LongUnaryOperator assertingUpdate;

    SameValueLongSupplier(LongSupplier supplier) {
        assertingUpdate = prev -> {
            long current = supplier.getAsLong();

            assert prev == NO_VALUE || current == prev : "Previous value was " + prev + ", but current value is " + current;

            return current;
        };
    }

    @Override
    public long getAsLong() {
        return previousValue.updateAndGet(assertingUpdate);
    }
}
