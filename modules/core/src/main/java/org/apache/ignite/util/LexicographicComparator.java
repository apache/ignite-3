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

package org.apache.ignite.util;

import java.util.Comparator;

/**
 * Byte array lexicographic comparator.
 */
public class LexicographicComparator implements Comparator<byte[]> {
    /** Comparator instance. */
    private static final Comparator<byte[]> INSTANCE = new LexicographicComparator();

    /**
     * Returns instance of comparator.
     *
     * @return Comparator instance.
     */
    public static Comparator<byte[]> getInstance() {
        return INSTANCE;
    }

    /** {@inheritDoc} */
    @Override public int compare(byte[] o1, byte[] o2) {
        int minLength = Math.min(o1.length, o2.length);

        for (int i = 0; i < minLength; ++i) {
            int res = Byte.compareUnsigned(o1[i], o2[i]);

            if (res != 0)
                return res;
        }

        return o1.length - o2.length;
    }
}
