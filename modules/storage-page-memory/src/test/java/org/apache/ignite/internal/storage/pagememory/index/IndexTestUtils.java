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

package org.apache.ignite.internal.storage.pagememory.index;

import static java.util.stream.Collectors.joining;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

/**
 * Helper class for testing indexes.
 */
class IndexTestUtils {
    /**
     * Returns a string with pseudo-random characters from 'A' to 'Z'.
     *
     * @param size Number of characters.
     */
    static String randomString(int size) {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        return IntStream.range(0, size)
                .map(i -> 'A' + random.nextInt('Z' - 'A'))
                .mapToObj(i -> String.valueOf((char) i))
                .collect(joining());
    }
}
