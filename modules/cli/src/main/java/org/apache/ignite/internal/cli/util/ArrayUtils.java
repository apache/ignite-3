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

package org.apache.ignite.internal.cli.util;

/** Utility class for arrays. */
public final class ArrayUtils {

    private ArrayUtils() {
    }

    /** Finds the last not empty word in array. */
    public static String findLastNotEmptyWord(String[] words) {
        for (int i = words.length - 1; i >= 0; i--) {
            if (!words[i].isBlank()) {
                return words[i];
            }
        }
        return "";
    }

    /** Finds the last not empty word in array before word from the end. */
    public static String findLastNotEmptyWordBeforeWordFromEnd(String[] words, String word) {
        if (word.isBlank()) {
            throw new IllegalArgumentException("word must not be blank");
        }
        if (words.length == 0) {
            return "";
        }
        boolean beforeWordFound = false;
        for (int i = words.length - 1; i >= 0; i--) {
            String currentWord = words[i];
            if (!currentWord.isBlank()) {
                if (beforeWordFound) {
                    return currentWord;
                } else {
                    if (currentWord.equals(word)) {
                        beforeWordFound = true;
                    }
                }
            }
        }
        return "";
    }

    /**
     * Checks if the first array starts with the second array.
     *
     * @param array1 First array.
     * @param array2 Second array.
     * @return {@code true} if the first array starts with the second array, {@code false} otherwise.
     */
    public static boolean firstStartsWithSecond(String[] array1, String[] array2) {
        // Check that the arrays are not null and that the first array is not shorter than the second
        if (array1 == null || array2 == null) {
            throw new IllegalArgumentException("Arrays cannot be null and the first array cannot be shorter than the second");
        }

        if (array1.length < array2.length) {
            return false;
        }

        // Check if the start of the first array is the same as the second array
        for (int i = 0; i < array2.length; i++) {
            if (!array1[i].equals(array2[i])) {
                return false;
            }
        }

        return true;
    }
}
