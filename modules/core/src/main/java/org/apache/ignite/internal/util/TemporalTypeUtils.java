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

package org.apache.ignite.internal.util;

/**
 * Helper class for temporal type conversions.
 */
public class TemporalTypeUtils {
    /**
     * Normalize nanoseconds regarding the precision.
     *
     * @param nanos     Nanoseconds.
     * @param precision Meaningful digits.
     * @return Normalized nanoseconds.
     */
    public static int normalizeNanos(int nanos, int precision) {
        switch (precision) {
            case 0:
                nanos = 0;
                break;
            case 1:
                nanos = (nanos / 100_000_000) * 100_000_000; // 100ms precision.
                break;
            case 2:
                nanos = (nanos / 10_000_000) * 10_000_000; // 10ms precision.
                break;
            case 3: {
                nanos = (nanos / 1_000_000) * 1_000_000; // 1ms precision.
                break;
            }
            case 4: {
                nanos = (nanos / 100_000) * 100_000; // 100us precision.
                break;
            }
            case 5: {
                nanos = (nanos / 10_000) * 10_000; // 10us precision.
                break;
            }
            case 6: {
                nanos = (nanos / 1_000) * 1_000; // 1us precision.
                break;
            }
            case 7: {
                nanos = (nanos / 100) * 100; // 100ns precision.
                break;
            }
            case 8: {
                nanos = (nanos / 10) * 10; // 10ns precision.
                break;
            }
            case 9: {
                // 1ns precision
                break;
            }
            default: // Should never get here.
                throw new IllegalArgumentException("Unsupported fractional seconds precision: " + precision);
        }

        return nanos;
    }
}
