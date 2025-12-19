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

package org.apache.ignite.table.mapper;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/** Provides utility methods to support records in Java 11. */
class RecordSupport {
    private RecordSupport() {
        // No-op.
    }

    /**
     * Java 11 compatible, reflection based alternative, that returns true if and only if this class is a record class.
     *
     * <p>Without reflection:
     *
     * <pre>
     * {@code
     * Class<?> clazz;
     * clazz.isRecord();
     * }
     * </pre>
     */
    static boolean isRecord(Class<?> clazz) throws IllegalAccessException {
        if (Runtime.version().version().get(0) < 14) {
            return false;
        }

        try {
            Method isRecordMtd = Class.class.getDeclaredMethod("isRecord");
            return (boolean) isRecordMtd.invoke(clazz);
        } catch (NoSuchMethodException ex) {
            // Should never happen, but if the method does not exist it would mean there's no support for records.
            return false;
        } catch (InvocationTargetException e) {
            // isRecord does not declare any checked exception. Therefore, this should be safe.
            var cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else if  (cause instanceof Error) {
                throw (Error) cause;
            } else {
                // Should never reach here.
                throw new RuntimeException(cause);
            }
        }
    }
}
