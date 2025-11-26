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

package org.apache.ignite.migrationtools.tablemanagement;

import java.util.Map;

/** Utility class for handling TableTypeRegistries. */
public class TableTypeRegistryUtils {
    /**
     * Materializes the table types into a key/value pair.
     *
     * @param descriptor Table Type descriptor.
     * @return Materialized key/value pair of the table types.
     * @throws ClassNotFoundException if one of the types is not available on the classpath.
     */
    public static Map.Entry<Class<?>, Class<?>> typesToEntry(TableTypeDescriptor descriptor) throws ClassNotFoundException {
        return Map.entry(
                Class.forName(descriptor.keyClassName()),
                Class.forName(descriptor.valClassName())
        );
    }

    /**
     * Creates a table descriptor with just the supplied type hints.
     *
     * @param keyClass Type of the key.
     * @param valClass Type of the value.
     * @return the new TableTypeDescriptor instance.
     */
    public static TableTypeDescriptor typeHints(Class<?> keyClass, Class<?> valClass) {
        return new TableTypeDescriptor(keyClass.getName(), valClass.getName(), null, null);
    }
}
