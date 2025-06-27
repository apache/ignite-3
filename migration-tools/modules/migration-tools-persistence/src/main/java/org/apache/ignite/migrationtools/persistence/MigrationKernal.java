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

package org.apache.ignite.migrationtools.persistence;

import java.lang.reflect.Field;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;

/** Custom {@link IgniteKernal} for the migration. */
public class MigrationKernal extends IgniteKernal {

    private static final Field CTX_FIELD;

    private static final Field CFG_FIELD;

    static {
        try {
            CTX_FIELD = IgniteKernal.class.getDeclaredField("ctx");
            CTX_FIELD.setAccessible(true);

            CFG_FIELD = IgniteKernal.class.getDeclaredField("cfg");
            CFG_FIELD.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }

    }

    /** Factory method. */
    public static MigrationKernal create(GridKernalContext ctx, IgniteConfiguration cfg) {
        try {
            var newObj = new MigrationKernal();
            CTX_FIELD.set(newObj, ctx);
            CFG_FIELD.set(newObj, cfg);
            return newObj;
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private MigrationKernal() {
        super();
    }
}
