/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
