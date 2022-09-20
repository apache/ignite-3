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

package org.apache.ignite.internal.sql.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.plan.Context;
import org.apache.ignite.internal.util.ArrayUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * User query context.
 */
public class QueryContext implements Context {
    /** Context params. */
    private final Object[] params;

    /**
     * Constructor.
     *
     * @param params Context params.
     */
    private QueryContext(Object[] params) {
        this.params = params;
    }

    /** {@inheritDoc} */
    @Override
    public <C> @Nullable C unwrap(Class<C> cls) {
        if (Object[].class == cls) {
            return cls.cast(params);
        }

        return Arrays.stream(params).filter(cls::isInstance).findFirst().map(cls::cast).orElse(null);
    }

    /**
     * Creates a context from a list of parameters.
     *
     * @param params Context parameters.
     * @return Query context.
     */
    public static QueryContext of(Object... params) {
        return !ArrayUtils.nullOrEmpty(params) ? new QueryContext(build(null, params).toArray())
                : new QueryContext(ArrayUtils.OBJECT_EMPTY_ARRAY);
    }

    private static List<Object> build(List<Object> dst, Object[] src) {
        if (dst == null) {
            dst = new ArrayList<>();
        }

        for (Object obj : src) {
            if (obj == null) {
                continue;
            }

            if (obj.getClass() == QueryContext.class) {
                build(dst, ((QueryContext) obj).params);
            } else {
                dst.add(obj);
            }
        }

        return dst;
    }
}
