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
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.Context;
import org.apache.ignite.internal.util.ArrayUtils;
import org.jetbrains.annotations.Nullable;

/**
 * User query context.
 */
public class QueryContext implements Context {

    /** A set of types of SQL commands that can be executed within an operation this context is associated with. **/
    private final Set<SqlQueryType> allowedQueries;

    /** Context params. */
    private final Object[] params;

    /**
     * Constructor.
     *
     * @param allowedQueries Allowed query types.
     * @param params Context params.
     */
    private QueryContext(Set<SqlQueryType> allowedQueries, Object[] params) {
        this.params = params;
        //use EnumSet to have the same order always
        this.allowedQueries = EnumSet.copyOf(allowedQueries);
    }

    /** Returns a set of {@link SqlQueryType allowed query types}. **/
    public Set<SqlQueryType> allowedQueryTypes() {
        return allowedQueries;
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
     * Creates a context that allows the given query types.
     *
     * @param allowedQueries A set of allowed query types.
     * @return Query context.
     */
    public static QueryContext create(Set<SqlQueryType> allowedQueries) {
        return create(allowedQueries, null, ArrayUtils.OBJECT_EMPTY_ARRAY);
    }

    /**
     * Creates a context that allows the given query types and includes an optional parameter.
     *
     * @param allowedQueries A set of allowed query types.
     * @param param A context parameter.
     * @return Query context.
     */
    public static QueryContext create(Set<SqlQueryType> allowedQueries, @Nullable Object param) {
        return create(allowedQueries, param, ArrayUtils.OBJECT_EMPTY_ARRAY);
    }

    /**
     * Creates a context that allows the given query types and includes a list of parameters.
     *
     * @param allowedQueries A set of allowed query types.
     * @param param An optional parameter.
     * @param params Optional array of additional parameters.
     * @return Query context.
     */
    public static QueryContext create(Set<SqlQueryType> allowedQueries, @Nullable Object param, Object... params) {
        Object[] paramsArray = params == null ? ArrayUtils.OBJECT_EMPTY_ARRAY : params;

        if (param == null && paramsArray.length == 0) {
            return new QueryContext(allowedQueries, ArrayUtils.OBJECT_EMPTY_ARRAY);
        } else {
            ArrayList<Object> dst = new ArrayList<>();
            build(dst, param);
            build(dst, paramsArray);
            return new QueryContext(allowedQueries, dst.toArray());
        }
    }

    private static void build(List<Object> dst, Object[] src) {
        for (Object obj : src) {
            build(dst, obj);
        }
    }

    private static void build(List<Object> dst, @Nullable Object obj) {
        if (obj == null) {
            return;
        }
        if (obj.getClass() == QueryContext.class) {
            build(dst, ((QueryContext) obj).params);
        } else {
            dst.add(obj);
        }
    }
}
