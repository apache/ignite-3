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

package org.apache.ignite.internal.sql.engine.util;

import static org.apache.ignite.internal.lang.IgniteExceptionMapper.unchecked;
import static org.apache.ignite.lang.ErrorGroups.Sql.SQL_ERR_GROUP;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.lang.IgniteExceptionMapper;
import org.apache.ignite.internal.lang.IgniteExceptionMappersProvider;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
import org.apache.ignite.internal.sql.engine.exec.RemoteFragmentExecutionException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.SqlException;

/**
 * SQL module exception mapper.
 */
@AutoService(IgniteExceptionMappersProvider.class)
public class SqlExceptionMapperProvider implements IgniteExceptionMappersProvider {
    @Override
    public Collection<IgniteExceptionMapper<?, ?>> mappers() {
        List<IgniteExceptionMapper<?, ?>> mappers = new ArrayList<>();

        mappers.add(unchecked(QueryCancelledException.class, err -> new SqlException(err.traceId(), err.code(), err.getMessage(), err)));

        mappers.add(unchecked(RemoteFragmentExecutionException.class, err -> {
            if (err.groupCode() == SQL_ERR_GROUP.groupCode()) {
                return new SqlException(err.traceId(), err.code(), err.getMessage(), err);
            }

            return new IgniteException(err.traceId(), err.code(), err.getMessage(), err);
        }));

        mappers.add(unchecked(CalciteContextException.class,
                err -> new SqlException(STMT_VALIDATION_ERR, "Failed to validate query. " + err.getMessage(), err)));

        mappers.add(unchecked(CatalogValidationException.class,
                err -> new SqlException(err.traceId(), STMT_VALIDATION_ERR, "Failed to validate query. " + err.getMessage(), err)));

        return mappers;
    }
}
