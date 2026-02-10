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

package org.apache.ignite.data;

import java.util.Collections;
import java.util.Set;
import org.springframework.data.jdbc.core.dialect.JdbcArrayColumns;
import org.springframework.data.jdbc.core.dialect.JdbcDialect;
import org.springframework.data.relational.core.dialect.AbstractDialect;
import org.springframework.data.relational.core.dialect.LimitClause;
import org.springframework.data.relational.core.dialect.LockClause;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.IdentifierProcessing.LetterCasing;
import org.springframework.data.relational.core.sql.IdentifierProcessing.Quoting;
import org.springframework.data.relational.core.sql.LockOptions;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Implementation of Ignite-specific dialect.
 */
public class IgniteDialect extends AbstractDialect implements JdbcDialect {

    /**
     * Singleton instance.
     */
    public static final IgniteDialect INSTANCE = new IgniteDialect();

    private IgniteDialect() {}

    private static final LimitClause LIMIT_CLAUSE = new LimitClause() {
        @Override
        public String getLimit(long limit) {
            return "LIMIT " + limit;
        }

        @Override
        public String getOffset(long offset) {
            return "OFFSET " + offset;
        }

        @Override
        public String getLimitOffset(long limit, long offset) {
            return String.format("OFFSET %d ROWS FETCH FIRST %d ROWS ONLY", offset, limit);
        }

        @Override
        public Position getClausePosition() {
            return Position.AFTER_ORDER_BY;
        }
    };

    static class IgniteArrayColumns implements JdbcArrayColumns {
        @Override
        public boolean isSupported() {
            return true;
        }

        @Override
        public Class<?> getArrayType(Class<?> userType) {
            Assert.notNull(userType, "Array component type must not be null");

            return ClassUtils.resolvePrimitiveIfNecessary(userType);
        }
    }

    @Override
    public LimitClause limit() {
        return LIMIT_CLAUSE;
    }

    static final LockClause LOCK_CLAUSE = new LockClause() {

        @Override
        public String getLock(LockOptions lockOptions) {
            return "";
        }

        @Override
        public Position getClausePosition() {
            return Position.AFTER_ORDER_BY;
        }
    };

    @Override
    public LockClause lock() {
        return LOCK_CLAUSE;
    }

    private final IgniteArrayColumns arrayColumns = new IgniteArrayColumns();

    @Override
    public JdbcArrayColumns getArraySupport() {
        return arrayColumns;
    }

    @Override
    public IdentifierProcessing getIdentifierProcessing() {
        return new IdentifierProcessing() {
            @Override
            public String quote(String identifier) {
                return Quoting.ANSI.apply(identifier);
            }

            @Override
            public String standardizeLetterCase(String identifier) {
                return identifier.toUpperCase();
            }
        };
    }

    @Override
    public Set<Class<?>> simpleTypes() {
        return Collections.emptySet();
    }

    @Override
    public boolean supportsSingleQueryLoading() {
        return false;
    }
}
