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

package org.apache.ignite.internal.catalog.sql;

import static org.apache.ignite.catalog.ColumnSorted.column;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.ignite.catalog.ColumnSorted;
import org.apache.ignite.catalog.SortOrder;

class IndexColumnImpl extends QueryPart {
    private static final Pattern SPACES = Pattern.compile("\\s+");

    private final ColumnSorted wrapped;

    public static IndexColumnImpl wrap(ColumnSorted column) {
        return new IndexColumnImpl(column);
    }

    private IndexColumnImpl(ColumnSorted wrapped) {
        this.wrapped = wrapped;
    }

    public static List<ColumnSorted> parseIndexColumnList(String columnList) {
        return QueryUtils.splitByComma(columnList).stream()
                .map(IndexColumnImpl::parseCol)
                .collect(Collectors.toList());
    }

    private static ColumnSorted parseCol(String columnRaw) {
        String[] split = SPACES.split(columnRaw, 2);
        String columnName = split[0].trim();
        if (split.length < 2) {
            return column(columnName);
        }
        SortOrder sortOrder = parseSortOrder(split[1].trim().toLowerCase());
        return column(columnName, sortOrder);
    }

    private static SortOrder parseSortOrder(String sortOrder) {
        String[] split = SPACES.split(sortOrder);
        if (split.length > 0) {
            boolean asc;
            switch (split[0].trim()) {
                case "asc":
                    asc = true;
                    break;
                case "desc":
                    asc = false;
                    break;
                case "nulls":
                    if (split.length > 1) {
                        String secondToken = split[1].trim();
                        if ("first".equals(secondToken)) {
                            return SortOrder.NULLS_FIRST;
                        } else if ("last".equals(secondToken)) {
                            return SortOrder.NULLS_LAST;
                        }
                    }
                    return SortOrder.DEFAULT;
                default:
                    return SortOrder.DEFAULT;
            }
            // We are here only if first token is "asc" or "desc"
            if (split.length > 2 && "nulls".equals(split[1].trim())) {
                String thirdToken = split[2].trim();
                if ("first".equals(thirdToken)) {
                    return asc ? SortOrder.ASC_NULLS_FIRST : SortOrder.DESC_NULLS_FIRST;
                } else if ("last".equals(thirdToken)) {
                    return asc ? SortOrder.ASC_NULLS_LAST : SortOrder.DESC_NULLS_LAST;
                }
            }
            return asc ? SortOrder.ASC : SortOrder.DESC;
        }
        return SortOrder.DEFAULT;
    }

    @Override
    protected void accept(QueryContext ctx) {
        ctx.visit(new Name(wrapped.columnName()));
        SortOrder sortOrder = wrapped.sortOrder();
        if (sortOrder != null && sortOrder != SortOrder.DEFAULT) {
            ctx.sql(" ").sql(sortOrder.sql());
        }
    }
}
