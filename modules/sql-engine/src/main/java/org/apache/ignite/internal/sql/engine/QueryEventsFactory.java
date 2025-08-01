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

import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.util.Map;
import org.apache.ignite.internal.eventlog.api.Event;
import org.apache.ignite.internal.eventlog.api.IgniteEventType;
import org.apache.ignite.internal.eventlog.event.EventUser;
import org.apache.ignite.internal.sql.engine.exec.fsm.QueryInfo;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * SQL query event factory.
 */
public class QueryEventsFactory {
    private final String localNode;

    public QueryEventsFactory(String localNode) {
        this.localNode = localNode;
    }

    /** Creates new {@link IgniteEventType#QUERY_STARTED} event. */
    public Event makeStartEvent(QueryInfo queryInfo, EventUser user) {
        Map<String, Object> fields = IgniteUtils.newLinkedHashMap(7);

        fillCommonFields(fields, queryInfo);

        return IgniteEventType.QUERY_STARTED.builder()
                .user(user)
                .timestamp(queryInfo.startTime().toEpochMilli())
                .fields(fields)
                .build();
    }

    /** Creates new {@link IgniteEventType#QUERY_FINISHED} event. */
    public Event makeFinishEvent(QueryInfo queryInfo, EventUser user, long finishTime) {
        Map<String, Object> fields = IgniteUtils.newLinkedHashMap(10);

        fillCommonFields(fields, queryInfo);

        fields.put(FieldNames.START_TIME, queryInfo.startTime().toEpochMilli());

        SqlQueryType queryType = queryInfo.queryType();

        fields.put(FieldNames.TYPE, queryType == null ? null : queryType.name());

        Throwable error = queryInfo.error();

        fields.put(FieldNames.ERROR, error == null ? null : unwrapCause(error).getMessage());

        return IgniteEventType.QUERY_FINISHED.builder()
                .user(user)
                .timestamp(finishTime)
                .fields(fields)
                .build();
    }

    private void fillCommonFields(Map<String, Object> fields, QueryInfo queryInfo) {
        fields.put(FieldNames.INITIATOR, localNode);
        fields.put(FieldNames.ID, queryInfo.id());
        fields.put(FieldNames.SCHEMA, queryInfo.schema());
        fields.put(FieldNames.SQL, queryInfo.sql());
        fields.put(FieldNames.PARENT_ID, queryInfo.parentId());
        fields.put(FieldNames.STATEMENT_NUMBER, queryInfo.statementNum());
        fields.put(FieldNames.TX_ID, queryInfo.transactionId());
    }

    /** Query events field names. */
    static class FieldNames {
        // Common fields.
        static final String INITIATOR = "initiatorNode";
        static final String ID = "id";
        static final String SCHEMA = "schema";
        static final String SQL = "sql";
        static final String PARENT_ID = "parentId";
        static final String STATEMENT_NUMBER = "statementNum";
        static final String TX_ID = "transactionId";

        // Finish event fields.
        static final String START_TIME = "startTime";
        static final String TYPE = "type";
        static final String ERROR = "errorMessage";
    }
}
