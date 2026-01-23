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

package org.apache.ignite.internal.sql.engine.message;

import static org.apache.ignite.internal.sql.engine.message.SqlQueryMessageGroup.GROUP_TYPE;

import org.apache.ignite.internal.network.annotations.MessageGroup;
import org.apache.ignite.internal.sql.engine.message.field.BooleanValueMessage;
import org.apache.ignite.internal.sql.engine.message.field.ByteArrayValueMessage;
import org.apache.ignite.internal.sql.engine.message.field.ByteValueMessage;
import org.apache.ignite.internal.sql.engine.message.field.DecimalValueMessage;
import org.apache.ignite.internal.sql.engine.message.field.DoubleValueMessage;
import org.apache.ignite.internal.sql.engine.message.field.FloatValueMessage;
import org.apache.ignite.internal.sql.engine.message.field.IntValueMessage;
import org.apache.ignite.internal.sql.engine.message.field.LongValueMessage;
import org.apache.ignite.internal.sql.engine.message.field.NullValueMessage;
import org.apache.ignite.internal.sql.engine.message.field.ShortValueMessage;
import org.apache.ignite.internal.sql.engine.message.field.StringValueMessage;
import org.apache.ignite.internal.sql.engine.message.field.UuidValueMessage;

/**
 * Message types for the sql query processing module.
 */
@MessageGroup(groupType = GROUP_TYPE, groupName = "SqlQueryMessages")
public final class SqlQueryMessageGroup {
    public static final short GROUP_TYPE = 4;

    public static final short QUERY_START_REQUEST = 0;

    public static final short QUERY_START_RESPONSE = 1;

    public static final short ERROR_MESSAGE = 2;

    public static final short QUERY_BATCH_MESSAGE = 3;

    /** See {@link QueryBatchRequestMessage} for details. */
    public static final short QUERY_BATCH_REQUEST = 4;

    public static final short QUERY_CLOSE_MESSAGE = 5;

    /** See {@link CancelOperationRequest} for the details. */
    public static final short OPERATION_CANCEL_REQUEST = 6;

    /** See {@link CancelOperationResponse} for the details. */
    public static final short OPERATION_CANCEL_RESPONSE = 7;

    /** See {@link SharedStateMessage} for the details. */
    public static final short SHARED_STATE_MESSAGE = 8;

    /** See {@link BooleanValueMessage} for the details. */
    public static final short BOOLEAN_FIELD_MESSAGE = 9;

    /** See {@link ByteValueMessage} for the details. */
    public static final short BYTE_FIELD_MESSAGE = 10;

    /** See {@link ShortValueMessage} for the details. */
    public static final short SHORT_FIELD_MESSAGE = 11;

    /** See {@link IntValueMessage} for the details. */
    public static final short INT_FIELD_MESSAGE = 12;

    /** See {@link LongValueMessage} for the details. */
    public static final short LONG_FIELD_MESSAGE = 13;

    /** See {@link FloatValueMessage} for the details. */
    public static final short FLOAT_FIELD_MESSAGE = 14;

    /** See {@link DoubleValueMessage} for the details. */
    public static final short DOUBLE_FIELD_MESSAGE = 15;

    /** See {@link DecimalValueMessage} for the details. */
    public static final short DECIMAL_FIELD_MESSAGE = 16;

    /** See {@link UuidValueMessage} for the details. */
    public static final short UUID_FIELD_MESSAGE = 17;

    /** See {@link StringValueMessage} for the details. */
    public static final short STRING_FIELD_MESSAGE = 18;

    /** See {@link NullValueMessage} for the details. */
    public static final short NULL_FIELD_MESSAGE = 19;

    /** See {@link ByteArrayValueMessage} for the details. */
    public static final short BYTE_ARRAY_FIELD_MESSAGE = 20;
}
