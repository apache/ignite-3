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

package org.apache.ignite.internal.sql.engine.message.field;

import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessageGroup;

/**
 * A message that contains a single {@link Short} value.
 */
@Transferable(SqlQueryMessageGroup.SHORT_FIELD_MESSAGE)
public interface ShortValueMessage extends SingleValueMessage<Short> {
    @Override
    Short value();
}
