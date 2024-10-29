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

package org.apache.ignite.internal.catalog;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import org.apache.ignite.catalog.annotations.Id;
import org.apache.ignite.catalog.annotations.Table;

/**
 * A POJO class with all supported column types.
 */
@Table
public final class AllColumnTypesPojo {
    @Id
    String str;
    Byte byte_col;
    Short short_col;
    Integer int_col;
    Long long_col;
    Float float_col;
    Double double_col;
    BigDecimal decimal_col;
    Boolean bool_col;
    byte[] bytes_col;
    UUID uuid_col;
    LocalDate date_col;
    LocalTime time_col;
    LocalDateTime datetime_col;
    Instant instant_col;
}
