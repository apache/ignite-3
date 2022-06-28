/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.lang;

public class CommonErrors {
    /** Unknown error group. */
    public static final ErrorGroup UNKNOWN_ERR_GROUP = ErrorGroup.newGroup("UNK", 1);

    /** Common error group. */
    public static final ErrorGroup COMMON_ERR_GROUP = ErrorGroup.newGroup("CMN", 2);

    /** Table error group. */
    public static final ErrorGroup TABLE_ERR_GROUP = ErrorGroup.newGroup("TBL", 3);

    /** Unknown error. */
    public static int UNKNOWN_ERR = UNKNOWN_ERR_GROUP.registerErrorCode(1);

    /** Table already exists. */
    public static int TABLE_ALREADY_EXISTS_ERR = TABLE_ERR_GROUP.registerErrorCode(1);
    /** Table not found. */
    public static int TABLE_NOT_FOUND_ERR = TABLE_ERR_GROUP.registerErrorCode(2);
    /** Column already exists. */
    public static int COLUMN_ALREADY_EXISTS_ERR = TABLE_ERR_GROUP.registerErrorCode(3);
    /** Column not found. */
    public static int COLUMN_NOT_FOUND_ERR = TABLE_ERR_GROUP.registerErrorCode(4);
}
