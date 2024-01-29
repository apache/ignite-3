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

package org.apache.ignite.lang;

import static org.apache.ignite.lang.ErrorGroups.Common.CURSOR_NO_MORE_PAGES_ERR;

/**
 * Exception is thrown when there are no more pages in the cursor.
 */
public class NoMorePagesException extends IgniteException {
    /**
     * Creates an exception instance.
     */
    public NoMorePagesException() {
        super(CURSOR_NO_MORE_PAGES_ERR, "There are no more pages.");
    }
}
