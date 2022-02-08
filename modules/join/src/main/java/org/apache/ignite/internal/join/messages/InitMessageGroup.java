/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.join.messages;

import org.apache.ignite.network.annotations.MessageGroup;

/**
 * Message Group for cluster initialization and CMG management.
 */
@MessageGroup(groupType = 6, groupName = "InitMessages")
public class InitMessageGroup {
    /**
     * Message type for {@link CmgInitMessage}.
     */
    public static final short CMG_INIT = 1;

    /**
     * Message type for {@link MetastorageInitMessage}.
     */
    public static final short METASTORAGE_INIT = 2;

    /**
     * Message type for {@link LeaderElectedMessage}.
     */
    public static final short LEADER_ELECTED = 3;

    /**
     * Message type for {@link InitErrorMessage}.
     */
    public static final short INIT_ERROR = 4;

    /**
     * Message type for {@link CancelInitMessage}.
     */
    public static final short CANCEL_INIT = 5;
}
