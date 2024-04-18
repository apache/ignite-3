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

package org.apache.ignite.internal.lowwatermark.message;

import static org.apache.ignite.internal.lowwatermark.message.LowWatermarkMessageGroup.GET_LOW_WATERMARK_RESPONSE;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;

/** Response to {@link GetLowWatermarkRequest}. */
@Transferable(GET_LOW_WATERMARK_RESPONSE)
public interface GetLowWatermarkResponse extends NetworkMessage {
    /** Returns the low watermark, {@link HybridTimestamp#NULL_HYBRID_TIMESTAMP} means no low watermark has been assigned yet. */
    long lowWatermark();
}
