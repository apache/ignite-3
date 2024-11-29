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

package org.apache.ignite.internal.network.scalecube;

import com.google.auto.service.AutoService;
import org.apache.ignite.internal.network.ChannelTypeModule;
import org.apache.ignite.internal.network.ChannelTypeRegistrar;

/** {@link ChannelTypeModule} for ScaleCube in network module. */
@AutoService(ChannelTypeModule.class)
public class ScaleCubeChannelTypeModule implements ChannelTypeModule {
    @Override
    public void register(ChannelTypeRegistrar channelTypeRegistrar) {
        channelTypeRegistrar.register(ScaleCubeDirectMarshallerTransport.SCALE_CUBE_CHANNEL_TYPE);
    }
}
