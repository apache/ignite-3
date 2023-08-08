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

package org.apache.ignite.internal.network.file.exception;

import org.apache.ignite.internal.network.file.messages.Identifier;
import org.apache.ignite.lang.ErrorGroups.Network;
import org.apache.ignite.lang.IgniteException;

/**
 * Exception thrown when file provider is not found for a metadata message.
 */
public class FileProviderNotFoundException extends IgniteException {
    /**
     * Constructor.
     *
     * @param message Message.
     */
    public FileProviderNotFoundException(Class<? extends Identifier> message) {
        super(Network.FILE_PROVIDER_NOT_FOUND_ERR, "File provider not found for message: " + message.getName());
    }
}
