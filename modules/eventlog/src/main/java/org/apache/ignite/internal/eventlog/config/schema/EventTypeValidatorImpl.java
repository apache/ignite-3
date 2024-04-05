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

package org.apache.ignite.internal.eventlog.config.schema;

import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.eventlog.event.EventTypeRegistry;

/** Validates event types. */
public class EventTypeValidatorImpl implements Validator<ValidEventType, String[]> {
    /** Static instance. */
    public static final EventTypeValidatorImpl INSTANCE = new EventTypeValidatorImpl();

    /** {@inheritDoc} */
    @Override
    public void validate(ValidEventType annotation, ValidationContext<String[]> ctx) {
        String[] eventTypes = ctx.getNewValue();
        if (eventTypes.length == 1 && eventTypes[0].isEmpty()) {
            return;
        }

        for (String eventType : eventTypes) {
            if (!EventTypeRegistry.contains(eventType)) {
                ctx.addIssue(new ValidationIssue(
                        ctx.currentKey(),
                        String.format(
                                "Unable to find event type '%s' in the system. Please, make sure you set the correct event type.",
                                eventType
                        )));
            }
        }
    }
}
