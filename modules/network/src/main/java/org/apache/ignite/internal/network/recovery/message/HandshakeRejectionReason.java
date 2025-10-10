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

package org.apache.ignite.internal.network.recovery.message;

import java.lang.System.Logger.Level;
import org.apache.ignite.internal.logger.IgniteLogger;

/**
 * Reason for handshake rejection.
 */
public enum HandshakeRejectionReason {
    /** The sender is stopping. */
    STOPPING(Level.DEBUG),

    /**
     * An attempt to establish a connection to itself. This should never happen and indicates a programming error.
     */
    LOOP(Level.INFO),

    /**
     * The sender has detected that the counterpart launch ID is stale (was earlier used to establish a connection). After this is received
     * it makes no sense to retry connections with same node identity (launch ID must be changed to make a retry).
     */
    STALE_LAUNCH_ID(Level.WARNING),

    /** The sender has detected a clinch and decided to terminate this handshake in favor of the competitor. */
    CLINCH(Level.DEBUG),

    /**
     * Cluster ID of the sender does not match the cluster ID of the counterpart.
     */
    CLUSTER_ID_MISMATCH(Level.WARNING),

    /**
     * Product (Ignite or a derivative product) of the sender does not match the product of the counterpart.
     */
    PRODUCT_MISMATCH(Level.WARNING),

    /**
     * Version of the sender product does not match the version of the counterpart.
     */
    VERSION_MISMATCH(Level.WARNING);

    HandshakeRejectionReason(Level logLevel) {
        this.logLevel = logLevel;
    }

    private final Level logLevel;

    /**
     * Prints a message at an appropriate log level. If the node is stopping, the message is always printed at DEBUG level to avoid
     * noisy logs during shutdown.
     *
     * @param nodeStopping Whether the node is stopping.
     * @param logger Logger to use.
     * @param message Message with optional format specifiers.
     * @param args Arguments referenced by the format specifiers in the message.
     */
    public void print(boolean nodeStopping, IgniteLogger logger, String message, Object... args) {
        if (nodeStopping) {
            logger.debug(message, args);
        } else {
            switch (logLevel) {
                case INFO:
                    logger.info(message, args);
                    break;

                case WARNING:
                    logger.warn(message, args);
                    break;

                case ERROR:
                    logger.error(message, args);
                    break;

                case DEBUG:
                    logger.debug(message, args);
                    break;

                default:
                    throw new IllegalStateException(logLevel.toString());
            }
        }
    }
}
