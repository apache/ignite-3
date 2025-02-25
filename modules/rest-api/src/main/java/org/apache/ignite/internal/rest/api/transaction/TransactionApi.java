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

package org.apache.ignite.internal.rest.api.transaction;

import static io.swagger.v3.oas.annotations.media.Schema.RequiredMode.REQUIRED;
import static org.apache.ignite.internal.rest.constants.MediaType.APPLICATION_JSON;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.rest.api.Problem;

/**
 * API for managing transactions.
 */
@Controller("/management/v1/transactions/")
@Tag(name = "transactions")
public interface TransactionApi {
    /**
     * Retrieves all in progress transactions.
     *
     *
     * @return A collection of all in progress transactions.
     */
    @Operation(summary = "Retrieve all in progress transactions", description = "Fetches all in progress transactions.")
    @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved in progress transactions.",
            content = @Content(mediaType = APPLICATION_JSON, array = @ArraySchema(schema = @Schema(implementation = TransactionInfo.class)))
    )
    @Get
    CompletableFuture<Collection<TransactionInfo>> transactions();

    /**
     * Retrieves transaction info of the specified transaction.
     *
     * @param transactionId The unique identifier of the transaction.
     * @return The transaction info of the specified transaction.
     */
    @Operation(summary = "Retrieve transaction state",
            description = "Fetches the current state of a specific transaction identified by transactionId.")
    @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved the transaction state.",
            content = @Content(mediaType = APPLICATION_JSON, schema = @Schema(implementation = TransactionInfo.class))
    )
    @ApiResponse(
            responseCode = "404",
            description = "Transaction not found.",
            content = @Content(mediaType = APPLICATION_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Get("/{transactionId}")
    CompletableFuture<TransactionInfo> transaction(@Schema(name = "transactionId",
            description = "The unique identifier of the transaction.", requiredMode = REQUIRED) UUID transactionId
    );

    /**
     * Kill a specific transaction identified by transactionId.
     *
     * @param transactionId The unique identifier of the transaction.
     * @return The result of the kill operation.
     */
    @Operation(summary = "Kill transaction.", description = "Kills a specific transaction identified by transactionId.")
    @ApiResponse(
            responseCode = "200",
            description = "Successfully killed the transaction.",
            content = @Content(mediaType = APPLICATION_JSON)
    )
    @ApiResponse(
            responseCode = "404",
            description = "Transaction not found.",
            content = @Content(mediaType = APPLICATION_JSON, schema = @Schema(implementation = Problem.class))
    )
    @ApiResponse(
            responseCode = "409",
            description = "Transaction is in an illegal state.",
            content = @Content(mediaType = APPLICATION_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Delete("/{transactionId}")
    CompletableFuture<Void> killTransaction(@Schema(name = "transactionId",
            description = "The unique identifier of the transaction.", requiredMode = REQUIRED) UUID transactionId
    );
}
