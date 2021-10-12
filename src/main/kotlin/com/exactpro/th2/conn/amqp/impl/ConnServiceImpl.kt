/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.conn.amqp.impl

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.conn.amqp.ConnService
import com.exactpro.th2.conn.amqp.MessageHolder
import com.google.protobuf.TextFormat
import java.time.Instant
import javax.naming.Context

class ConnServiceImpl(
    private val parameters: ConnParameters,
    onMessage: (Direction, MessageHolder) -> Unit,
    onEvent: (Event) -> Unit
) : ConnService(onMessage, onEvent) {

    private lateinit var client: AmqpClient

    private fun toMap(parameters: ConnParameters): Map<String, String?> {
        val environmentDetails = HashMap<String, String?>()
        environmentDetails[Context.INITIAL_CONTEXT_FACTORY] = parameters.initialContextFactory
        environmentDetails["connectionfactory.factorylookup"] = parameters.factorylookup
        environmentDetails["queue.sendQueue"] = parameters.sendQueue
        environmentDetails["queue.receiveQueue"] = parameters.receiveQueue
        environmentDetails["queue.replyTo"] = parameters.replyTo
        environmentDetails["contentType"] = parameters.contentType
        return environmentDetails
    }

    override fun start() {
        logger.info { "Starting the conn" }
        val errorReporter : (Exception) -> Unit = {e -> reportError(e, {}) }
        val config : Map<String, String?> = toMap(parameters)
        client = AmqpClient(config, errorReporter)

        val listener : (ByteArray) -> Unit = { bytes -> messageReceived(MessageHolder(bytes, Instant.now()))}
        client.setMessageListener(listener)
    }

    fun start(client: AmqpClient) {
        logger.info { "Starting the conn" }
        this.client = client

        val listener : (ByteArray) -> Unit = { bytes -> messageReceived(MessageHolder(bytes, Instant.now()))}
        client.setMessageListener(listener)
    }

    override fun send(message: RawMessage) {
        logger.debug { "Sending message: ${TextFormat.shortDebugString(message)}" }
        val bytes = message.body.toByteArray()
        client.send(bytes)
        messageSent(MessageHolder(bytes, Instant.now()))
    }

    override fun close() {
        logger.info { "Closing the conn" }
        client.stop()
    }
}