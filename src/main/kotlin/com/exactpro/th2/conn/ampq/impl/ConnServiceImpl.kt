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

package com.exactpro.th2.conn.ampq.impl

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.conn.ampq.ConnService
import com.exactpro.th2.conn.ampq.MessageHolder
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.TextFormat

class ConnServiceImpl(
    private val parameters: ConnParameters,
    onMessage: (Direction, MessageHolder) -> Unit,
    onEvent: (Event) -> Unit,
) : ConnService(onMessage, onEvent) {

    private lateinit var client: AmqpClient

    override fun start() {
        logger.info { "Starting the conn" }
        val errorReporter : (Throwable) -> Unit = {}
        val config : Map<String, String> = ObjectMapper().convertValue(parameters, object: TypeReference<Map<String, String>>(){})
        client = AmqpClient(config, errorReporter)

        client.start()

        val listener : (ByteArray) -> Unit = { bytes -> messageReceived(MessageHolder(bytes))}
        client.setMessageListener(listener)
    }

    override fun send(message: Message) {
        logger.debug { "Sending message: ${TextFormat.shortDebugString(message)}" }
        val bytes = message.toByteArray()
        client.send(bytes)
        messageSent(MessageHolder(bytes))
    }

    override fun close() {
        logger.info { "Closing the conn" }
        client.stop();
    }
}