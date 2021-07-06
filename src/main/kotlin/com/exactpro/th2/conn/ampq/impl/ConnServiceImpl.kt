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
import com.google.protobuf.TextFormat

class ConnServiceImpl(
    private val parameters: ConnParameters,
    onMessage: (Direction, MessageHolder) -> Unit,
    onEvent: (Event) -> Unit,
) : ConnService(onMessage, onEvent) {
    override fun start() {
        logger.info { "Starting the conn" }
        TODO("Not yet implemented")
    }

    override fun send(message: Message) {
        logger.debug { "Sending message: ${TextFormat.shortDebugString(message)}" }
        // do not forget to report the sent message
        // you need to convert them to its raw format and report using
        //
        // messageSent(MessageHolder(body, messageProperties/*if need*/))
        TODO("Not yet implemented")
    }

    override fun close() {
        logger.info { "Closing the conn" }
        TODO("Not yet implemented")
    }
}