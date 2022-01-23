/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.conn.amqp.connservice

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.conn.amqp.MessageHolder
import mu.KotlinLogging
import java.io.IOException

abstract class ConnService(
    private val onMessage: (Direction, MessageHolder) -> Unit,
    private val onEvent: (Event) -> Unit
) : AutoCloseable {
    protected val logger = KotlinLogging.logger { ConnService::class.simpleName }

    /**
     * The method will be called when the box is initializing.
     * Connection to the system should be performed in that method.
     */
    @Throws(IOException::class)
    abstract fun start()

    /**
     * Sends the specified message to the system
     *
     * @param message message to send
     */
    @Throws(IOException::class)
    abstract fun send(message: RawMessage)

    protected fun messageSent(holder: MessageHolder) = onMessage(Direction.SECOND, holder)

    protected fun messageReceived(holder: MessageHolder) {
        logger.debug { "Received bytes: ${holder.body}" }
        onMessage(Direction.FIRST, holder)
    }

    protected fun reportError(ex: Throwable? = null, block: Event.() -> Unit, vararg relatedMessages: MessageID) {
        Event.start().endTimestamp().apply {
            status(Event.Status.FAILED)
            var curEx: Throwable? = ex
            while (curEx != null) {
                bodyData(EventUtils.createMessageBean(curEx.message))
                curEx = curEx.cause
            }
            relatedMessages.forEach(this::messageID)
        }.apply(block).apply(onEvent)
    }
}