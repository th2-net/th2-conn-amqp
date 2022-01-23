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

package com.exactpro.th2.conn.amqp

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.common.message.toTimestamp

import com.exactpro.th2.common.schema.message.MessageRouter
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.check
import org.mockito.kotlin.verify
import java.time.Instant

class MessagePublisherTest {

    @Test
    internal fun `test publisher`() {
        val msg = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Msg></Msg>"
        val alias = "test-alias"

        val publisher = MessagePublisher(alias, 1000, mockRouter)
        val instant = Instant.now()
        val timestamp = instant.toTimestamp()
        publisher.onMessage(Direction.FIRST, MessageHolder(msg.toByteArray(), instant))
        Thread.sleep(2050)
        verify(mockRouter).sendAll(check {
            Assertions.assertEquals(1, it.messagesCount)
            val rawMessage = it.getMessages(0)
            Assertions.assertEquals(msg, rawMessage.body.toStringUtf8())
            Assertions.assertEquals(alias, rawMessage.metadata.id.connectionId.sessionAlias)
            Assertions.assertNotNull(rawMessage.metadata.timestamp)
            Assertions.assertEquals(timestamp, rawMessage.metadata.timestamp)
        }, check {
            Assertions.assertEquals("first", it)
        })
    }

    companion object {
        val mockRouter = mock<MessageRouter<RawMessageBatch>>()
    }

}
