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

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.conn.amqp.configuration.ConnParameters
import com.exactpro.th2.conn.amqp.connservice.ConnServiceImpl
import com.exactpro.th2.conn.amqp.client.IClient
import com.google.protobuf.ByteString
import mu.KotlinLogging
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.check
import org.mockito.kotlin.verify

class ConnServiceTest {

    @Test
    internal fun `test conn service`() {
        val xmlText = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Msg></Msg>"
        val conParams = ConnParameters(
            "",
            "",
            "",
            ""
        )

        val onEvent = { event: Event ->
            LOGGER.info { "Event: $event was sent" }
        }

        val connService = ConnServiceImpl(
            conParams,
            onMessage,
            onEvent
        )

        connService.start(amqpClient)
        connService.send(RawMessage.newBuilder().apply {
            body = ByteString.copyFrom(xmlText.toByteArray())
        }.build())

        verify(onMessage)(check {
            Assertions.assertEquals(Direction.SECOND, it)
        }, check {
            Assertions.assertEquals(xmlText, it.body.decodeToString())
            Assertions.assertNotNull(it.sendTime)
        })
    }

    companion object {
        val amqpClient = mock<IClient>()
        val onMessage = mock<(Direction, MessageHolder) -> Unit>()
        private val LOGGER = KotlinLogging.logger { }

    }
}