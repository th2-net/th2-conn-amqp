package com.exactpro.th2.conn.amqp

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.RawMessageBatch

import com.exactpro.th2.common.schema.message.MessageRouter
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.check
import org.mockito.kotlin.verify

class MessagePublisherTest {
    @Test
    internal fun `test publisher`() {
        val msg = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Msg></Msg>"

        val publisher = MessagePublisher("test-alias", 1000, mockRouter)
        publisher.onMessage(Direction.FIRST, MessageHolder(msg.toByteArray()))
        Thread.sleep(1050)
        verify(mockRouter).sendAll(check {
            Assertions.assertEquals(msg, it.getMessages(0).body.toStringUtf8())
        }, check {
            Assertions.assertEquals("first", it)
        })
    }

    companion object {
        val mockRouter  = mock<MessageRouter<RawMessageBatch>>()
    }
}
