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

package com.exactpro.th2.conn.amqp.client

import mu.KotlinLogging
import org.apache.qpid.jms.JmsConnection
import org.apache.qpid.jms.JmsConsumer
import org.apache.qpid.jms.JmsContext
import org.apache.qpid.jms.JmsProducer
import java.nio.charset.StandardCharsets
import java.util.Properties
import javax.annotation.concurrent.NotThreadSafe
import javax.jms.ConnectionFactory
import javax.jms.Destination
import javax.jms.JMSContext
import javax.jms.Message
import javax.jms.MessageListener
import javax.naming.InitialContext

typealias Config = Map<String, String>

@NotThreadSafe
class AmqpClient(config: Config, val errorReporter: (Throwable) -> Unit) : IClient {

    private val sessionAliasPropertyName = "sessionAlias"
    private val consumer: JmsConsumer
    private val jmsContext: JmsContext
    private val connection: JmsConnection
    private val producer: JmsProducer
    private val sendDestination: Destination
    private val receiveDestination: Destination
    private val sessionAlias: String

    init {
        val properties = Properties().apply { putAll(config) }
        InitialContext(properties).let { context ->
            val connectionFactory = context.lookup("factorylookup") as ConnectionFactory
            connection = connectionFactory.createConnection() as JmsConnection
            jmsContext = JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE)

            sendDestination = context.lookup("sendQueue") as Destination
            receiveDestination = context.lookup("receiveQueue") as Destination
        }

        sessionAlias = properties[sessionAliasPropertyName] as String
        jmsContext.start()
        LOGGER.info("Connected to amqp broker successfully")

        consumer = jmsContext.createConsumer(receiveDestination) as JmsConsumer
        LOGGER.info("Queue consumer created to read data form the Queue:  {}", receiveDestination)

        producer = jmsContext.createProducer() as JmsProducer
    }

    override fun setMessageListener(callback: (ByteArray) -> Unit) {
        LOGGER.debug("Set an asynchronous queue listener")
        consumer.messageListener = MessageListener { message: Message ->
            LOGGER.debug("Message received from the Queue:  {}", receiveDestination.toString())
            message.runCatching(Companion::toBytes).onSuccess(callback).onFailure {
                LOGGER.error(it) { "Error while processing incoming message" }
                errorReporter(it)
            }
            message.runCatching(Message::acknowledge).onFailure {
                LOGGER.error(it) { "Error while acknowledging received message" }
                errorReporter(it)
            }
        }
    }

    override fun send(data: MessageBody) {
        producer.send(sendDestination, String(data))
        LOGGER.info("Message sent successfully")
    }

    override fun stop() {
        consumer.close()
        jmsContext.close()
        LOGGER.info("Stopped amqp client")
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { AmqpClient::class.simpleName }
        private fun toBytes(message: Message): ByteArray = message.getBody(String::class.java).toByteArray(StandardCharsets.UTF_8)
    }

}