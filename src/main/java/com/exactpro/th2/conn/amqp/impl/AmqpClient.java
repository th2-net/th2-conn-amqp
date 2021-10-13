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

package com.exactpro.th2.conn.amqp.impl;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import javax.annotation.concurrent.NotThreadSafe;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConsumer;
import org.apache.qpid.jms.JmsContext;
import org.apache.qpid.jms.JmsProducer;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.message.JmsMessageTransformation;
import org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;

@NotThreadSafe
public class AmqpClient {
    private static Logger LOGGER = LoggerFactory.getLogger(AmqpClient.class);

    private final JmsConsumer consumer;
    private final JmsContext jmsContext;
    private final JmsConnection connection;
    private final JmsProducer producer;
    private final Destination sendDestination;
    private final Destination receiveDestination;
    private final Consumer<Exception> errorReporter;

    public AmqpClient(Map<String, String> config, Consumer<Exception> errorReporter) throws NamingException, JMSException {
        this.errorReporter = errorReporter;

        Properties properties = new Properties();
        properties.putAll(config);
        Context context = new InitialContext(properties);

        ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("factorylookup");
        connection = (JmsConnection) connectionFactory.createConnection();
        jmsContext = new JmsContext(connection, JMSContext.CLIENT_ACKNOWLEDGE);

        jmsContext.start();

        LOGGER.info("Connected to amqp broker successfully");

        sendDestination = (Destination) context.lookup("sendQueue");
        receiveDestination = (Destination) context.lookup("receiveQueue");

        consumer = (JmsConsumer) jmsContext.createConsumer(receiveDestination);
        LOGGER.info("Queue consumer created to read data form the Queue:  {}", receiveDestination);

        // Lets create a queue producer and send the message
        producer = (JmsProducer) jmsContext.createProducer();
    }

    public void setMessageListener(Consumer<byte[]> listener) {
        // Set an asynchronous queue listener
        consumer.setMessageListener(message ->
        {
            LOGGER.info("Message received from the Queue:  {}", receiveDestination);
            byte[] bytes = null;
            try {
                bytes = toBytes(message);
            } catch (JMSException e) {
                LOGGER.error("Error while getting bytes of the received message", e);
                errorReporter.accept(e);
            }
            listener.accept(bytes);
            try {
                message.acknowledge();
                LOGGER.info("Message successfully processed and Acknowledged");
            } catch (JMSException e) {
                LOGGER.error("Error while acknowledging received message", e);
                errorReporter.accept(e);
            }
        });
    }

    public void send(byte[] data) {
        producer.send(sendDestination, new String(data));
        LOGGER.info("Message sent successfully");
    }

    public void stop() {
        consumer.close();
        jmsContext.close();
        LOGGER.info("Stopped amqp client");
    }

    private byte[] toBytes(Message message) throws JMSException {
        String body = message.getBody(String.class);
        LOGGER.debug("Received message: {}", body);
//        JmsMessage jmsMessage = JmsMessageTransformation.transformMessage(connection, message);
//        ByteBuf buffer = ((AmqpJmsMessageFacade) jmsMessage.getFacade()).encodeMessage();
        return body.getBytes(StandardCharsets.UTF_8);
    }
}
