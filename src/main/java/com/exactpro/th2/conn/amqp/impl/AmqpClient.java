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

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import javax.annotation.concurrent.NotThreadSafe;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.qpid.jms.JmsConsumer;
import org.apache.qpid.jms.JmsContext;
import org.apache.qpid.jms.JmsProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class AmqpClient {
    private static Logger LOGGER = LoggerFactory.getLogger(AmqpClient.class);

    private final JmsConsumer consumer;
    private final JmsContext jmsContext;
    private final JmsProducer producer;
    private final Destination sendDestination;
    private final Destination receiveDestination;
    private final Consumer<Throwable> errorReporter;

    public AmqpClient(Map<String, String> config, Consumer<Throwable> errorReporter) throws NamingException {
        this.errorReporter = errorReporter;

        Properties properties = new Properties();
        properties.putAll(config);
        Context context = new InitialContext(properties);

        ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("factorylookup");
        jmsContext = (JmsContext) connectionFactory.createContext(JmsContext.CLIENT_ACKNOWLEDGE);

        jmsContext.start();

        LOGGER.info("Connected to amqp broker successfully");

        sendDestination = (Destination) context.lookup("sendQueue");
        receiveDestination = (Destination) context.lookup("receiveQueue");

        consumer = (JmsConsumer) jmsContext.createConsumer(receiveDestination);
        LOGGER.info("Queue consumer created to read data form the Queue:  {}", receiveDestination);

        // Lets create a queue producer and send the message
        producer = (JmsProducer) jmsContext.createProducer();
        // TODO: The producer doesn't relate to sendDestination. Maybe avoid information from log
        LOGGER.info("Queue producer created for the Queue:  {}", sendDestination);
    }

    public void setMessageListener(Consumer<byte[]> listener) {
        // Set an asynchronous queue listener
        consumer.setMessageListener(message ->
        {
            // TODO: conn-amqp should work with the base JMS message and convert it to bytes to send to th2
            TextMessage textMessage = (TextMessage) message;
            LOGGER.info("Message received form the Queue:  {}", receiveDestination);
            try {
                listener.accept(textMessage.getText().getBytes(Charset.defaultCharset()));
                textMessage.acknowledge();
                LOGGER.info("Message successfully processed and Acknowledged");
            } catch (Exception e) {
                errorReporter.accept(e);
            }
        });
    }

    public void send(byte[] data) {
        producer.send(sendDestination, data);
        LOGGER.info("Message sent successfully");
    }

    public void stop() {
        consumer.close();
        jmsContext.close();
        LOGGER.info("Stopped amqp client");
    }
}
