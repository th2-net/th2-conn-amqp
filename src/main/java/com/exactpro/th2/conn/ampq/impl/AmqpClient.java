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

package com.exactpro.th2.conn.ampq.impl;

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
    private static Logger logger = LoggerFactory.getLogger(AmqpClient.class);

    private JmsConsumer consumer;
    private JmsContext jmsContext;
    private JmsProducer producer;
    private Destination sendDestination;
    private Destination receiveDestination;
    private final Map<String, String> config;
    private final Consumer<Throwable> errorReporter;

    public AmqpClient(Map<String, String> config, Consumer<Throwable> errorReporter) {
        this.config = config;
        this.errorReporter = errorReporter;
    }

    public void start() throws NamingException {

        Properties properties = new Properties();
        properties.putAll(config);
        Context context = new InitialContext(properties);

        ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("factorylookup");
        jmsContext = (JmsContext) connectionFactory.createContext(JmsContext.CLIENT_ACKNOWLEDGE);

        jmsContext.start();

        logger.info("Connected to amqp broker successfully");

        sendDestination = (Destination) context.lookup("sendQueue");
        receiveDestination = (Destination) context.lookup("receiveQueue");

        consumer = (JmsConsumer) jmsContext.createConsumer(receiveDestination);
        logger.info("Queue consumer created to read data form the Queue:  {}", receiveDestination);

        // Lets create a queue producer and send the message
        producer = (JmsProducer) jmsContext.createProducer();
        logger.info("Queue producer created for the Queue:  {}", sendDestination);
    }

    public void setMessageListener(Consumer<byte[]> listener) {
         // Set an asynchronous queue listener
        consumer.setMessageListener(message ->
        {
            TextMessage textMessage = (TextMessage) message;
            logger.info("Message received form the Queue:  {}", receiveDestination);
            try {
                listener.accept(textMessage.getText().getBytes(Charset.defaultCharset()));
                textMessage.acknowledge();
                logger.info("Message successfully processed and Acknowledged");
            } catch (Exception e) {
                errorReporter.accept(e);
            }
        });
    }

    public void send(byte[] data) {
        producer.send(sendDestination, new String(data, Charset.defaultCharset()));
        logger.info("Message sent successfully");
    }

    public void stop() {
        consumer.close();
        jmsContext.close();
        logger.info("Stopped amqp client");
    }
}
