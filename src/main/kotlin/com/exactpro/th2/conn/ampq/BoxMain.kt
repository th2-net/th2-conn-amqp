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

@file:JvmName("BoxMain")

package com.exactpro.th2.conn.ampq

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.MessageBatch
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.common.metrics.liveness
import com.exactpro.th2.common.metrics.readiness
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.conn.ampq.impl.Configuration
import com.exactpro.th2.conn.ampq.impl.ConnServiceImpl
import mu.KotlinLogging
import java.time.Instant
import java.util.Deque
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.system.exitProcess

private val LOGGER = KotlinLogging.logger { }

fun main(args: Array<String>) {
    LOGGER.info { "Starting the box" }
    // Here is an entry point to the th2-box.

    // Configure shutdown hook for closing all resources
    // and the lock condition to await termination.
    //
    // If you use the logic that doesn't require additional threads
    // and you can run everything on main thread
    // you can omit the part with locks (but please keep the resources queue)
    val resources: Deque<AutoCloseable> = ConcurrentLinkedDeque()
    val lock = ReentrantLock()
    val condition: Condition = lock.newCondition()
    configureShutdownHook(resources, lock, condition)

    try {
        // You need to initialize the CommonFactory

        // You can use custom paths to each config that is required for the CommonFactory
        // If args are empty the default path will be chosen.
        val factory = CommonFactory.createFromArguments(*args)
        // do not forget to add resource to the resources queue
        resources += factory

        // The BOX is alive
        liveness = true
        val configuration = factory.getCustomConfiguration(Configuration::class.java)

        val eventRouter = factory.eventBatchRouter
        val rootEvent = Event.start().endTimestamp()
            .status(Event.Status.PASSED)
            .name("${configuration.rootEventName}_${Instant.now()}")
            .type("Microservice")
            .bodyData(EventUtils.createMessageBean("Root event for the conn box"))

        eventRouter.send(
            EventBatch.newBuilder()
                .addEvents(rootEvent.toProtoEvent(null))
                .build()
        )

        // Do additional initialization required to your logic
        val rawRouter: MessageRouter<RawMessageBatch> = factory.messageRouterRawBatch
        val parsedRouter: MessageRouter<MessageBatch> = factory.messageRouterParsedBatch

        val publisher = MessagePublisher(
            configuration.sessionAlias,
            configuration.drainIntervalMills,
            rawRouter
        )
        resources += publisher

        val service: ConnService = ConnServiceImpl(
            parameters = configuration.parameters,
            onMessage = publisher::onMessage,
            onEvent = { event ->
                eventRouter.safeSend(event, rootEvent.id)
            }
        )
        resources += service

        parsedRouter.subscribeAll { _, parsedBatch ->
            parsedBatch.messagesList.forEach { msg ->
                runCatching { service.send(msg) }
                    .onFailure {
                        eventRouter.safeSend(
                            Event.start().endTimestamp()
                                .status(Event.Status.FAILED)
                                .type("SendError")
                                .name("Cannot send message ${msg.metadata.messageType}")
                                .apply {
                                    var ex: Throwable? = it
                                    do {
                                        ex?.apply { bodyData(EventUtils.createMessageBean(message)) }
                                        ex = ex?.cause
                                    } while (ex != null)
                                },
                            if (msg.hasParentEventId()) msg.parentEventId.id else rootEvent.id
                        )
                    }
            }
        }

        service.start()

        // The BOX is ready to work
        readiness = true

        awaitShutdown(lock, condition)
    } catch (ex: Exception) {
        LOGGER.error(ex) { "Cannot start the box" }
        exitProcess(1)
    }
}

private fun MessageRouter<EventBatch>.safeSend(event: Event, parentId: String?) {
    runCatching { send(EventBatch.newBuilder().addEvents(event.toProtoEvent(parentId)).build()) }
        .onFailure { LOGGER.error(it) { "Cannot send event with id ${event.id}" } }
}

private fun configureShutdownHook(resources: Deque<AutoCloseable>, lock: ReentrantLock, condition: Condition) {
    Runtime.getRuntime().addShutdownHook(thread(
        start = false,
        name = "Shutdown hook"
    ) {
        LOGGER.info { "Shutdown start" }
        readiness = false
        try {
            lock.lock()
            condition.signalAll()
        } finally {
            lock.unlock()
        }
        resources.descendingIterator().forEachRemaining { resource ->
            try {
                resource.close()
            } catch (e: Exception) {
                LOGGER.error(e) { "Cannot close resource ${resource::class}" }
            }
        }
        liveness = false
        LOGGER.info { "Shutdown end" }
    })
}

@Throws(InterruptedException::class)
private fun awaitShutdown(lock: ReentrantLock, condition: Condition) {
    try {
        lock.lock()
        LOGGER.info { "Wait shutdown" }
        condition.await()
        LOGGER.info { "App shutdown" }
    } finally {
        lock.unlock()
    }
}