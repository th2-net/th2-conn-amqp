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

@file:JvmName("BoxMain")

package com.exactpro.th2.conn.amqp

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.common.metrics.liveness
import com.exactpro.th2.common.metrics.readiness
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.conn.amqp.configuration.Configuration
import com.exactpro.th2.conn.amqp.configuration.ConnParameters
import com.exactpro.th2.conn.amqp.connservice.ConnService
import com.exactpro.th2.conn.amqp.connservice.ConnServiceImpl
import mu.KotlinLogging
import java.time.Instant
import java.util.Deque
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.system.exitProcess

private val LOGGER = KotlinLogging.logger { }

fun main(args: Array<String>) {
    LOGGER.info { "Starting the box" }
    val resources: Deque<AutoCloseable> = ConcurrentLinkedDeque()
    val lock = ReentrantLock()
    val condition: Condition = lock.newCondition()
    configureShutdownHook(resources, lock, condition)

    try {
        val factory = CommonFactory.createFromArguments(*args)
        resources += factory
        liveness = true

        val configuration = factory.getCustomConfiguration(Configuration::class.java)

        val eventRouter = factory.eventBatchRouter
        val rootEvent = Event.start().endTimestamp()
            .status(Event.Status.PASSED)
            .name("${configuration.rootEventName}_${Instant.now()}")
            .type("Microservice")
            .bodyData(EventUtils.createMessageBean("Root event for the amqp conn box"))

        eventRouter.send(
            EventBatch.newBuilder()
                .addEvents(rootEvent.toProtoEvent(null))
                .build()
        )

        configureConnectionParameters(configuration, eventRouter, rootEvent)

        val rawRouter: MessageRouter<RawMessageBatch> = factory.messageRouterRawBatch
        val aliasToService = HashMap<String, ConnService>()
        val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
        configuration.sessions?.forEach { connParameters ->
            run {
                val publisher = MessagePublisher(
                    connParameters.sessionAlias,
                    configuration.drainIntervalMills,
                    rawRouter,
                    executor
                )
                resources += publisher

                val service: ConnService = ConnServiceImpl(
                    parameters = connParameters,
                    onMessage = publisher::onMessage,
                    onEvent = { event ->

                        eventRouter.safeSend(event, rootEvent.id)
                    }
                )
                resources += service
                aliasToService[connParameters.sessionAlias] = service
                service.start()
            }
        } ?: error("Connection parameters can't be blank. Use {sessions} or {parameters + sessioAlias}")
        rawRouter.subscribeAll { _, rawBatch ->
            rawBatch.messagesList.forEach { msg ->
                msg.runCatching {
                    val alias = msg.metadata.id.connectionId.sessionAlias
                    aliasToService.getOrElse(
                        alias,
                        { error("Can't find service by alias {$alias}") }
                    ).send(this)
                }.onFailure {
                    eventRouter.safeSend(
                        Event.start().endTimestamp()
                            .status(Event.Status.FAILED)
                            .type("SendError")
                            .name("Cannot send message: ${msg.metadata}")
                            .apply {
                                var ex: Throwable? = it
                                while (ex != null) {
                                    bodyData(EventUtils.createMessageBean(ex.message))
                                    ex = ex.cause
                                }
                            },
                        if (msg.hasParentEventId()) msg.parentEventId.id else rootEvent.id
                    )
                }.onSuccess {
                    if (configuration.enableMessageSendingEvent) {
                        eventRouter.safeSend(
                            Event.start().endTimestamp()
                                .status(Event.Status.PASSED)
                                .type("Message")
                                .name("Message was sent:  ${msg.metadata.id.sequence}")
                                .apply {
                                    messageID(msg.metadata.id)
                                },
                            if (msg.hasParentEventId()) msg.parentEventId.id else rootEvent.id
                        )
                    }
                }
            }
        }
        readiness = true

        awaitShutdown(lock, condition)
    } catch (ex: Exception) {
        LOGGER.error(ex) { "Cannot start the box" }
        exitProcess(1)
    }
}

private fun configureConnectionParameters(
    configuration: Configuration,
    eventRouter: MessageRouter<EventBatch>,
    rootEvent: Event
) {
    when {
        configuration.parameters != null && configuration.sessionAlias == null -> {
            sendError(
                eventRouter,
                "The connection {parameters} configuration requires {sessionAlias}. Please specify the option or use the {sessions} configuration instead",
                rootEvent
            )
        }
        configuration.sessions != null && configuration.parameters != null ->
            sendError(
                eventRouter,
                "Configuration can't contain both connection version. It must be {sessions} or {parameters}",
                rootEvent
            )

        configuration.parameters != null && configuration.sessions == null && configuration.sessionAlias != null -> {
            (configuration.sessions as ArrayList<ConnParameters>).plusAssign(
                ConnParameters(
                    sessionAlias = configuration.sessionAlias,
                    initialContextFactory = configuration.parameters.initialContextFactory,
                    factorylookup = configuration.parameters.factorylookup,
                    sendQueue = configuration.parameters.sendQueue,
                    receiveQueue = configuration.parameters.receiveQueue
                )
            )
        }
    }
}

private fun sendError(eventRouter: MessageRouter<EventBatch>, reason: String, rootEvent: Event) {
    eventRouter.safeSend(
        Event.start().endTimestamp()
            .status(Event.Status.FAILED)
            .type("Error")
            .name(reason),
        rootEvent.id
    )
    error(reason)
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
        LOGGER.debug { "Shutdown start" }
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
        LOGGER.debug { "Shutdown end" }
    })
}

@Throws(InterruptedException::class)
private fun awaitShutdown(lock: ReentrantLock, condition: Condition) {
    try {
        lock.lock()
        LOGGER.debug { "Wait shutdown" }
        condition.await()
        LOGGER.info { "App shutdown" }
    } finally {
        lock.unlock()
    }
}