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

package com.exactpro.th2.conn.amqp

import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.common.grpc.RawMessageMetadata
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.QueueAttribute
import com.exactpro.th2.common.message.toTimestamp
import com.google.protobuf.ByteString
import io.prometheus.client.Counter
import mu.KotlinLogging
import java.time.Instant
import java.util.EnumMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class MessagePublisher(
    private val sessionAlias: String,
    private val drainIntervalMills: Long,
    private val rawRouter: MessageRouter<RawMessageBatch>,
) : AutoCloseable {
    private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
    private val directionStates: Map<Direction, DirectionState> = EnumMap<Direction, DirectionState>(Direction::class.java).apply {
        put(Direction.FIRST, DirectionState())
        put(Direction.SECOND, DirectionState())
    }

    init {
        executor.scheduleAtFixedRate(this::drainMessages, drainIntervalMills, drainIntervalMills, TimeUnit.MILLISECONDS)
    }

    fun onMessage(direction: Direction, holder: MessageHolder) {
        try {
            directionState(direction).addMessage(holder)
        } catch (ex: Exception) {
            LOGGER.error(ex) { "Cannot add message for direction $direction. ${holder.body.contentToString()}" }
        }
        counters[direction]?.run {
            labels(sessionAlias).inc()
        }
    }

    private fun drainMessages() {
        for ((direction, state) in directionStates) {
            try {
                val listToPublish = state.drain()
                if (listToPublish.isEmpty()) continue

                var firstSequence = getFirstBatchSequence(direction, listToPublish.size.toLong())
                val builder = RawMessageBatch.newBuilder()
                for (toPublish in listToPublish) {
                    builder.addMessages(
                        RawMessage.newBuilder().apply {
                            body = ByteString.copyFrom(toPublish.body)
                            metadata = createMetadata(direction, firstSequence++, toPublish.messageProperties, toPublish.sendTime)
                        }
                    )
                }
                builder.build().let { messages ->
                    rawRouter.sendAll(messages, direction.queueAttribute.toString())
                    LOGGER.debug { "Published butch with messages: ${messages.toString()}" }
                }

            } catch (ex: Exception) {
                LOGGER.error(ex) { "Cannot drain events for $direction" }
            }
        }
    }

    private fun createMetadata(direction: Direction, sequence: Long, messageProperties: Map<String, String>, instant: Instant): RawMessageMetadata {
        return RawMessageMetadata.newBuilder().apply {
            setId(MessageID.newBuilder()
                .setDirection(direction)
                .setSequence(sequence)
                .setConnectionId(ConnectionID.newBuilder().setSessionAlias(sessionAlias))
            )
            timestamp = instant.toTimestamp()
            putAllProperties(messageProperties)
        }.build()
    }

    private val Direction.queueAttribute: QueueAttribute
        get() = when (this) {
            Direction.FIRST -> QueueAttribute.FIRST
            Direction.SECOND -> QueueAttribute.SECOND
            else -> throw IllegalArgumentException("Unsupported direction: $this")
        }

    private fun getFirstBatchSequence(direction: Direction, batchSize: Long): Long =
        directionState(direction).firstBatchSequence(batchSize)

    private fun directionState(direction: Direction): DirectionState = requireNotNull(directionStates[direction]) {
        "Unsupported direction: $direction"
    }

    override fun close() {
        executor.shutdown()
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                LOGGER.warn { "Executor is not terminated after timeout. Shutdown it now" }
                val remainingTasks = executor.shutdownNow()
                LOGGER.warn { "Remaining tasks after termination: ${remainingTasks.size}" }
            }
        } catch (ex: InterruptedException) {
            LOGGER.error(ex) { "Executor awaiting interrupted" }
            Thread.currentThread().interrupt()
        }
    }

    private class DirectionState {
        private val sequence = AtomicLong(initSequence())
        private val messagesToPublish: MutableList<MessageHolder> = arrayListOf()

        fun firstBatchSequence(batchSize: Long): Long = sequence.getAndAdd(batchSize)

        fun addMessage(holder: MessageHolder) {
            synchronized(messagesToPublish) {
                messagesToPublish += holder
            }
        }

        fun drain(): List<MessageHolder> {
            return synchronized(messagesToPublish) {
                if (messagesToPublish.isEmpty()) return@synchronized emptyList()

                val result = messagesToPublish.toList()
                messagesToPublish.clear()
                result
            }
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }

        private fun initSequence(): Long = Instant.now().run { epochSecond * 1_000_000_000 + nano }

        private val counters: Map<Direction, Counter> = mapOf(
            // FIXME: use DEFAULT_SESSION_ALIAS_LABEL_NAME variable
            Direction.FIRST to Counter.build().apply {
                name("th2_conn_incoming_msg_quantity")
                labelNames("session_alias")
                help("Quantity of incoming messages to conn")
            }.register(),
            Direction.SECOND to Counter.build().apply {
                name("th2_conn_outgoing_msg_quantity")
                labelNames("session_alias")
                help("Quantity of outgoing messages from conn")
            }.register()
        )
    }
}