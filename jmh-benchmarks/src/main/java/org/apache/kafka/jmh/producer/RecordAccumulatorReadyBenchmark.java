/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.jmh.producer;

import org.apache.kafka.clients.MetadataSnapshot;
import org.apache.kafka.clients.producer.internals.BufferPool;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks {@link RecordAccumulator#ready} (which drives {@code partitionReady()} internally),
 * the method the Sender background thread calls on every {@code runOnce()} cycle to decide which
 * nodes have data ready to send. This loop runs once per partition per cycle regardless of
 * traffic, so its per-partition cost matters most for producers with many partitions.
 *
 * <p>Fixed at 1000 partitions: large enough for the result to be stable across repeated runs,
 * small enough to keep each run fast.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RecordAccumulatorReadyBenchmark {

    private static final int PARTITION_COUNT = 1000;

    private static final String TOPIC = "bench-topic";

    private RecordAccumulator accumulator;
    private MetadataSnapshot metadataSnapshot;
    private Metrics metrics;

    @Setup(Level.Trial)
    public void setup() throws InterruptedException {
        Node node = new Node(0, "localhost", 9092);

        List<PartitionMetadata> partitionMetadatas = new ArrayList<>(PARTITION_COUNT);
        for (int i = 0; i < PARTITION_COUNT; i++) {
            List<Integer> replicaIds = Collections.singletonList(node.id());
            partitionMetadatas.add(new PartitionMetadata(Errors.NONE, new TopicPartition(TOPIC, i),
                Optional.of(node.id()), Optional.empty(), replicaIds, replicaIds,
                Collections.emptyList()));
        }

        Map<Integer, Node> nodesById = new HashMap<>();
        nodesById.put(node.id(), node);
        metadataSnapshot = new MetadataSnapshot(null, nodesById, partitionMetadatas,
            Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null,
            Collections.emptyMap());
        Cluster cluster = metadataSnapshot.cluster();

        LogContext logContext = new LogContext();
        metrics = new Metrics(Time.SYSTEM);
        int batchSize = 16 * 1024;
        // Every partition gets one batch below and nothing ever drains, so the pool must hold
        // PARTITION_COUNT batches at once, or append() blocks forever waiting for freed memory.
        long totalMemory = (long) (PARTITION_COUNT + 16) * batchSize;
        RecordAccumulator.PartitionerConfig partitionerConfig =
            new RecordAccumulator.PartitionerConfig(true, 0, false, null);
        accumulator = new RecordAccumulator(
            logContext,
            batchSize,
            Compression.NONE,
            0,
            100L,
            1000L,
            120_000,
            partitionerConfig,
            metrics,
            "producer-metrics",
            Time.SYSTEM,
            null,
            new BufferPool(totalMemory, batchSize, metrics, Time.SYSTEM, "producer-metrics"));

        // Give every partition a pending batch so ready() can't early-exit on an empty deque,
        // matching a producer that's actively sending to all of its partitions.
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = new byte[100];
        long nowMs = Time.SYSTEM.milliseconds();
        for (int i = 0; i < PARTITION_COUNT; i++) {
            accumulator.append(TOPIC, i, 0L, key, value, null, null, 0L, nowMs, cluster);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        metrics.close();
    }

    @Benchmark
    public RecordAccumulator.ReadyCheckResult ready() {
        return accumulator.ready(metadataSnapshot, Time.SYSTEM.milliseconds());
    }
}
