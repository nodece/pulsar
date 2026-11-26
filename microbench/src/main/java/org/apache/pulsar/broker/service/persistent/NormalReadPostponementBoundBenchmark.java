/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service.persistent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures the decision that lets a normal read go ahead of pending replays once replays have postponed it
 * for longer than the bound: {@code shouldYieldToNormalRead()} in
 * {@link PersistentDispatcherMultipleConsumers}.
 *
 * <p>The benchmark models that method with the same field kinds and early-exit order: a final bound field,
 * volatile {@code normalReadStarved}, the cursor's {@code hasMoreEntries()} (two volatile position reads
 * and a compare, as in {@code ManagedCursorImpl}), the volatile blocked-on-unacked flag, and
 * {@code System.nanoTime()}. Bound 0 exits after the first field read, which is the path when the bound is
 * disabled. With a bound and no readable backlog the check exits before reading the clock; that is the
 * case where yielding would only park a waiting read. Only the postponed-replay path is modeled, so the
 * measurement excludes ledger reads, entry dispatch, and consumer writes.
 *
 * <p>Run with bound 0 as the baseline and bound 100 as the feature. The absolute numbers include
 * {@code System.nanoTime()} platform costs; confirm them on Linux x86_64 before using them outside a
 * comparison. For example:
 * <pre>{@code
 * ./gradlew :microbench:shadowJar
 * java -jar microbench/build/libs/microbench-*-benchmarks.jar \
 *     NormalReadPostponementBoundBenchmark -p boundMillis=0,100
 * }</pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class NormalReadPostponementBoundBenchmark {
    private static final int FALSE = 0;
    private static final int TRUE = 1;

    /** One replay round on the postponement path, with the dispatcher's fields and early exits. */
    @State(Scope.Thread)
    public static class BoundState {
        @Param({"0", "100"})
        public int boundMillis;

        @Param({"backlog", "noBacklog"})
        public String cursorState;

        private final AtomicIntegerFieldUpdater<BoundState> blockedUpdater =
                AtomicIntegerFieldUpdater.newUpdater(BoundState.class, "blocked");
        public volatile int blocked = FALSE;
        public volatile boolean normalReadStarved = true;
        public volatile long normalReadStarvedSinceNanos;
        private long maxNormalReadDelayNanos;
        private volatile Position readPosition;
        private volatile Position lastPosition;

        @Setup(Level.Trial)
        public void setup() {
            maxNormalReadDelayNanos = TimeUnit.MILLISECONDS.toNanos(boundMillis);
            normalReadStarvedSinceNanos = 0L;
            readPosition = PositionFactory.create(1, 1000);
            lastPosition = PositionFactory.create(1, "backlog".equals(cursorState) ? 2000 : 999);
        }

        boolean hasMoreEntries() {
            return cursorHasMoreEntries(readPosition, lastPosition);
        }

        boolean shouldYieldToNormalRead() {
            if (maxNormalReadDelayNanos == 0 || !normalReadStarved || !hasMoreEntries()) {
                return false;
            }
            if (blockedUpdater.get(this) == TRUE) {
                return false;
            }
            return System.nanoTime() - normalReadStarvedSinceNanos >= maxNormalReadDelayNanos;
        }
    }

    /** The cursor read without the surrounding field reads, to price {@code hasMoreEntries()} alone. */
    @State(Scope.Thread)
    public static class CursorState {
        private volatile Position readPosition;
        private volatile Position lastPosition;

        @Setup(Level.Trial)
        public void setup() {
            readPosition = PositionFactory.create(1, 1000);
            lastPosition = PositionFactory.create(1, 2000);
        }
    }

    /** The one volatile store a normal read performs when it resets the postponement clock. */
    @State(Scope.Thread)
    public static class ResetState {
        public volatile boolean normalReadStarved = true;
    }

    @Benchmark
    public boolean shouldYieldToNormalRead(BoundState state) {
        return state.shouldYieldToNormalRead();
    }

    @Benchmark
    public boolean cursorHasMoreEntries(CursorState state) {
        return cursorHasMoreEntries(state.readPosition, state.lastPosition);
    }

    @Benchmark
    public long currentTimeNanos() {
        return System.nanoTime();
    }

    @Benchmark
    public boolean clearNormalReadStarved(ResetState state) {
        boolean wasStarved = state.normalReadStarved;
        state.normalReadStarved = false;
        return wasStarved;
    }

    private static boolean cursorHasMoreEntries(Position readPosition, Position lastPosition) {
        // The benchmark uses a writer position with a real entry, so the -1 fallback that counts entries
        // in the previous ledger is not modeled.
        if (lastPosition.getEntryId() != -1) {
            return readPosition.compareTo(lastPosition) <= 0;
        }
        return false;
    }
}
