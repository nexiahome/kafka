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
package org.apache.kafka.test;


import static org.apache.kafka.streams.processor.AsyncProcessingResult.Status.OFFSET_UPDATED;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.processor.AsyncProcessingResult;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.SourceNode;

public class MockSourceNode<K, V> extends SourceNode<K, V> {

    public static final String NAME = "MOCK-SOURCE-";
    public static final AtomicInteger INDEX = new AtomicInteger(1);

    public int numReceived = 0;
    public final ArrayList<K> keys = new ArrayList<>();
    public final ArrayList<V> values = new ArrayList<>();
    public final ArrayList<Long> offsets = new ArrayList<>();
    public boolean initialized;
    public boolean closed;
    public boolean processedSynchronously = true;
    public boolean shouldOverrideProcessAsync = false;
    public AsyncProcessingResult overrideAsyncResult;

    public MockSourceNode(final String[] topics, final Deserializer<K> keyDeserializer, final Deserializer<V> valDeserializer) {
        super(NAME + INDEX.getAndIncrement(), Arrays.asList(topics), keyDeserializer, valDeserializer);
    }

    @Override
    public void process(final K key, final V value) {
        this.numReceived++;
        this.keys.add(key);
        this.values.add(value);
    }

    @Override
    public AsyncProcessingResult maybeProcessAsync(final K key, final V value, final long offset) {
        AsyncProcessingResult result = shouldOverrideProcessAsync ? overrideAsyncResult :
            new AsyncProcessingResult(OFFSET_UPDATED, offset);

        if (result.getStatus() == OFFSET_UPDATED) {
            process(key, value);
            this.offsets.add(result.getLastProcessedOffset());
            this.processedSynchronously = false;
        }

        return result;
    }

    @Override
    public void init(final InternalProcessorContext context) {
        super.init(context);
        initialized = true;
    }

    @Override
    public void close() {
        super.close();
        this.closed = true;
    }

    public void overrideAsyncAndReturnOffset(long offset) {
        shouldOverrideProcessAsync = true;
        overrideAsyncResult = new AsyncProcessingResult(OFFSET_UPDATED, offset);
    }

    public void resetAsyncOverride() {
        shouldOverrideProcessAsync = false;
    }

    public void overrideAsyncResult(AsyncProcessingResult asyncProcessingResult) {
        shouldOverrideProcessAsync = true;
        overrideAsyncResult = asyncProcessingResult;
    }
}
