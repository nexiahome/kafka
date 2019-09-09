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
package org.apache.kafka.streams.processor.internals;

import static org.apache.kafka.common.utils.Utils.min;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl.KeyValueStoreReadWriteDecorator;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl.SessionStoreReadWriteDecorator;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl.TimestampedKeyValueStoreReadWriteDecorator;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl.TimestampedWindowStoreReadWriteDecorator;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl.WindowStoreReadWriteDecorator;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.ThreadCache;

import java.time.Duration;
import java.util.List;

public class GlobalProcessorContextImpl extends AbstractProcessorContext {


    public GlobalProcessorContextImpl(final StreamsConfig config,
                                      final StateManager stateMgr,
                                      final StreamsMetricsImpl metrics,
                                      final ThreadCache cache) {
        super(new TaskId(-1, -1), config, metrics, stateMgr, cache);
    }

    @SuppressWarnings("unchecked")
    @Override
    public StateStore getStateStore(final String name) {
        final StateStore store = stateManager.getGlobalStore(name);

        if (store instanceof TimestampedKeyValueStore) {
            return new TimestampedKeyValueStoreReadWriteDecorator((TimestampedKeyValueStore) store);
        } else if (store instanceof KeyValueStore) {
            return new KeyValueStoreReadWriteDecorator((KeyValueStore) store);
        } else if (store instanceof TimestampedWindowStore) {
            return new TimestampedWindowStoreReadWriteDecorator((TimestampedWindowStore) store);
        } else if (store instanceof WindowStore) {
            return new WindowStoreReadWriteDecorator((WindowStore) store);
        } else if (store instanceof SessionStore) {
            return new SessionStoreReadWriteDecorator((SessionStore) store);
        }

        return store;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> long forward(final K key, final V value) {
        final ProcessorNode previousNode = currentNode();
        Long lastProcessedOffset = offset();
        try {
            for (final ProcessorNode child : (List<ProcessorNode<K, V>>) currentNode().children()) {
                setCurrentNode(child);
                lastProcessedOffset = min(child.maybeProcessAsync(key, value, offset()), lastProcessedOffset);
            }
        } finally {
            setCurrentNode(previousNode);
        }
        return lastProcessedOffset;
    }

    /**
     * No-op. This should only be called on GlobalStateStore#flush and there should be no child nodes
     */
    @Override
    public <K, V> long forward(final K key, final V value, final To to) {
        if (!currentNode().children().isEmpty()) {
            throw new IllegalStateException("This method should only be called on 'GlobalStateStore.flush' that should not have any children.");
        }
        return recordContext().offset();
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    @Deprecated
    public <K, V> long forward(final K key, final V value, final int childIndex) {
        throw new UnsupportedOperationException("this should not happen: forward() not supported in global processor context.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    @Deprecated
    public <K, V> long forward(final K key, final V value, final String childName) {
        throw new UnsupportedOperationException("this should not happen: forward() not supported in global processor context.");
    }

    @Override
    public void commit() {
        //no-op
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    @Deprecated
    public Cancellable schedule(final long interval, final PunctuationType type, final Punctuator callback) {
        throw new UnsupportedOperationException("this should not happen: schedule() not supported in global processor context.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public Cancellable schedule(final Duration interval, final PunctuationType type, final Punctuator callback) {
        throw new UnsupportedOperationException("this should not happen: schedule() not supported in global processor context.");
    }
}