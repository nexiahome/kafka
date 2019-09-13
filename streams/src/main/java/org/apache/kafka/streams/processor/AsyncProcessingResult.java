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
package org.apache.kafka.streams.processor;

import static org.apache.kafka.common.utils.Utils.min;

public class AsyncProcessingResult {
    public enum Status {
        OFFSET_NOT_UPDATED,
        OFFSET_UPDATED,
        ASYNC_PAUSE
    }

    private Status status;
    private long lastProcessedOffset;

    public AsyncProcessingResult(final Status status, final long offset) {
        this.status = status;
        this.lastProcessedOffset = offset;
    }

    AsyncProcessingResult(final Status status) {
        this.status = status;
        this.lastProcessedOffset = -1;
    }

    public long getLastProcessedOffset() {
        return lastProcessedOffset;
    }

    public Status getStatus() {
        return status;
    }

    public void merge(final AsyncProcessingResult other) {
        if (status.equals(Status.OFFSET_NOT_UPDATED) || other.getStatus().equals(Status.OFFSET_NOT_UPDATED)) {
            status = Status.OFFSET_NOT_UPDATED;
        } else if (status.equals(Status.ASYNC_PAUSE) || other.getStatus().equals(Status.ASYNC_PAUSE)) {
            status = Status.ASYNC_PAUSE;
        } else {
            status = Status.OFFSET_UPDATED;
        }
        this.lastProcessedOffset = min(this.lastProcessedOffset, other.getLastProcessedOffset());
    }
}
