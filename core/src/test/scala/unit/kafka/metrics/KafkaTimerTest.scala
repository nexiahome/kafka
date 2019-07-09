package kafka.metrics

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.junit.Test
import java.util.concurrent.TimeUnit
import org.junit.Assert._
import com.codahale.metrics.{MetricRegistry, Clock, SlidingWindowReservoir, Timer}

class KafkaTimerTest {

  @Test
  def testKafkaTimer() {
    val clock = new ManualClock
    val manualTimer = new Timer(new SlidingWindowReservoir(1000), clock)
    val testRegistry = new MetricRegistry()
    val metric = testRegistry.register(MetricRegistry.name(this.getClass, "TestTimer"), manualTimer)
    val Epsilon = java.lang.Double.longBitsToDouble(0x3ca0000000000000L)

    val timer = new KafkaTimer(metric)
    timer.time {
      clock.addMillis(1000)
    }
    assertEquals(1, metric.getCount())
    assertTrue((metric.getSnapshot.getMax - 1000000000).abs <= Epsilon)
    assertTrue((metric.getSnapshot.getMin - 1000000000).abs <= Epsilon)
  }

  private class ManualClock extends Clock {

    private var ticksInNanos = 0L

    override def getTick() = {
      ticksInNanos
    }

    override def getTime() = {
      TimeUnit.NANOSECONDS.toMillis(ticksInNanos)
    }

    def addMillis(millis: Long) {
      ticksInNanos += TimeUnit.MILLISECONDS.toNanos(millis)
    }
  }
}
