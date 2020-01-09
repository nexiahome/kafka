/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.metrics

import java.util.concurrent.TimeUnit

import com.codahale.metrics.Gauge
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.SharedMetricRegistries
import kafka.utils.Logging
import org.apache.kafka.common.utils.Sanitizer

trait KafkaMetricsGroup extends Logging {

  /**
   * Creates a new String name for gauges, meters, etc. created for this metrics group.
   * @param name Descriptive name of the metric.
   * @param tags Additional attributes which mBean will have.
   * @return Sanitized metric name as a String.
   */
  def metricName(name: String, tags: scala.collection.Map[String, String]): String = {
    val klass = this.getClass
    val pkg = if (klass.getPackage == null) "" else klass.getPackage.getName
    val simpleName = klass.getSimpleName.replaceAll("\\$$", "")

    explicitMetricName(pkg, simpleName, name, tags)
  }


  protected def explicitMetricName(group: String, typeName: String, name: String,
                                   tags: scala.collection.Map[String, String] = Map.empty): String = {

    val nameBuilder: StringBuilder = new StringBuilder

    nameBuilder.append(group)

    nameBuilder.append(".{type=").append(typeName).append("}")

    if (name.length > 0) {
      nameBuilder.append(".{name=").append(name).append("}")
    }

    nameBuilder.append(KafkaMetricsGroup.toTagSuffix(tags))

    MetricRegistry.name(nameBuilder.toString)
  }

  def newGauge[T](name: String, metric: Gauge[T], tags: scala.collection.Map[String, String] = Map.empty) = {
    val registry = SharedMetricRegistries.getOrCreate("default")
    val fullName = metricName(name, tags)
    val supplier: MetricRegistry.MetricSupplier[Gauge[_]] = new MetricRegistry.MetricSupplier[Gauge[_]] {
      override def newMetric(): Gauge[T] = metric
    }
    registry.gauge(fullName, supplier).asInstanceOf[Gauge[T]]
  }

  def newMeter(name: String, eventType: String, timeUnit: TimeUnit, tags: scala.collection.Map[String, String] = Map.empty) = {
    val registry = SharedMetricRegistries.getOrCreate("default")
    val fullName = metricName(name, tags)

    registry.meter(fullName)
  }

  def newHistogram(name: String, biased: Boolean = true, tags: scala.collection.Map[String, String] = Map.empty) = {
    val registry = SharedMetricRegistries.getOrCreate("default")
    val fullName = metricName(name, tags)

    registry.histogram(fullName)
  }

  def newTimer(name: String, durationUnit: TimeUnit, rateUnit: TimeUnit, tags: scala.collection.Map[String, String] = Map.empty) = {
    val registry = SharedMetricRegistries.getOrCreate("default")
    val fullName = metricName(name, tags)

    registry.timer(fullName)
  }

  def removeMetric(name: String, tags: scala.collection.Map[String, String] = Map.empty) =
    SharedMetricRegistries.getOrCreate("default").remove(metricName(name, tags))
}
object KafkaMetricsGroup extends KafkaMetricsGroup {
  private def toTagSuffix(tags: collection.Map[String, String]): String = {
    val filteredTags = tags.filter { case (_, tagValue) => tagValue != "" }
    if (filteredTags.nonEmpty) {
      val tagsString = filteredTags.toSeq.sortBy(_._1).map { case (key, value) => "{%s=%s}".format(key, Sanitizer.jmxSanitize(value)) }.mkString(".")
      "." + tagsString
    }
    else ""
  }
}
