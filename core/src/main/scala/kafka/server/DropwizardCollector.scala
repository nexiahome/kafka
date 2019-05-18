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

package kafka.server

import java.util.concurrent.TimeUnit
import java.util.{ArrayList, Arrays, HashMap, List}

import com.codahale.metrics._
import io.prometheus.client.Collector
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.hotspot.DefaultExports

import kafka.utils._

import scala.collection.JavaConverters._

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.lonelyplanet.prometheus.api.MetricsEndpoint

class PrometheusMetricsServer(theInterface: String, thePort: Short) {
  implicit val system = ActorSystem("prometheusmetrics")
  implicit val dispatcher = system.dispatcher
  implicit val materializer = ActorMaterializer()

  val DEFAULT_METRIC_REGISTRY = SharedMetricRegistries.getOrCreate("default")
  val prometheusRegistry = CollectorRegistry.defaultRegistry
  prometheusRegistry.register(new DropwizardCollector(DEFAULT_METRIC_REGISTRY))

  DefaultExports.initialize()

  val metricsEndpoint = new MetricsEndpoint(prometheusRegistry)
  val routes = metricsEndpoint.routes
  val server = Http().bindAndHandle(routes, interface = theInterface, port = thePort)
}

/** Adapted from io.prometheus.client.dropwizard.DropwizardExports */
class DropwizardCollector(registry: MetricRegistry)
    extends io.prometheus.client.Collector
    with io.prometheus.client.Collector.Describable
    with Logging {
  private val METRIC_LABEL_RE = """\.\{([^=}]*)=([^}]*)\}""".r.unanchored
  private val METRIC_NAME_RE = "[^a-zA-Z0-9:_]".r.unanchored
  private val METRIC_STARTS_WITH_NONDIGIT_RE = "[^0-9].*".r

  this.logIdent = "[Prometheus Dropwizard Metrics Collector]: "

  private def getHelpMessage(metricName: String, metric: Metric) =
    s"Dropwizard ${metric.getClass.getName} ${metricName}"

  /**
   * Parse out labels. Replace all unsupported chars with '_', prepend '_' if name starts with
   * digit.
   *
   * @param dropwizardName original metric name.
   * @return the sanitized metric name, the label names, and the label values.
   */
  private def parseMetricName(dropwizardName: String) = {
    val labelNames = new ArrayList[String]
    val labelValues = new ArrayList[String]

    val matchIter = METRIC_LABEL_RE.findAllIn(dropwizardName)
    while (matchIter.hasNext) {
      matchIter.next
      val noMetaLabelName = METRIC_NAME_RE.replaceAllIn(matchIter.group(1), "_")
      labelNames.add(noMetaLabelName)
      labelValues.add(matchIter.group(2))
    }

    val noLabelName = METRIC_LABEL_RE.replaceAllIn(dropwizardName, "")
    val noMetaName = METRIC_NAME_RE.replaceAllIn(noLabelName, "_")
    val name = noMetaName match {
      case METRIC_STARTS_WITH_NONDIGIT_RE(_*) => noMetaName
      case _ => "_" + noMetaName
    }

    (name, labelNames, labelValues)
  }

  private def copyAndAlter(base: ArrayList[String], index: Int, value: String) = {
    val copied = new ArrayList[String](base)
    copied.set(index, value)
    copied
  }

  /** Export a Meter as as prometheus COUNTER. */
  private def fromMeter(dropwizardName: String, meter: Meter) = {
    // Build a list of MetricFamilySamples for Prometheus; Meters map to Prometheus Counters
    val (name, labelNames, labelValues) = parseMetricName(dropwizardName)
    Arrays.asList(
        new Collector.MetricFamilySamples(
            name,
            Collector.Type.COUNTER,
            getHelpMessage(dropwizardName, meter),
            Arrays.asList(
                new Collector.MetricFamilySamples.Sample(
                    name, labelNames, labelValues, meter.getCount))))
  }

  /**
   * Export counter as Prometheus <a
   * href="https://prometheus.io/docs/concepts/metric_types/#gauge">Gauge</a>.
   */
  private def fromCounter(dropwizardName: String, counter: Counter) = {
    val (name, labelNames, labelValues) = parseMetricName(dropwizardName)
    Arrays.asList(
        new Collector.MetricFamilySamples(
            name,
            Collector.Type.GAUGE,
            getHelpMessage(dropwizardName, counter),
            Arrays.asList(
                new Collector.MetricFamilySamples.Sample(
                    name,
                    labelNames,
                    labelValues,
                    counter.getCount.toDouble))))
  }

  /** Export gauge as a prometheus gauge. */
  private def fromGauge(dropwizardName: String, gauge: Gauge[_]) = {
    val (name, labelNames, labelValues) = parseMetricName(dropwizardName)
    val obj = gauge.getValue

    val value = obj match {
      case x: Boolean => if (x) 1.0 else 0.0
      case x: Int => x.toDouble
      case x: Long => x.toDouble
      case x: Double => x
      case Nil => {
        // TODO we really ought to raise an exception here
        debug(s"Null value for Gauge ${name}")
        Nil
      }
      case _ => {
        // TODO we really ought to raise an exception here
        debug(s"Invalid type for Gauge ${name}: ${obj.getClass.getName}")
        Nil
      }
    }

    value match {
      case x: Double => Arrays.asList(
          new Collector.MetricFamilySamples(
              name,
              Collector.Type.GAUGE,
              getHelpMessage(dropwizardName, gauge),
              Arrays.asList(
                  new Collector.MetricFamilySamples.Sample(
                      name, labelNames, labelValues, x))))
      case _ => Arrays.asList()
    }
  }

  /**
   * Export a histogram snapshot as a prometheus SUMMARY.
   *
   * @param dropwizardName metric name.
   * @param snapshot the histogram snapshot.
   * @param count the total sample count for this snapshot.
   * @param factor a factor to apply to histogram values.
   */
  private def fromSnapshotAndCount(dropwizardName: String, snapshot: Snapshot, count: Long, factor: Double, helpMessage: String) = {
    val (name, labelNames, labelValues) = parseMetricName(dropwizardName)
    val labelNamesPlus = new ArrayList[String](labelNames)
    labelNamesPlus.add("quantile")
    val quantileIndex = labelValues.size
    val labelValuesPlus = new ArrayList[String](labelValues)
    labelValuesPlus.add("DummyQuantile")

    val sampleList = scala.collection.immutable.List(
      new Collector.MetricFamilySamples.Sample(
          name,
          labelNamesPlus,
          copyAndAlter(labelValuesPlus, quantileIndex, "0.5"),
          snapshot.getMedian() * factor),
      new Collector.MetricFamilySamples.Sample(
          name,
          labelNamesPlus,
          copyAndAlter(labelValuesPlus, quantileIndex, "0.75"),
          snapshot.get75thPercentile() * factor),
      new Collector.MetricFamilySamples.Sample(
          name,
          labelNamesPlus,
          copyAndAlter(labelValuesPlus, quantileIndex, "0.95"),
          snapshot.get95thPercentile() * factor),
      new Collector.MetricFamilySamples.Sample(
          name,
          labelNamesPlus,
          copyAndAlter(labelValuesPlus, quantileIndex, "0.98"),
          snapshot.get98thPercentile() * factor),
      new Collector.MetricFamilySamples.Sample(
          name,
          labelNamesPlus,
          copyAndAlter(labelValuesPlus, quantileIndex, "0.99"),
          snapshot.get99thPercentile() * factor),
      new Collector.MetricFamilySamples.Sample(
          name,
          labelNamesPlus,
          copyAndAlter(labelValuesPlus, quantileIndex, "0.999"),
          snapshot.get999thPercentile() * factor),
      new Collector.MetricFamilySamples.Sample(name + "_count", labelNames, labelValues, count)
    )

    val samples: List[Collector.MetricFamilySamples.Sample] = sampleList.asJava
    Arrays.asList(new Collector.MetricFamilySamples(name, Collector.Type.SUMMARY, helpMessage, samples))
  }

  /** Convert histogram snapshot. */
  private def fromHistogram(dropwizardName: String, histogram: Histogram) =
    fromSnapshotAndCount(
        dropwizardName,
        histogram.getSnapshot,
        histogram.getCount,
        1.0,
        getHelpMessage(dropwizardName, histogram)
    )

  /** Export Dropwizard Timer as a histogram. Use TIME_UNIT as time unit. */
  private def fromTimer(dropwizardName: String, timer: Timer) =
    fromSnapshotAndCount(
        dropwizardName,
        timer.getSnapshot,
        timer.getCount,
        1.0D / TimeUnit.SECONDS.toNanos(1L),
        getHelpMessage(dropwizardName, timer)
    )

  override def collect(): List[Collector.MetricFamilySamples] = {
    val mfSamples = new ArrayList[Collector.MetricFamilySamples]
    val mfHash = new HashMap[String, Collector.MetricFamilySamples]

    registry.getMeters.entrySet.asScala.foreach { entry =>
      fromMeter(entry.getKey, entry.getValue).asScala.foreach { mfs =>
        if (mfHash.containsKey(mfs.name)) {
          mfHash.get(mfs.name).samples.addAll(mfs.samples)
        } else {
          mfHash.put(mfs.name, mfs)
        }
      }
    }
    registry.getCounters.entrySet.asScala.foreach { entry =>
      fromCounter(entry.getKey, entry.getValue).asScala.foreach { mfs =>
        if (mfHash.containsKey(mfs.name)) {
          mfHash.get(mfs.name).samples.addAll(mfs.samples)
        } else {
          mfHash.put(mfs.name, mfs)
        }
      }
    }
    registry.getGauges.entrySet.asScala.foreach { entry =>
      fromGauge(entry.getKey, entry.getValue.asInstanceOf[Gauge[_]]).asScala.foreach { mfs =>
        if (mfHash.containsKey(mfs.name)) {
          mfHash.get(mfs.name).samples.addAll(mfs.samples)
        } else {
          mfHash.put(mfs.name, mfs)
        }
      }
    }
    registry.getHistograms.entrySet.asScala.foreach { entry =>
      fromHistogram(entry.getKey, entry.getValue).asScala.foreach { mfs =>
        if (mfHash.containsKey(mfs.name)) {
          mfHash.get(mfs.name).samples.addAll(mfs.samples)
        } else {
          mfHash.put(mfs.name, mfs)
        }
      }
    }
    registry.getTimers.entrySet.asScala.foreach { entry =>
      fromTimer(entry.getKey, entry.getValue).asScala.foreach { mfs =>
        if (mfHash.containsKey(mfs.name)) {
          mfHash.get(mfs.name).samples.addAll(mfs.samples)
        } else {
          mfHash.put(mfs.name, mfs)
        }
      }
    }

    mfSamples.addAll(mfHash.values())
    mfSamples
  }

  override def describe(): List[Collector.MetricFamilySamples] = new ArrayList[Collector.MetricFamilySamples]
}
