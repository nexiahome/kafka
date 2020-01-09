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

import java.util.Properties

import javax.management.ObjectName
import com.codahale.metrics.SharedMetricRegistries
import com.codahale.metrics.Meter
import org.junit.Test
import org.junit.Assert._
import kafka.integration.KafkaServerTestHarness
import kafka.server._
import kafka.utils._

import scala.collection._
import scala.collection.JavaConverters._
import kafka.log.LogConfig
import org.apache.kafka.common.TopicPartition

class MetricsTest extends KafkaServerTestHarness with Logging {
  val numNodes = 2
  val numParts = 2

  val overridingProps = new Properties
  overridingProps.put(KafkaConfig.NumPartitionsProp, numParts.toString)

  def generateConfigs =
    TestUtils.createBrokerConfigs(numNodes, zkConnect).map(KafkaConfig.fromProps(_, overridingProps))

  val nMessages = 2

  @Test
  def testMetricsReporterAfterDeletingTopic(): Unit = {
    val topic = "test-topic-metric"
    createTopic(topic, 1, 1)
    adminZkClient.deleteTopic(topic)
    TestUtils.verifyTopicDeletion(zkClient, topic, 1, servers)
    assertEquals("Topic metrics exists after deleteTopic", Set.empty, topicMetrics(Some(topic)))
  }

  @Test
  def testBrokerTopicMetricsUnregisteredAfterDeletingTopic(): Unit = {
    val topic = "test-broker-topic-metric"
    createTopic(topic, 2, 1)
    // Produce a few messages to create the metrics
    // Don't consume messages as it may cause metrics to be re-created causing the test to fail, see KAFKA-5238
    TestUtils.generateAndProduceMessages(servers, topic, nMessages)
    assertTrue("Topic metrics don't exist", topicMetrics(Some(topic)).nonEmpty)
    servers.foreach(s => assertNotNull(s.brokerTopicStats.topicStats(topic)))
    adminZkClient.deleteTopic(topic)
    TestUtils.verifyTopicDeletion(zkClient, topic, 1, servers)
    assertEquals("Topic metrics exists after deleteTopic", Set.empty, topicMetrics(Some(topic)))
  }

  @Test
  def testClusterIdMetric(): Unit = {
    // Check if clusterId metric exists.
    val metrics = SharedMetricRegistries.getOrCreate("default").getMetrics
    assertEquals(metrics.keySet.asScala.count(_ == "kafka.server.{type=KafkaServer}.{name=ClusterId}"), 1)
  }

  @Test
  def testGeneralBrokerTopicMetricsAreGreedilyRegistered(): Unit = {
    val topic = "test-broker-topic-metric"
    createTopic(topic, 2, 1)

    // The broker metrics for all topics should be greedily registered
    assertTrue("General topic metrics don't exist", topicMetrics(None).nonEmpty)
    assertEquals(servers.head.brokerTopicStats.allTopicsStats.metricMap.size, topicMetrics(None).size)
    // topic metrics should be lazily registered
    assertTrue("Topic metrics aren't lazily registered", topicMetrics(Some(topic)).isEmpty)
    TestUtils.generateAndProduceMessages(servers, topic, nMessages)
    assertTrue("Topic metrics aren't registered", topicMetrics(Some(topic)).nonEmpty)
  }

  @Test
  def testWindowsStyleTagNames(): Unit = {
    val path = "C:\\windows-path\\kafka-logs"
    val tags = Map("dir" -> path)
    val expectedTag = Set(tags.keySet.head, ObjectName.quote(path)).mkString("=")
    val metricname = KafkaMetricsGroup.metricName("test-metric", tags)
    assert(metricname.endsWith(".{" + expectedTag + "}"))
  }

  @Test
  def testBrokerTopicMetricsBytesInOut(): Unit = {
    val topic = "test-bytes-in-out"
    val replicationBytesIn = BrokerTopicStats.ReplicationBytesInPerSec
    val replicationBytesOut = BrokerTopicStats.ReplicationBytesOutPerSec
    val bytesIn = s"${BrokerTopicStats.BytesInPerSec}"
    val bytesOut = s"${BrokerTopicStats.BytesOutPerSec}"

    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.MinInSyncReplicasProp, "2")
    createTopic(topic, 1, numNodes, topicConfig)
    // Produce a few messages to create the metrics
    TestUtils.generateAndProduceMessages(servers, topic, nMessages)

    // Check the log size for each broker so that we can distinguish between failures caused by replication issues
    // versus failures caused by the metrics
    val topicPartition = new TopicPartition(topic, 0)
    servers.foreach { server =>
      val log = server.logManager.getLog(new TopicPartition(topic, 0))
      val brokerId = server.config.brokerId
      val logSize = log.map(_.size)
      assertTrue(s"Expected broker $brokerId to have a Log for $topicPartition with positive size, actual: $logSize",
        logSize.map(_ > 0).getOrElse(false))
    }

    // Consume messages to make bytesOut tick
    TestUtils.consumeTopicRecords(servers, topic, nMessages)
    val initialReplicationBytesIn = TestUtils.meterCount(replicationBytesIn)
    val initialReplicationBytesOut = TestUtils.meterCount(replicationBytesOut)
    val initialBytesIn = TestUtils.topicMeterCount(bytesIn, topic)
    val initialBytesOut = TestUtils.topicMeterCount(bytesOut, topic)

    // BytesOut doesn't include replication, so it shouldn't have changed
    assertEquals(initialBytesOut, TestUtils.meterCount(bytesOut))

    // Produce a few messages to make the metrics tick
    TestUtils.generateAndProduceMessages(servers, topic, nMessages)

    assertTrue(TestUtils.meterCount(replicationBytesIn) > initialReplicationBytesIn)
    assertTrue(TestUtils.meterCount(replicationBytesOut) > initialReplicationBytesOut)
    assertTrue(TestUtils.topicMeterCount(bytesIn, topic) > initialBytesIn)

    // Consume messages to make bytesOut tick
    TestUtils.consumeTopicRecords(servers, topic, nMessages)

    assertTrue(TestUtils.topicMeterCount(bytesOut, topic) > initialBytesOut)
  }

  @Test
  def testControllerMetrics(): Unit = {
    val metricNames = SharedMetricRegistries.getOrCreate("default").getNames

    assertEquals(metricNames.asScala.count(_ == "kafka.controller.{type=KafkaController}.{name=ActiveControllerCount}"), 1)
    assertEquals(metricNames.asScala.count(_ == "kafka.controller.{type=KafkaController}.{name=OfflinePartitionsCount}"), 1)
    assertEquals(metricNames.asScala.count(_ == "kafka.controller.{type=KafkaController}.{name=PreferredReplicaImbalanceCount}"), 1)
    assertEquals(metricNames.asScala.count(_ == "kafka.controller.{type=KafkaController}.{name=GlobalTopicCount}"), 1)
    assertEquals(metricNames.asScala.count(_ == "kafka.controller.{type=KafkaController}.{name=GlobalPartitionCount}"), 1)
    assertEquals(metricNames.asScala.count(_ == "kafka.controller.{type=KafkaController}.{name=TopicsToDeleteCount}"), 1)
    assertEquals(metricNames.asScala.count(_ == "kafka.controller.{type=KafkaController}.{name=ReplicasToDeleteCount}"), 1)
    assertEquals(metricNames.asScala.count(_ == "kafka.controller.{type=KafkaController}.{name=TopicsIneligibleToDeleteCount}"), 1)
    assertEquals(metricNames.asScala.count(_ == "kafka.controller.{type=KafkaController}.{name=ReplicasIneligibleToDeleteCount}"), 1)
  }

  /**
   * Test that the metrics are created with the right name, testZooKeeperStateChangeRateMetrics
   * and testZooKeeperSessionStateMetric in ZooKeeperClientTest test the metrics behaviour.
   */
  @Test
  def testSessionExpireListenerMetrics(): Unit = {
    val metricNames = SharedMetricRegistries.getOrCreate("default").getNames

    assertEquals(metricNames.asScala.count(_ == "kafka.server.{type=SessionExpireListener}.{name=SessionState}"), 1)
    assertEquals(metricNames.asScala.count(_ == "kafka.server.{type=SessionExpireListener}.{name=ZooKeeperExpiresPerSec}"), 1)
    assertEquals(metricNames.asScala.count(_ == "kafka.server.{type=SessionExpireListener}.{name=ZooKeeperDisconnectsPerSec}"), 1)
  }

  private def topicMetrics(topic: Option[String]): Set[String] = {
    val metricNames = SharedMetricRegistries.getOrCreate("default").getNames.asScala
    filterByTopicMetricRegex(metricNames, topic)
  }

  private def filterByTopicMetricRegex(metrics: Set[String], topic: Option[String]): Set[String] = {
    val pattern = (".*BrokerTopicMetrics.*" + topic.map(t => s"\\.\\{topic=${t}\\}").getOrElse("")).r.pattern
    metrics.filter(pattern.matcher(_).matches())
  }
}
