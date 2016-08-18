/*
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

package net.solliance.spark.streaming.examples.workloads

import net.liftweb.json._
import net.solliance.spark.streaming.examples.arguments._
import net.solliance.spark.streaming.examples.arguments.EventhubsArgumentParser.ArgumentMap
import net.solliance.spark.streaming.examples.common._
import org.apache.spark._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.eventhubs.EventHubsUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

case class TempDataPoint(temp: Double, createDate : java.util.Date, deviceId : String)

object EventhubsEmitAlerts {

  implicit val formats = DefaultFormats

  val maxAlertTemp = 68.0
  val minAlertTemp = 65.0

  def createStreamingContext(inputOptions: ArgumentMap): StreamingContext = {

    val eventHubsParameters = Map[String, String](
      "eventhubs.namespace" -> inputOptions(Symbol(EventhubsArgumentKeys.EventhubsNamespace)).asInstanceOf[String],
      "eventhubs.name" -> inputOptions(Symbol(EventhubsArgumentKeys.EventhubsName)).asInstanceOf[String],
      "eventhubs.policyname" -> inputOptions(Symbol(EventhubsArgumentKeys.PolicyName)).asInstanceOf[String],
      "eventhubs.policykey" -> inputOptions(Symbol(EventhubsArgumentKeys.PolicyKey)).asInstanceOf[String],
      "eventhubs.consumergroup" -> inputOptions(Symbol(EventhubsArgumentKeys.ConsumerGroup)).asInstanceOf[String],
      "eventhubs.partition.count" -> inputOptions(Symbol(EventhubsArgumentKeys.PartitionCount))
      .asInstanceOf[Int].toString,
      "eventhubs.checkpoint.interval" -> inputOptions(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds))
      .asInstanceOf[Int].toString,
      "eventhubs.checkpoint.dir" -> inputOptions(Symbol(EventhubsArgumentKeys.CheckpointDirectory)).asInstanceOf[String]
    )

    val sparkConfiguration = new SparkConf().setAppName(this.getClass.getSimpleName)

    sparkConfiguration.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    sparkConfiguration.set("spark.streaming.driver.writeAheadLog.closeFileAfterWrite", "true")
    sparkConfiguration.set("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite", "true")
    sparkConfiguration.set("spark.streaming.stopGracefullyOnShutdown", "true")

    val sparkContext = new SparkContext(sparkConfiguration)

    val streamingContext = new StreamingContext(sparkContext,
      Seconds(inputOptions(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds)).asInstanceOf[Int]))
    streamingContext.checkpoint(inputOptions(Symbol(EventhubsArgumentKeys.CheckpointDirectory)).asInstanceOf[String])

    val eventHubsStream = EventHubsUtils.createUnionStream(streamingContext, eventHubsParameters)

    val eventHubsWindowedStream = eventHubsStream
      .window(Seconds(inputOptions(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds)).asInstanceOf[Int]))

    defineComputations(streamingContext, eventHubsWindowedStream, inputOptions)

    streamingContext

  }

  def defineComputations(streamingContext : StreamingContext, windowedStream : DStream[Array[Byte]], inputOptions: ArgumentMap) = {

    // Count number of events received in the past batch
    val batchEventCount = windowedStream.count()
    batchEventCount.print()

    // Count number of events received so far
    val totalEventCountDStream = windowedStream.map(m => (StreamStatistics.streamLengthKey, 1L))
    val totalEventCount = totalEventCountDStream.updateStateByKey[Long](StreamStatistics.streamLength)
    totalEventCount.checkpoint(Seconds(inputOptions(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds))
      .asInstanceOf[Int]))
    totalEventCount.print()

    // Simulate detecting an alert condition
    windowedStream.map(x => EventContent(new String(x)))
      .foreachRDD { rdd =>
        rdd.foreachPartition { partition =>
        //...Create/open connection to destination...

          partition.foreach {record =>
            // examine alert status
            val json = parse(record.EventDetails)
            val dataPoint = json.extract[TempDataPoint]

            if (dataPoint.temp > maxAlertTemp)
              {
                println(s"=== reading ABOVE bounds. DeviceId: ${dataPoint.deviceId}, Temp: ${dataPoint.temp} ===")
                //...push alert out ...
              }
            else if (dataPoint.temp < minAlertTemp)
              {
                println(s"=== reading BELOW bounds. DeviceId: ${dataPoint.deviceId}, Temp: ${dataPoint.temp} ===")
                //...push alert out ...
              }
          }
        //...Close connection ...
        }
      }

  }

  def main(inputArguments: Array[String]): Unit = {

    val inputOptions: ArgumentMap = EventhubsArgumentParser.parseArguments(Map(), inputArguments.toList)

    EventhubsArgumentParser.verifyEventhubsEventCountArguments(inputOptions)

    //Create or recreate (from checkpoint storage) streaming context
    val streamingContext = StreamingContext
      .getOrCreate(inputOptions(Symbol(EventhubsArgumentKeys.CheckpointDirectory)).asInstanceOf[String],
      () => createStreamingContext(inputOptions))

    streamingContext.start()

    if(inputOptions.contains(Symbol(EventhubsArgumentKeys.TimeoutInMinutes))) {

      streamingContext.awaitTerminationOrTimeout(inputOptions(Symbol(EventhubsArgumentKeys.TimeoutInMinutes))
        .asInstanceOf[Long] * 60 * 1000)
    }
    else {

      streamingContext.awaitTermination()
    }
  }
}


