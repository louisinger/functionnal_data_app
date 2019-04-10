package com.main

import java.io.FileWriter
import java.time.Duration
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}

import scala.annotation.tailrec
import scala.collection.JavaConverters._


object Main {

  def storeMessage(filePath: String, messageToAppend: String): Boolean = {
    val fw = new FileWriter("data.json", true)
    try {
      fw.append(s"$messageToAppend\n")
      true
    } catch {
      case e: Exception => {
        e.printStackTrace()
        false
      }
    } finally {
      fw.close()
    }
  }

  @tailrec def startConsuming(consumer: KafkaConsumer[String, Message]): Unit = {
    val records: ConsumerRecords[String, Message] = consumer.poll(Duration.ofMillis(100))
    var msg: Message = null
    records.asScala.foreach { record =>
      msg = record.value()
      storeMessage("data.json", s"""{"droneName":"${msg.getDroneName}","value":"${msg.getValue}","isIncident":"${msg.getIncident}"}""")
      println(s"offset = ${record.offset()}, key=${record.key()}, value=${msg}")
    }
    startConsuming(consumer)
  }

  def exploreData(): Unit = {

  }

  def main(args: Array[String]): Unit = {
    val properties: Properties = new Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[MessageDeserializer])
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[MessageDeserializer])
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "monitors")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer: KafkaConsumer[String, Message] = new KafkaConsumer[String, Message](properties)
    consumer.subscribe(List("main").asJava)

    println("What do you want to do? \n\t1. Consume message\n\t2. Search for data")
    print("\nEnter your choice number: ")
    val choiceNumber = scala.io.StdIn.readInt()
    if(choiceNumber == 1) startConsuming(consumer)
    else exploreData()
  }
}