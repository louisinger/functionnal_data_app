package com.main

import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Deserializer

class MessageDeserializer extends Deserializer[Message]{

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): Message = {
    val mapper = new ObjectMapper()
    val message = mapper.readValue(data, classOf[Message])
    println("Deserializer gives: " + message)
    message
  }

  override def close(): Unit = {}
}
