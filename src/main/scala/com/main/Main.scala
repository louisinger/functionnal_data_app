package com.main

import java.io.{File, FileWriter}
import java.time.Duration
import java.util.Properties

import com.utils.MessageUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec
import scala.collection.JavaConverters._


object Main {

  val SIMULATION_NAME: String = "simu1"


  def storeMessage(filePath: String, messageToAppend: String): Boolean = {
    val fw = new FileWriter(filePath, true)
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

  def getOrCreateDataFile(simulationName: String): String = {
    val filePath = s"data/$simulationName.json"
    val f = new File(filePath)
    if(!f.exists()) f.createNewFile()
    filePath
  }

  @tailrec def startConsuming(consumer: KafkaConsumer[String, Message]): Unit = {
    val records: ConsumerRecords[String, Message] = consumer.poll(Duration.ofMillis(100))
    var msg: Message = null
    records.asScala.foreach { record =>
      msg = record.value()
      storeMessage(getOrCreateDataFile(msg.getSimulationName), s"""{"droneName":"${msg.getDroneName}","value":"${msg.getValue}","isIncident":"${msg.getIncident}"}""")
      println(s"offset = ${record.offset()}, key=${record.key()}, value=${msg}")
    }
    startConsuming(consumer)
  }


  def getAllIncidentMessages(rdd: RDD[Message]): RDD[String] = rdd.filter(msg => msg.getIncident).map(msg => msg.toString)

  def getDroneList(rdd: RDD[Message]) : RDD[String] = rdd.map(msg => msg.getDroneName).distinct()

  def getAllMessagesForADrone(rdd: RDD[Message]): RDD[String] = {
    val dronesList = getDroneList(rdd).collect()
    println("Drones list:")
    dronesList.foreach(println)
    print("Enter the drone to analyse: ")
    val droneNameInput = scala.io.StdIn.readLine()
    println("Messages for the drone " + droneNameInput + ":")
    rdd.filter(msg => msg.getDroneName == droneNameInput).map(msg => msg.toString)
  }

  @tailrec def exploreData(): Unit = {

    // Load data for a simulationName
    def loadData(simulationName: String): RDD[Message] = {
      val conf = new SparkConf()
        .setAppName("ExploreData")
        .setMaster("local[*]")

      val sc = SparkContext.getOrCreate(conf)
      sc.textFile(getOrCreateDataFile(SIMULATION_NAME)).mapPartitions(MessageUtils.parseFromJson)
    }

    // Get the list of the different simulations
    def getListSimulations: List[String] = {
      val dir = new File("./data")
      if(dir.exists() && dir.isDirectory) {
        dir.listFiles.filter(_.isFile).toList.map(_.getName.dropRight(5))
      } else {
        List[String]()
      }
    }

    // Explore data
    val simulationsList = getListSimulations
    var i_simulation = 1
    simulationsList.foreach(simulation => {
      println(i_simulation + ": " + simulation)
      i_simulation += 1
    })
    println("Enter the simulation to analyse: ")
    val simulationNumberChoosen = scala.io.StdIn.readInt
    val listChoosen = simulationsList(simulationNumberChoosen - 1)
    println("Loading the data...")
    val rdd = loadData(listChoosen)
    println("Data successfully loaded.")

    println("Choose a request to process:")
    println("1. Get the incident messages")
    println("2. Get all the messages")
    println("3. get all messages for a drone")
    print("Enter the number of the request to process on data:")
    val requestNumber = scala.io.StdIn.readInt()
    val result: RDD[String] = requestNumber match{
      case 1 => getAllIncidentMessages(rdd)
      case 2 => rdd.map(m => m.toString)
      case 3 => getAllMessagesForADrone(rdd)
      case _ => rdd.map(msg => msg.toString)
    }

    val messages = result.collect()
    messages.foreach(println(_))
    println("--- End of request ---")


    println("Do you want to process another research request? tape 1 for Yes and 2 for No.")
    val continue = scala.io.StdIn.readInt
    if(continue==1) exploreData()
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