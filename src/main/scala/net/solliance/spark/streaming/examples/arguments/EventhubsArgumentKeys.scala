package net.solliance.spark.streaming.examples.arguments

/**
  * Created by zoinertejada on 8/17/16.
  */
object EventhubsArgumentKeys extends Enumeration {
  val EventhubsNamespace: String = "eventhubsNamespace"
  val EventhubsName: String = "eventhubsName"
  val PolicyName: String = "policyName"
  val PolicyKey: String = "policyKey"
  val ConsumerGroup: String = "consumerGroup"
  val PartitionCount: String = "partitionCount"
  val BatchIntervalInSeconds: String = "batchInterval"
  val CheckpointDirectory: String = "checkpointDirectory"
  val EventCountFolder: String = "eventCountFolder"
  val EventStoreFolder: String = "eventStoreFolder"
  val EventHiveTable: String = "eventHiveTable"
  val SQLServerFQDN: String = "sqlServerFQDN"
  val SQLDatabaseName: String = "sqlDatabaseName"
  val DatabaseUsername: String = "databaseUsername"
  val DatabasePassword: String = "databasePassword"
  val EventSQLTable: String = "eventSQLTable"
  val TimeoutInMinutes: String = "jobTimeout"
}
