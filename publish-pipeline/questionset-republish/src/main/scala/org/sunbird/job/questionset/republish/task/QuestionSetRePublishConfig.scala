package org.sunbird.job.questionset.republish.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.questionset.republish.domain.PublishMetadata

import java.util
import scala.collection.JavaConverters._

class QuestionSetRePublishConfig(override val config: Config) extends PublishConfig(config, "questionset-republish"){

	implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
	implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
	implicit val publishMetaTypeInfo: TypeInformation[PublishMetadata] = TypeExtractor.getForClass(classOf[PublishMetadata])

	// Job Configuration
	val jobEnv: String = config.getString("job.env")

	// Kafka Topics Configuration
	val kafkaInputTopic: String = config.getString("kafka.input.topic")
	val postPublishTopic: String = config.getString("kafka.post_publish.topic")
	val inputConsumerName = "questionset-republish-consumer"

	// Parallelism
	override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
	val eventRouterParallelism: Int = config.getInt("task.router.parallelism")

	// Metric List
	val totalEventsCount = "total-events-count"
	val skippedEventCount = "skipped-event-count"
	val questionRePublishEventCount = "question-republish-count"
	val questionRePublishSuccessEventCount = "question-republish-success-count"
	val questionRePublishFailedEventCount = "question-republish-failed-count"
	val questionSetRePublishEventCount = "questionset-republish-count"
	val questionSetRePublishSuccessEventCount = "questionset-republish-success-count"
	val questionSetRePublishFailedEventCount = "questionset-republish-failed-count"

	// Cassandra Configurations
	val cassandraHost: String = config.getString("lms-cassandra.host")
	val cassandraPort: Int = config.getInt("lms-cassandra.port")
	val questionKeyspaceName = config.getString("question.keyspace")
	val questionTableName = config.getString("question.table")
	val questionSetKeyspaceName = config.getString("questionset.keyspace")
	val questionSetTableName = config.getString("questionset.table")

	// Neo4J Configurations
	val graphRoutePath = config.getString("neo4j.routePath")
	val graphName = config.getString("neo4j.graph")

	// Cache Config
	val cacheDbId: Int = if(config.hasPath("redis.database.qsCache.id")) config.getInt("redis.database.qsCache.id") else 0

	// Out Tags
	val questionRePublishOutTag: OutputTag[PublishMetadata] = OutputTag[PublishMetadata]("question-republish")
	val questionSetRePublishOutTag: OutputTag[PublishMetadata] = OutputTag[PublishMetadata]("questionset-republish")

	// Service Urls
	val printServiceBaseUrl: String = config.getString("print_service.base_url")

	val definitionBasePath: String = if (config.hasPath("schema.basePath")) config.getString("schema.basePath") else "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local"
	val schemaSupportVersionMap = if (config.hasPath("schema.supportedVersion")) config.getObject("schema.supportedVersion").unwrapped().asScala.toMap else Map[String, AnyRef]()
}
