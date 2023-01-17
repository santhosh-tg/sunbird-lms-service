package org.sunbird.job.questionset.republish.function

import akka.dispatch.ExecutionContexts
import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.publish.helpers.EcarPackageType
import org.sunbird.job.questionset.republish.domain.PublishMetadata
import org.sunbird.job.questionset.republish.helpers.QuestionPublisher
import org.sunbird.job.questionset.republish.task.QuestionSetRePublishConfig
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, HttpUtil, Neo4JUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}
import java.lang.reflect.Type
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.cache.{DataCache, RedisConnect}

import scala.concurrent.ExecutionContext

class QuestionRePublishFunction(config: QuestionSetRePublishConfig, httpUtil: HttpUtil,
                              @transient var neo4JUtil: Neo4JUtil = null,
                              @transient var cassandraUtil: CassandraUtil = null,
                              @transient var cloudStorageUtil: CloudStorageUtil = null,
                              @transient var definitionCache: DefinitionCache = null,
                              @transient var definitionConfig: DefinitionConfig = null)
                             (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[PublishMetadata, String](config) with QuestionPublisher {

  private[this] val logger = LoggerFactory.getLogger(classOf[QuestionRePublishFunction])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
  private val readerConfig = ExtDataConfig(config.questionKeyspaceName, config.questionTableName)

  @transient var ec: ExecutionContext = _
  @transient var cache: DataCache = _
  private val pkgTypes = List(EcarPackageType.FULL.toString, EcarPackageType.ONLINE.toString)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.cassandraHost, config.cassandraPort, config)
    neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName, config)
    cloudStorageUtil = new CloudStorageUtil(config)
    ec = ExecutionContexts.global
    definitionCache = new DefinitionCache()
    definitionConfig = DefinitionConfig(config.schemaSupportVersionMap, config.definitionBasePath)
    cache = new DataCache(config, new RedisConnect(config), config.cacheDbId, List())
    cache.init()
  }

  override def close(): Unit = {
    super.close()
    cassandraUtil.close()
    cache.close()
  }

  override def metricsList(): List[String] = {
    List(config.questionRePublishEventCount, config.questionRePublishSuccessEventCount, config.questionRePublishFailedEventCount)
  }

  override def processElement(data: PublishMetadata, context: ProcessFunction[PublishMetadata, String]#Context, metrics: Metrics): Unit = {
    logger.info("Question publishing started for : " + data.identifier)
    metrics.incCounter(config.questionRePublishEventCount)
    val obj = getObject(data.identifier, data.pkgVersion, data.mimeType, data.publishType, readerConfig)(neo4JUtil, cassandraUtil,config)
    try {
      val messages: List[String] = validate(obj, obj.identifier, validateQuestion)
      if (messages.isEmpty) {
        cache.del(obj.identifier)
        val enrichedObj = enrichObject(obj)(neo4JUtil, cassandraUtil, readerConfig, cloudStorageUtil, config, definitionCache, definitionConfig)
        val objWithArtifactUrl = updateArtifactUrl(enrichedObj, EcarPackageType.FULL.toString)(ec, neo4JUtil, cloudStorageUtil, definitionCache, definitionConfig, config, httpUtil)
        val objWithEcar = getObjectWithEcar(objWithArtifactUrl, pkgTypes)(ec, neo4JUtil, cloudStorageUtil, config, definitionCache, definitionConfig, httpUtil)
        logger.info("Ecar generation done for Question: " + objWithEcar.identifier)
        saveOnSuccess(objWithEcar)(neo4JUtil, cassandraUtil, readerConfig, definitionCache, definitionConfig)
        metrics.incCounter(config.questionRePublishSuccessEventCount)
        logger.info("Question publishing completed successfully for : " + data.identifier)
      } else {
        val upPkgVersion = obj.pkgVersion + 1
        val migrVer = 0.2
        val nodeId = obj.dbId
        val errorMessages = messages.mkString("; ")
        val query = s"""MATCH (n:domain{IL_UNIQUE_ID:"$nodeId"}) SET n.status="Failed", n.pkgVersion=$upPkgVersion, n.publishError="$errorMessages", n.migrationVersion=$migrVer, $auditPropsUpdateQuery;"""
        neo4JUtil.executeQuery(query)
        metrics.incCounter(config.questionRePublishFailedEventCount)
        logger.info("Question publishing failed for : " + data.identifier)
      }
    } catch {
      case e: Exception => {
        val upPkgVersion = obj.pkgVersion + 1
        val migrVer = 0.2
        val nodeId = obj.dbId
        val errorMessages = e.getLocalizedMessage
        val query = s"""MATCH (n:domain{IL_UNIQUE_ID:"$nodeId"}) SET n.status="Failed", n.pkgVersion=$upPkgVersion, n.publishError="$errorMessages", n.migrationVersion=$migrVer, $auditPropsUpdateQuery;"""
        neo4JUtil.executeQuery(query)
        metrics.incCounter(config.questionRePublishFailedEventCount)
        logger.info("Question re-publishing failed for : " + data.identifier + "| Error : "+e.getLocalizedMessage)
        e.printStackTrace()

      }
    }

  }

  private def auditPropsUpdateQuery(): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    val updatedOn = sdf.format(new Date())
    s"""n.lastUpdatedOn="$updatedOn",n.lastStatusChangedOn="$updatedOn""""
  }
  
}
