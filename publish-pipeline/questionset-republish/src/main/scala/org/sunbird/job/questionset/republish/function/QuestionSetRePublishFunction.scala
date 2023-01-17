package org.sunbird.job.questionset.republish.function

import akka.dispatch.ExecutionContexts
import com.google.gson.reflect.TypeToken
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.publish.helpers.EcarPackageType
import org.sunbird.job.questionset.republish.domain.PublishMetadata
import org.sunbird.job.questionset.republish.helpers.QuestionSetPublisher
import org.sunbird.job.questionset.republish.task.QuestionSetRePublishConfig
import org.sunbird.job.util._
import org.sunbird.job.{BaseProcessFunction, Metrics}
import java.lang.reflect.Type
import java.text.SimpleDateFormat
import java.util.Date

import org.sunbird.job.cache.{DataCache, RedisConnect}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

class QuestionSetRePublishFunction(config: QuestionSetRePublishConfig, httpUtil: HttpUtil,
                                   @transient var neo4JUtil: Neo4JUtil = null,
                                   @transient var cassandraUtil: CassandraUtil = null,
                                   @transient var cloudStorageUtil: CloudStorageUtil = null,
                                   @transient var definitionCache: DefinitionCache = null,
                                   @transient var definitionConfig: DefinitionConfig = null)
                                  (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[PublishMetadata, String](config) with QuestionSetPublisher {

	private[this] val logger = LoggerFactory.getLogger(classOf[QuestionSetRePublishFunction])
	val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
	val liveStatus = List("Live", "Unlisted")

	@transient var ec: ExecutionContext = _
	@transient var cache: DataCache = _
	private val pkgTypes = List(EcarPackageType.SPINE.toString, EcarPackageType.ONLINE.toString, EcarPackageType.FULL.toString)

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
		List(config.questionSetRePublishEventCount, config.questionSetRePublishSuccessEventCount, config.questionSetRePublishFailedEventCount)
	}

	override def processElement(data: PublishMetadata, context: ProcessFunction[PublishMetadata, String]#Context, metrics: Metrics): Unit = {
		logger.info("QuestionSet publishing started for : " + data.identifier)
		metrics.incCounter(config.questionSetRePublishEventCount)
		val definition: ObjectDefinition = definitionCache.getDefinition(data.objectType, config.schemaSupportVersionMap.getOrElse(data.objectType.toLowerCase(), "1.0").asInstanceOf[String], config.definitionBasePath)
		val readerConfig = ExtDataConfig(config.questionSetKeyspaceName, config.questionSetTableName, definition.getExternalPrimaryKey, definition.getExternalProps)
		val qDef: ObjectDefinition = definitionCache.getDefinition("Question", config.schemaSupportVersionMap.getOrElse("question", "1.0").asInstanceOf[String], config.definitionBasePath)
		val qReaderConfig = ExtDataConfig(config.questionKeyspaceName, qDef.getExternalTable, qDef.getExternalPrimaryKey, qDef.getExternalProps)
		val obj = getObject(data.identifier, data.pkgVersion, data.mimeType, data.publishType, readerConfig)(neo4JUtil, cassandraUtil, config)
		logger.info("processElement ::: obj metadata before publish ::: " + ScalaJsonUtil.serialize(obj.metadata))
		logger.info("processElement ::: obj hierarchy before publish ::: " + ScalaJsonUtil.serialize(obj.hierarchy.getOrElse(Map())))
		try {
			val messages: List[String] = validate(obj, obj.identifier, validateQuestionSet)
			if (messages.isEmpty) {
				val cacheKey = s"""qs_hierarchy_${obj.identifier}"""
				cache.del(cacheKey)
				cache.del(obj.identifier)
				val qList: List[ObjectData] = getQuestions(obj, qReaderConfig)(cassandraUtil)
				logger.info("processElement ::: child questions list from hierarchy :::  " + qList)
				//Skipped publishing children question as part of republish
				// Enrich Object as well as hierarchy
				val enrichedObj = enrichObject(obj)(neo4JUtil, cassandraUtil, readerConfig, cloudStorageUtil, config, definitionCache, definitionConfig)
				logger.info(s"processElement ::: object enrichment done for ${obj.identifier}")
				logger.info("processElement :::  obj metadata post enrichment :: " + ScalaJsonUtil.serialize(enrichedObj.metadata))
				logger.info("processElement :::  obj hierarchy post enrichment :: " + ScalaJsonUtil.serialize(enrichedObj.hierarchy.get))
				val objWithArtifactUrl = updateArtifactUrl(enrichedObj, EcarPackageType.FULL.toString)(ec, neo4JUtil, cloudStorageUtil, definitionCache, definitionConfig, config, httpUtil)
				// Generate ECAR
				val objWithEcar = generateECAR(objWithArtifactUrl, pkgTypes)(ec, neo4JUtil, cloudStorageUtil, config, definitionCache, definitionConfig, httpUtil)
				// Generate PDF URL
				val updatedObj = generatePreviewUrl(objWithEcar, qList)(httpUtil, cloudStorageUtil)
				saveOnSuccess(updatedObj)(neo4JUtil, cassandraUtil, readerConfig, definitionCache, definitionConfig)
				logger.info("QuestionSet publishing completed successfully for : " + data.identifier)
				metrics.incCounter(config.questionSetRePublishSuccessEventCount)
			} else {
				val upPkgVersion = obj.pkgVersion + 1
				val migrVer = 0.2
				val nodeId = obj.dbId
				val errorMessages = messages.mkString("; ")
				val query = s"""MATCH (n:domain{IL_UNIQUE_ID:"$nodeId"}) SET n.status="Failed", n.pkgVersion=$upPkgVersion, n.publishError="$errorMessages", n.migrationVersion=$migrVer, $auditPropsUpdateQuery;"""
				neo4JUtil.executeQuery(query)
				metrics.incCounter(config.questionSetRePublishFailedEventCount)
				logger.info("QuestionSet Re-publishing failed for : " + data.identifier)
			}
		} catch {
			case e: Exception => {
				val upPkgVersion = obj.pkgVersion + 1
				val migrVer = 0.2
				val nodeId = obj.dbId
				val query = s"""MATCH (n:domain{IL_UNIQUE_ID:"$nodeId"}) SET n.status="Failed", n.pkgVersion=$upPkgVersion, n.publishError="${e.getMessage}", n.migrationVersion=$migrVer, $auditPropsUpdateQuery;"""
				neo4JUtil.executeQuery(query)
			}
		}
	}

	def generateECAR(data: ObjectData, pkgTypes: List[String])(implicit ec: ExecutionContext, neo4JUtil: Neo4JUtil, cloudStorageUtil: CloudStorageUtil, config: PublishConfig, defCache: DefinitionCache, defConfig: DefinitionConfig, httpUtil: HttpUtil): ObjectData = {
		val ecarMap: Map[String, String] = generateEcar(data, pkgTypes)
		val variants: java.util.Map[String, java.util.Map[String, String]] = ecarMap.map { case (key, value) => key.toLowerCase -> Map[String, String]("ecarUrl" -> value, "size" -> httpUtil.getSize(value).toString).asJava }.asJava
		logger.info("QuestionSetPublishFunction ::: generateECAR ::: ecar map ::: " + ecarMap)
		val meta: Map[String, AnyRef] = Map("downloadUrl" -> ecarMap.getOrElse(EcarPackageType.FULL.toString, ""), "variants" -> variants, "size" -> httpUtil.getSize(ecarMap.getOrElse(EcarPackageType.FULL.toString, "")).asInstanceOf[AnyRef])
		new ObjectData(data.identifier, data.metadata ++ meta, data.extData, data.hierarchy)
	}

	def generatePreviewUrl(data: ObjectData, qList: List[ObjectData])(implicit httpUtil: HttpUtil, cloudStorageUtil: CloudStorageUtil): ObjectData = {
		val (pdfUrl, previewUrl) = getPdfFileUrl(qList, data, "questionSetTemplate.vm", config.printServiceBaseUrl, System.currentTimeMillis().toString)(httpUtil, cloudStorageUtil)
		logger.info("generatePreviewUrl ::: finalPdfUrl ::: " + pdfUrl.getOrElse(""))
		logger.info("generatePreviewUrl ::: finalPreviewUrl ::: " + previewUrl.getOrElse(""))
		new ObjectData(data.identifier, data.metadata ++ Map("previewUrl" -> previewUrl.getOrElse(""), "pdfUrl" -> pdfUrl.getOrElse("")), data.extData, data.hierarchy)
	}

	def isValidChildQuestion(obj: ObjectData, createdBy: String): Boolean = {
		//TODO: Change the logic
		StringUtils.equalsIgnoreCase("Parent", obj.getString("visibility", "")) || (StringUtils.equalsIgnoreCase("Default", obj.getString("visibility", "")) && !liveStatus.contains(obj.getString("status", "")) && StringUtils.equalsIgnoreCase(createdBy, obj.getString("createdBy", "")))
	}

	private def auditPropsUpdateQuery(): String = {
		val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
		val updatedOn = sdf.format(new Date())
		s"""n.lastUpdatedOn="$updatedOn",n.lastStatusChangedOn="$updatedOn""""
	}

}
