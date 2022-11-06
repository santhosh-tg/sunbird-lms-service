package org.sunbird.job.questionset.republish.function

import com.google.gson.reflect.TypeToken
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.questionset.republish.domain.{Event, PublishMetadata}
import org.sunbird.job.{BaseProcessFunction, Metrics}
import java.lang.reflect.Type

import org.sunbird.job.questionset.republish.task.QuestionSetRePublishConfig

class PublishEventRouter(config: QuestionSetRePublishConfig) extends BaseProcessFunction[Event, String](config) {


	private[this] val logger = LoggerFactory.getLogger(classOf[PublishEventRouter])
	val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType

	override def open(parameters: Configuration): Unit = {
		super.open(parameters)
	}

	override def close(): Unit = {
		super.close()
	}

	override def metricsList(): List[String] = {
		List(config.skippedEventCount, config.totalEventsCount)
	}

	override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
		metrics.incCounter(config.totalEventsCount)
		if (event.validEvent()) {
			logger.info("PublishEventRouter :: Event: " + event)
			event.objectType match {
				case "Question" => {
					logger.info("PublishEventRouter :: Sending Question For RePublish Having Identifier: " + event.objectId)
					context.output(config.questionRePublishOutTag, PublishMetadata(event.objectId, event.objectType, event.mimeType, event.pkgVersion, event.publishType, event.lastPublishedBy))
				}
				case "QuestionSet" => {
					logger.info("PublishEventRouter :: Sending QuestionSet For RePublish Having Identifier: " + event.objectId)
					context.output(config.questionSetRePublishOutTag, PublishMetadata(event.objectId, event.objectType, event.mimeType, event.pkgVersion, event.publishType, event.lastPublishedBy))
				}
				case _ => {
					metrics.incCounter(config.skippedEventCount)
					logger.info("Invalid Object Type Received For RePublish.| Identifier : " + event.objectId + " , objectType : " + event.objectType)
				}
			}
		} else {
      logger.warn("Republish Event skipped for identifier: " + event.objectId + " | objectType: " + event.objectType + " |pkgVersion")
			metrics.incCounter(config.skippedEventCount)
		}
	}
}
