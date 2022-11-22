package org.sunbird.job.util

import java.util

import org.apache.commons.collections.MapUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.BaseJobConfig

import scala.collection.JavaConverters._

object CSPMetaUtil {

	private[this] val logger = LoggerFactory.getLogger(classOf[CSPMetaUtil])

	def updateAbsolutePath(data: util.Map[String, AnyRef])(implicit config: BaseJobConfig): util.Map[String, AnyRef] = {
		val cspMeta: util.List[String] = config.config.getStringList("cloudstorage.metadata.list")
		val absolutePath = config.getString("cloudstorage.read_base_path", "") + java.io.File.separator + config.getString("cloud_storage_container", "")
		if (MapUtils.isNotEmpty(data)) {
			val updatedMeta: util.Map[String, AnyRef] = data.asScala.map(x => if (cspMeta.contains(x._1)) (x._1, x._2.asInstanceOf[String].replace("CLOUD_STORAGE_BASE_PATH", absolutePath)) else (x._1, x._2)).toMap.asJava
			updatedMeta
		} else data
	}

	def updateAbsolutePath(data: util.List[util.Map[String, AnyRef]])(implicit config: BaseJobConfig): util.List[util.Map[String, AnyRef]] = {
		val cspMeta: util.List[String] = config.config.getStringList("cloudstorage.metadata.list")
		val absolutePath = config.getString("cloudstorage.read_base_path", "") + java.io.File.separator + config.getString("cloud_storage_container", "")
		data.asScala.toList.map(meta => {
			if (MapUtils.isNotEmpty(meta)) {
				val updatedMeta: util.Map[String, AnyRef] = meta.asScala.map(x => if (cspMeta.contains(x._1)) (x._1, x._2.asInstanceOf[String].replace("CLOUD_STORAGE_BASE_PATH", absolutePath)) else (x._1, x._2)).toMap.asJava
				updatedMeta
			} else meta
		}).asJava
	}

	def updateAbsolutePath(data: String)(implicit config: BaseJobConfig): String = {
		val absolutePath = config.getString("cloudstorage.read_base_path", "") + java.io.File.separator + config.getString("cloud_storage_container", "")
		if (StringUtils.isNotEmpty(data)) {
			val updatedData: String = data.replaceAll("CLOUD_STORAGE_BASE_PATH", absolutePath)
			updatedData
		} else data
	}

	def updateRelativePath(query: String)(implicit config: BaseJobConfig): String = {
		logger.info("CSPMetaUtil ::: updateRelativePath ::: query before url replace :: " + query)
		val validCSPSource: util.List[String] = config.config.getStringList("cloudstorage.write_base_path")
		validCSPSource.forEach(basePath => {
			val path = basePath + java.io.File.separator + config.getString("cloud_storage_container", "")
			if (query.contains(path)) {
				logger.info("CSPMetaUtil ::: updateRelativePath ::: replacing urls")
				query.replaceAll(path, "CLOUD_STORAGE_BASE_PATH")
			}
		})
		logger.info("CSPMetaUtil ::: updateRelativePath ::: query after url replace :: " + query)
		query
	}

	def updateRelativePath(data: util.Map[String, AnyRef])(implicit config: BaseJobConfig): util.Map[String, AnyRef] = {
		val cspMeta: util.List[String] = config.config.getStringList("cloudstorage.metadata.list")
		val validCSPSource: util.List[String] = config.config.getStringList("cloudstorage.write_base_path")
		val basePath: List[String] = validCSPSource.asScala.toList.map(source => source + java.io.File.separator + config.getString("cloud_storage_container", ""))
		if (MapUtils.isNotEmpty(data)) {
			//val updatedMeta: util.Map[String, AnyRef] = data.entrySet().stream().map(x=> if(cspMeta.contains(x.getKey)) (x.getKey, basePath.map(path => if(x.getValue.asInstanceOf[String].contains(path)) x.getValue.asInstanceOf[String].replace(path, "CLOUD_STORAGE_BASE_PATH") else x.getValue)) else (x.getKey, x.getValue))
			val updatedMeta: util.Map[String, AnyRef] = data.asScala.map(x => if (cspMeta.contains(x._1)) (x._1, basePath.map(path => if (x._2.asInstanceOf[String].contains(path)) x._2.asInstanceOf[String].replace(path, "CLOUD_STORAGE_BASE_PATH") else x._2)) else (x._1, x._2)).toMap.asJava
			updatedMeta
		} else data
	}

	def updateCloudPath(objList: List[Map[String, AnyRef]])(implicit config: BaseJobConfig): List[Map[String, AnyRef]] = {
		val cspMeta: util.List[String] = config.config.getStringList("cloudstorage.metadata.list")
		val newCloudPath: String = config.getString("cloudstorage.read_base_path", "") + java.io.File.separator + config.getString("cloud_storage_container", "")
		val validCSPSource: util.List[String] = config.config.getStringList("cloudstorage.write_base_path")
		val basePath: List[String] = validCSPSource.asScala.toList.map(source => source + java.io.File.separator + config.getString("cloud_storage_container", ""))
		objList.map(data => {
			if (null != data && data.nonEmpty) {
				val updatedData: Map[String, AnyRef] = data.map(x => if (cspMeta.contains(x._1)) (x._1, basePath.map(path => if (x._2.asInstanceOf[String].contains(path)) x._2.asInstanceOf[String].replace(path, newCloudPath) else x._2)) else (x._1, x._2))
				updatedData
			} else data
		})
	}

}

class CSPMetaUtil {}
