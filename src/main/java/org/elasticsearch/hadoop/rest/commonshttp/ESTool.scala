package org.elasticsearch.hadoop.rest.commonshttp

import java.util.concurrent.ConcurrentHashMap

import org.apache.commons.logging.LogFactory
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.hadoop.cfg.{PropertiesSettings, Settings}
import org.elasticsearch.hadoop.rest.{InitializationUtils, RestService, SimpleRequest}
import org.elasticsearch.hadoop.serialization.builder.ValueWriter
import org.elasticsearch.hadoop.serialization.field.FieldExtractor
import org.elasticsearch.hadoop.serialization.{BytesConverter, JdkBytesConverter}
import org.elasticsearch.hadoop.util.BytesArray
import org.elasticsearch.spark.serialization.{ScalaMapFieldExtractor, ScalaValueWriter}

/**
 * 4/8/16 WilliamZhu(allwefantasy@gmail.com)
 */
object ESTool {
  private val items = new ConcurrentHashMap[String, CommonsHttpTransport]()
  protected val log = LogFactory.getLog(this.getClass())

  def transport(settings: Settings, host: String): CommonsHttpTransport = {
    if (items.containsKey(host)) items.get(host)
    synchronized {
      val abc = new CommonsHttpTransport(settings, host)
      items.put(host, abc)
    }
    items.get(host)
  }

  def createRequest(node: String, path: String, body: Array[Byte]) = {
    new SimpleRequest(org.elasticsearch.hadoop.rest.Request.Method.POST, s"http://$node:9200", path, new BytesArray(body))
  }



  def write(serializedSettings: String, data: Iterator[Map[String, String]]) = {

    def processData(data: Iterator[Map[String, String]]): Any = {
      val next = data.next
      next
    }

    def valueWriter: Class[_ <: ValueWriter[_]] = classOf[ScalaValueWriter]
    def bytesConverter: Class[_ <: BytesConverter] = classOf[JdkBytesConverter]
    def fieldExtractor: Class[_ <: FieldExtractor] = classOf[ScalaMapFieldExtractor]

    lazy val settings = {
      val settings = new PropertiesSettings().load(serializedSettings);
      settings.setProperty(ES_RESOURCE_READ, "vod_media_info_v1/info")
      settings.setProperty(ES_RESOURCE_WRITE, "vod_media_info_v1/info")
      InitializationUtils.setValueWriterIfNotSet(settings, valueWriter, log)
      InitializationUtils.setBytesConverterIfNeeded(settings, bytesConverter, log)
      InitializationUtils.setFieldExtractorIfNotSet(settings, fieldExtractor, log)

      settings
    }

    val writer = RestService.createWriter(settings, -1, -1, log)
    while (data.hasNext) {
      writer.repository.writeToIndex(processData(data))
    }
    writer.close()
    writer.repository.stats()
  }
}
