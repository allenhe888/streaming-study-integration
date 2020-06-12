package com.bigdata.streaming.streamsets.sparktransformer

import java.io.Serializable
import java.util

import com.streamsets.pipeline.api.{Field, Record}
import com.streamsets.pipeline.spark.api.{SparkTransformer, TransformResult}
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}

class TransformerDemo extends SparkTransformer with Serializable {

  private var configsMap:java.util.Map[String, String] = new util.HashMap[String, String]()

  def setConfigMap(configs:java.util.Map[String, String]):Unit={
    this.configsMap = configs
  }

  val valuePath = "/text"
  val mapped = "/mapped"


  var emptyRDD : JavaRDD[(Record,String)] = _

  override def init(context: JavaSparkContext, parameters: util.List[String]): Unit = {
    super.init(context, parameters)
    emptyRDD = context.emptyRDD //  Create an empty JavaPairRDD to return as 'errors'

  }

  override def transform(recordRDD: JavaRDD[Record]): TransformResult = {
    val rdd = recordRDD.rdd

    val errors = emptyRDD

    var count = 0
    rdd.map((rc)=>{
      count= count+1
      rc.get().getValueAsMap.put("count",Field.create(count))
      rc
    })
    val partitions = rdd.getNumPartitions

    val result = rdd.map((record)=>{
      var attrMap:util.Map[String,Field] = new util.HashMap[String,Field]()
      if(record.has("/attr")){
        attrMap = record.get("/attr").getValueAsMap
      }else{
        attrMap = new util.HashMap[String,Field]()
      }
      attrMap.put("javaIoTmpDir",Field.create(System.getProperty("java.io.tmpdir")))
      attrMap.put("userName",Field.create(System.getProperty("user.name")))
      if(null != partitions ){
        attrMap.put("numPartitions",Field.create(partitions))
      }
      record.get().getValueAsMap.put("attr",Field.create(attrMap))
      record
    })

    // return result
    new TransformResult(result.toJavaRDD(),new JavaPairRDD[Record,String](errors))
  }
}
