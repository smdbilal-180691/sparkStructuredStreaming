package com.bilal.spark24
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{StringType,StructField,StructType}
import java.io.File
import scala.collection.mutable.ListBuffer
object spark_test {
  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder().master("local").appName("Hoo").getOrCreate()
    
    val schema = StructType(Seq(StructField("id",StringType),StructField("name",StringType),StructField("misc",StringType)))
    val s = spark
    .readStream
    .option("maxFilesPerTrigger", 1)
    .option("sep",",")
    .schema(schema)
    .csv("C:\\Users\\Mohamed Bilal\\Downloads\\spark_inp")
     
       
   val y = s.writeStream.option("checkpointLocation", "G:\\workspace\\spark24\\spark_check_for").foreachBatch((df,batchId)=>{
     df.show()
     df.write.mode(SaveMode.Append).csv("G:\\workspace\\spark24\\spark_out_foreachbatch")
     
     //Rename files
     
     def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
    } else {
        List[File]()
    }
}
    var res = new ListBuffer()
    val files = getListOfFiles("G:\\workspace\\spark24\\spark_out_foreachbatch")
    var j = 0
    for(i <- files)
    {
      j +=1
      var k=j.toString()
      if(!(i.toString().contains("crc") || i.toString().contains("SUCCESS")))
      {
        print(i)
        new File(i.toString()).renameTo(new File(s"G:\\workspace\\spark24\\spark_out_foreachbatch\\output${k}.csv"))
      }
    }
   }).start()
   
   y.awaitTermination()
   
            
            
  }
}