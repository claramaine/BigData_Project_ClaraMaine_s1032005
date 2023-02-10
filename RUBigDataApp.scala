package org.rubigdata

// Old imports I dont want to delete
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.scalalang.typed

// Class definitions we need in the remainder:
import org.apache.hadoop.io.NullWritable
import de.l3s.concatgz.io.warc.{WarcGzInputFormat,WarcWritable}
import de.l3s.concatgz.data.WarcRecord

// Overrule default settings
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

// needed for processing text
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document,Element}
import collection.JavaConverters._

// needed for creating the schema
import org.apache.spark.sql.types._

object RUBigDataApp {
  def main(args: Array[String]) {
    //val warcfile = s"/opt/hadoop/rubigdata/CC-MAIN-20210417102129-20210417132129-00338.warc.gz"
    val warcfile = s"hdfs:///single-warc-segment"
	//val warcfile = s"hdfs:///user/s1032005/CC-MAIN-20210417102129-20210417132129-00338.warc.gz"
	
	val spark = SparkSession.builder.appName("RUBigDataApp").getOrCreate()
	spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

	val sc = spark.sparkContext
	val warcs = sc.newAPIHadoopFile(
              warcfile,
              classOf[WarcGzInputFormat],             // InputFormat
              classOf[NullWritable],                  // Key
              classOf[WarcWritable]                   // Value
    )
	
	//sanity check
	val nHTML = warcs.count()
	println(nHTML)
	println(s"Output:")
	
	val warc_records = warcs.map{ wr => wr._2 }
                        .filter{_.isValid()}
                        .map{_.getRecord()}
	
	val schema = new StructType()
      .add("ant1",StringType,true)
      .add("ant2",StringType,true)

	val antpairs = spark.read
        .format("csv")
        .option("header", false)
        .option("multiLine", true)
        .schema(schema)
        .load("hdfs:/user/s1032005/antonym_data.csv") //.cache()
    
	// sanity check
	antpairs.printSchema()
	antpairs.show(5)

	
	val warc_text = warc_records
				.map(wr => wr.getHttpStringBody() )
				.map(wr => Jsoup.parse(wr))
				.filter(wr => wr.select("html").first().attr("lang") == "en")
				.map(_.body().text())
				.filter(_.length > 0)
				
	val warc_words = warc_text
				.flatMap(_.split(" "))
				.map(_.replaceAll(
					"[^a-zA-Z]", "")
					.toLowerCase
				)
				
	val antpairslist1 = antpairs.select("ant1").map(f=>f.getString(0)).collect.toList
	val antpairslist2 = antpairs.select("ant2").map(f=>f.getString(0)).collect.toList
				
	val wordcounts = warc_words
				.filter(w => antpairslist1.contains(w) || antpairslist2.contains(w))
				.map(word => (word, 1))
				.reduceByKey(_+_)
				
	val wordcountdf = wordcounts.toDF("word", "count")
	wordcountdf.show(5)
	
	val antpairscounts = antpairs.join(
                                wordcountdf.withColumnRenamed("word1", "count1").alias("c1"), 
                                $"c1.word" === antpairs("ant1"), 
                                "right"
                                )
                          .withColumnRenamed("count", "count1")
                          .join(wordcountdf.withColumnRenamed("word2", "count2").alias("c2"), $"c2.word" === antpairs("ant2"))
                          .withColumnRenamed("count", "count2")
                          .select("ant1", "count1", "ant2", "count2")

	antpairscounts.show(5)
	
	val output_path = s"hdfs:///user/s1032005/antonym_counts"
	antpairscounts.coalesce(1)
      .write
      .option("header","true")
      .option("sep",",")
      .mode("overwrite")
      .csv(output_path)
	  
	println("Antonym pair counts Saved!")
	
	spark.stop()
  }
}
