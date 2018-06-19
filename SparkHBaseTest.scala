import org.apache.spark.SparkContext

/***import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{HTableDescriptor,HColumnDescriptor}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{Put,HTable}
import org.apache.log4j._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result***/

object SparkHBaseTest {

  def main(args: Array[String]) {
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "SparkHBaseTest")

    println("hello spark hbase ---> 1")

    //val conf = HBaseConfiguration.create()
    /***val tablename = "SparkHBasesTable"
    conf.set(TableInputFormat.INPUT_TABLE,tablename)
    val admin = new HBaseAdmin(conf)
    if(!admin.isTableAvailable(tablename)){
      print("creating table:"+tablename+"\t")
      val tableDescription = new HTableDescriptor(tablename)
      tableDescription.addFamily(new HColumnDescriptor("cf".getBytes()));
      admin.createTable(tableDescription);
    } else {
      print("table already exists")
    }

    val table = new HTable(conf,tablename);
    for(x <- 1 to 10){
      var p = new Put(new String("row" + x).getBytes());
      p.add("cf".getBytes(),"column1".getBytes(),new String("value" + x).getBytes());
      table.put(p);
      print("Data Entered In Table")
    }
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable],classOf[Result])
    print("RecordCount->>"+hBaseRDD.count())
    sc.stop()***/
  }
}
