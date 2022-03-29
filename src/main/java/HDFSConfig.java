import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.spark.SparkDataFile;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.exceptions.*;
import org.apache.log4j.BasicConfigurator;
//import org.apache.logging.log4j.Logger;
//import org.apache.logging.log4j.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.Level;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalog.Catalog;
import org.apache.spark.sql.SparkSession;
import org.mortbay.log.Log;
import org.sparkproject.jetty.io.RuntimeIOException;
//import org.apache.hadoop.util.Shell;
import org.apache.iceberg.spark.SparkSchemaUtil;


public class HDFSConfig {

    private static final Logger LOGGER = Logger.getLogger(HDFSConfig.class.getName());
    public static void main(String[] args) throws InterruptedException {
//        HadoopCatalog();
        SparkInitial();

    }

    public  static void TestSaveTable(){
        // Init Logger
        BasicConfigurator.configure();
        LOGGER.setLevel(Level.INFO);

        // Get rootpath of prject
        String rootPath = System.getProperty("user.dir");
        try{
            LOGGER.info("Start config");
            Configuration conf = new Configuration();

            HadoopTables tables = new HadoopTables(conf);

            Schema schema = new Schema(
                    Types.NestedField.required(1, "level", Types.StringType.get()),
                    Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
                    Types.NestedField.required(3, "message", Types.StringType.get()),
                    Types.NestedField.optional(4, "call_stack", Types.ListType.ofRequired(5, Types.StringType.get()))
            );
            LOGGER.info("Start init spec!");
            PartitionSpec spec = PartitionSpec.builderFor(schema)
                    .hour("event_time")
                    .identity("level")
                    .build();

//            String warehousePath = rootPath;
            String warehousePath = "hdfs://127.0.0.1:9000/data/";
            LOGGER.info("Init hadoop catalog");
            HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
            TableIdentifier name = TableIdentifier.of("/table");
            LOGGER.info("Create Table");
            Table table = catalog.loadTable(name);
            Transaction transaction = table.newTransaction();
//            AppendFiles appendFiles = transaction.newAppend();


            LOGGER.info("OKKOKOKOK");
        }
        catch (Exception e){
            LOGGER.log(Level.WARNING, e.toString());
        }
    }

    public static void HadoopCatalog(){
        /*
        *  Connect with hdfs and show table properties
        * */
        Configuration conf = new Configuration();
        String warehousePath = "hdfs://localhost:9000/raw_data/bg_dichvu.json";
//        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
//        Table table = catalog.loadTable(TableIdentifier.of("/table"));
//        System.out.println(table.properties());
        HadoopTables tables = new HadoopTables(conf);
        Table table = tables.load(warehousePath);
        AppendFiles appenTable = table.newAppend();
//        DataFile dataFile = new SparkDataFile()
//        appenTable.appendFile()
        System.out.println(table.snapshot(1));
        System.out.println(table.name());
//        System.out.println(table.);
        LOGGER.info("Finish");
    }
    public  static void SparkInitial() throws InterruptedException {
        String warehousePath = "hdfs://localhost:9000/raw_data/*";
        /*
        *spark.sql.catalog.hadoop_prod = org.apache.iceberg.spark.SparkCatalog
            spark.sql.catalog.hadoop_prod.type = hadoop
            spark.sql.catalog.hadoop_prod.warehouse = hdfs://nn:8020/warehouse/path
        * */


        SparkSession spark_iceberg = SparkSession
                .builder().master("local")
                .appName("Chi-so-giao")
                .config("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.hadoop_prod.type", "hadoop")
                .config("spark.sql.catalog.hadoop_prod.uri", "hdfs://localhost:9000/raw_data/")
                .getOrCreate();
        long countTimeIce = 0;
        Runnable runnable =
                () -> {
                    //Count with iceberg
                    for(int i = 0;i<10;i++) {
                        if (i==0) continue;
                        long start3 = System.currentTimeMillis();
                        Dataset<Row> bg = spark_iceberg.read().json(warehousePath);
                        bg.show();
                        long start4 = System.currentTimeMillis();
                        countTimeIce += (start4 - start3);
                    }
                };
        Thread thread = new Thread(runnable);
        thread.start();

        SparkSession spark = SparkSession.builder().master("local")
                .appName("Chi-so-giao")
                .getOrCreate();


        // Count without iceberg
        long countTime = 0;
        for (int i = 0;i<10;i++) {
            if (i==0) continue;
            long start1 = System.currentTimeMillis();
            Dataset<Row> bg1 = spark.read().json(warehousePath);
            bg1.show();
            long start2 = System.currentTimeMillis();
            countTime += (start2 - start1);
        }

        thread.join();

//        LOGGER.info(S
        System.out.println("FINISH");
        System.out.println(String.format("RESULT: Normal: %d , Iceberg: %d", countTime, countTimeIce[0]));
    }

}
