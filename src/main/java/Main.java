import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.types.Types;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Main {
    public static TemporaryFolder temp = new TemporaryFolder();
    public static void main(String[] args) throws IOException {
        test();
        return;

    }

    public static void  test(){
        Configuration conf = new Configuration();
        HadoopTables tables = new HadoopTables(conf);

        Schema schema = new Schema(
                Types.NestedField.required(1, "level", Types.StringType.get()),
                Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
                Types.NestedField.required(3, "message", Types.StringType.get()),
                Types.NestedField.optional(4, "call_stack", Types.ListType.ofRequired(5, Types.StringType.get()))
        );

        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .hour("event_time")
                .identity("level")
                .build();

        String table_location = "/home/hoang/";
        Table table = tables.create(schema, spec, table_location);

    }
}
