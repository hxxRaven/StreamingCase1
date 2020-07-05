package spark.project.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Hbase操作工具类,建议参与单例模式
 * */

public class HBaseUtils {
    HBaseAdmin admin = null;
    Configuration conf = null;

    private HBaseUtils(){
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.226.130:2181,192.168.226.131:2181,192.168.226.132:2181");
        conf.set("hbase.rootdir", "hdfs://192.168.226.130:9000/hbase");

        try {
            admin = new HBaseAdmin(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HBaseUtils instance = null;

    public static synchronized HBaseUtils getInstance() {
        if(null == instance) {
            instance = new HBaseUtils();
        }
        return instance;
    }
    //根据表名获取table实例
    public HTable getTable(String tableName) {
        HTable table = null;

        try {
            table = new HTable(conf, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    //添加数据到表
    public void put(String tableName, String rowkey, String cf, String columns, String value) {
        HTable table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(cf), Bytes.toBytes(columns), Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        HTable table = HBaseUtils.getInstance().getTable("course_clickcount");
//        System.out.println(table.getName().getNameAsString());
        String tableName = "course_clickcount";
        String rowkey = "20171111_88";
        String cf = "info";
        String columns = "click_count";
        String value = "2";
        HBaseUtils.getInstance().put(tableName, rowkey, cf, columns, value);

    }

}
