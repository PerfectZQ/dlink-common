package com.sensetime.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.List;

/**
 * @author zhangqiang
 * @since 2021/4/21 15:29
 */
public class HBase {

    public static Connection getHBaseConnection(String zkHost, String zkPort) {
        Connection conn = null;
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", zkPort);
        configuration.set("hbase.zookeeper.quorum", zkHost);
        /*
         * 在进行插入操作的时候，HBase会挨个检查要插入的列，检查每个列的大小
         * 是否小于`maxKeyValueSize`值，当`cell`的大小大于`maxKeyValueSize`
         * 时，就会抛出`KeyValue size too large`的异常。
         * 方法1：把它设为可以被最大 region size 整除的更大的数，比如10485760(10MB)
         * 方法2：设置为<=0的数，禁用该项检查。
         */
        configuration.set("hbase.client.keyvalue.maxsize", "-1");
        // 解决 java.io.IOException: No FileSystem for scheme: hdfs
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        // configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
        try {
            conn = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static Connection getHBaseConnection() {
        Connection conn = null;
        Configuration configuration = HBaseConfiguration.create();
        /*
         * 在进行插入操作的时候，HBase会挨个检查要插入的列，检查每个列的大小
         * 是否小于`maxKeyValueSize`值，当`cell`的大小大于`maxKeyValueSize`
         * 时，就会抛出`KeyValue size too large`的异常。
         * 方法1：把它设为可以被最大 region size 整除的更大的数，比如10485760(10MB)
         * 方法2：设置为<=0的数，禁用该项检查。
         */
        configuration.set("hbase.client.keyvalue.maxsize", "-1");
        // 解决 java.io.IOException: No FileSystem for scheme: hdfs
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        //configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
        try {
            conn = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 如果表不存在，在创建表
     *
     * @param tableName  namespace:tableName
     * @param connection 如果主动传递一个 connection 对象，需要自己管理 connection 的释放
     *                   不传递则会自动创建一个 connection 对象并在完成操作后立即释放
     */
    public static synchronized void createTableIfNotExist(String tableName, Connection connection, String[] columnFamilies) {
        try {
            Admin admin = connection.getAdmin();
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
                if (columnFamilies == null || columnFamilies.length == 0)
                    throw new IllegalArgumentException("columnFamilies can't be null or empty");
                for (String columnFamily : columnFamilies) {
                    tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamily));
                }
                admin.createTable(tableDescriptorBuilder.build());
            }
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 异步写入 HBase
     *
     * @param hTable namespace:tableName
     * @param puts
     * @return
     * @throws Exception
     */
    public static long asyncPut(String hTable, List<Put> puts) throws Exception {
        long currentTime = System.currentTimeMillis();
        Connection conn = getHBaseConnection();
        final BufferedMutator.ExceptionListener listener = (e, mutator) -> {
            for (int i = 0; i < e.getNumExceptions(); i++) {
                System.out.println("Failed to sent put " + e.getRow(i) + ".");
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(hTable))
                .listener(listener);
        params.writeBufferSize(5 * 1024 * 1024);
        final BufferedMutator mutator = conn.getBufferedMutator(params);
        try {
            mutator.mutate(puts);
            mutator.flush();
        } finally {
            mutator.close();
            conn.close();
        }
        return System.currentTimeMillis() - currentTime;
    }

    /**
     * @param hTable namespace:tableName
     * @param puts
     * @return
     * @throws Exception
     */
    public static long put(String hTable, List<Put> puts) throws Exception {
        long currentTime = System.currentTimeMillis();
        Connection conn = getHBaseConnection();
        HTable htable = (HTable) conn.getTable(TableName.valueOf(hTable));
        try {
            htable.put(puts);
        } finally {
            htable.close();
            conn.close();
        }
        return System.currentTimeMillis() - currentTime;
    }


}

