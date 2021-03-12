//package com.zyccx;
//
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.*;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.util.Bytes;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.NavigableMap;
//
//
//public class HBaseDao {
//    static Configuration config;
//    static Connection connection;
//
//    static {
//        Configuration HBASE_CONFIG = new Configuration();
//        HBASE_CONFIG.set("hbase.zookeeper.quorum", "MDSnn01,MDSnn02,MDSdn01");
//        HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
//
//        config = HBaseConfiguration.create(HBASE_CONFIG);
//        try {
//            connection = ConnectionFactory.createConnection(config);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    /**
//     * 创建表
//     *
//     * @param tableName tableName
//     * @param families  families
//     * @throws IOException
//     */
//    public static void createTable(String tableName, String[] families) throws IOException {
//        final Admin admin = connection.getAdmin();
//        final TableName tabName = TableName.valueOf(tableName);
//
//        if (admin.tableExists(tabName)) {
//            System.out.println(String.format("table %s already exists!", tableName));
//        } else {
//            HTableDescriptor tableDescriptor = new HTableDescriptor(tabName);
//            for (int i = 0; i < families.length; i++) {
//                tableDescriptor.addFamily(new HColumnDescriptor(families[i]));
//            }
//            admin.createTable(tableDescriptor);
//            System.out.println(String.format("create table %s ok.", tableName));
//        }
//    }
//
//    /**
//     * deleteTable
//     *
//     * @param tableName tableName
//     */
//    public static void deleteTable(String tableName) {
//        try {
//            final Admin admin = connection.getAdmin();
//            admin.disableTable(TableName.valueOf(tableName));
//            admin.deleteTable(TableName.valueOf(tableName));
//            System.out.println(String.format("delete table %s ok.", tableName));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    /**
//     * 入库数据
//     *
//     * @param tableName
//     * @param rowKey
//     * @param family
//     * @param qualifier
//     * @param value
//     */
//    public static void addRecord(String tableName, String rowKey, String family, String qualifier, String value) {
//        try {
//            final Table table = connection.getTable(TableName.valueOf(tableName));
//            final Put put = new Put(Bytes.toBytes(rowKey));
//            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
//            table.put(put);
//            System.out.println(String.format(String.format("insert record %s to table %s ok.", rowKey, tableName)));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    //根据row key获取表中的该行数据
//    public static void get(String tableName, String rowKey) throws IOException {
//        get(TableName.valueOf(tableName), rowKey);
//    }
//
//    //根据row key获取表中的该行数据
//    public static void get(TableName tableName, String rowKey) throws IOException {
//        get(connection, tableName, rowKey);
//    }
//
//    //根据row key获取表中的该行数据
//    public static void get(Connection connection, String tableName, String rowKey) throws IOException {
//        get(connection, TableName.valueOf(tableName), rowKey);
//    }
//
//    //根据row key获取表中的该行数据
//    public static void get(Connection connection, TableName tableName, String rowKey) throws IOException {
//        Table table = null;
//        try {
//            table = connection.getTable(tableName);
//            Get get = new Get(Bytes.toBytes(rowKey));
//            Result result = table.get(get);
//            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> navigableMap = result.getMap();
//            for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : navigableMap.entrySet()) {
//
//                System.out.println(String.format("rowKey:%s columnFamily:%s", rowKey, Bytes.toString(entry.getKey())));
//                NavigableMap<byte[], NavigableMap<Long, byte[]>> map = entry.getValue();
//                for (Map.Entry<byte[], NavigableMap<Long, byte[]>> en : map.entrySet()) {
//                    System.out.print(Bytes.toString(en.getKey())+"##");
//                    NavigableMap<Long, byte[]> nm = en.getValue();
//                    for (Map.Entry<Long, byte[]> me : nm.entrySet()) {
//                        System.out.print(me.getKey() + "###");
//                        System.out.println(Bytes.toString(me.getValue()));
//                    }
//
//                }
//                System.out.println("");
//            }
//        } finally {
//            if (table != null) {
//                table.close();
//            }
//        }
//    }
//    public static void scan(String tableName) throws IOException {
//        scan(TableName.valueOf(tableName));
//    }
//
//    public static void scan(TableName tableName) throws IOException {
//        scan(connection, tableName);
//    }
//    public static void scan(Connection connection, String tableName) throws IOException {
//        scan(connection, TableName.valueOf(tableName));
//    }
//
//    public static void scan(Connection connection, TableName tableName) throws IOException {
//        Table table = null;
//        try {
//            table = connection.getTable(tableName);
//            ResultScanner rs = null;
//            try {
//                //Scan scan = new Scan(Bytes.toBytes("u120000"), Bytes.toBytes("u200000"));
//                rs = table.getScanner(new Scan());
//                for (Result r : rs) {
//                    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> navigableMap = r.getMap();
//                    for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : navigableMap.entrySet()) {
//                        System.out.println(String.format("row:%s key:%s", Bytes.toString(r.getRow()), Bytes.toString(entry.getKey())));
//
//                        NavigableMap<byte[], NavigableMap<Long, byte[]>> map = entry.getValue();
//                        for (Map.Entry<byte[], NavigableMap<Long, byte[]>> en : map.entrySet()) {
//                            System.out.print(Bytes.toString(en.getKey()) + "##");
//                            NavigableMap<Long, byte[]> ma = en.getValue();
//                            for (Map.Entry<Long, byte[]> e : ma.entrySet()) {
//                                System.out.print(e.getKey() + "###");
//                                System.out.println(Bytes.toString(e.getValue()));
//                            }
//                        }
//                        System.out.println("");
//                    }
//                }
//            } finally {
//                if (rs != null) {
//                    rs.close();
//                }
//            }
//        } finally {
//            if (table != null) {
//                table.close();
//            }
//        }
//    }
//
//
//    /**
//     * deleteRecord
//     *
//     * @param tableName tableName
//     * @param rowKey    rowKey
//     * @throws IOException
//     */
//    public static void deleteRecord(String tableName, String rowKey) throws IOException {
//        Table table = connection.getTable(TableName.valueOf(tableName));
//        List list = new ArrayList();
//        Delete delete = new Delete(rowKey.getBytes());
//        list.add(delete);
//        table.delete(list);
//        System.out.println("delete record " + rowKey + " ok.");
//    }
//}
