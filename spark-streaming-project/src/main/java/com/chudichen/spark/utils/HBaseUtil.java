package com.chudichen.spark.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Java操作HBase工具类，使用单例编写
 *
 * @author chudichen
 * @date 2020-08-31
 */
public enum HBaseUtil {

    /** 单例实例 */
    INSTANCE;

    private Admin admin = null;
    private Configuration configuration;
    private Connection connection = null;

    HBaseUtil() {
        configuration = new Configuration();
        configuration.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
        configuration.set("hbase.zookeeper.quorum", "localhost");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getTable() {
        try {
            TableName[] names = admin.listTableNames();
            for (TableName tableName : names) {
                System.out.println("Table Name is : " + tableName.getNameAsString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private boolean isExists (String table) throws IOException {
        TableName tableName = TableName.valueOf(table);

        boolean exists = admin.tableExists(tableName);
        if (exists) {
            System.out.println("Table " + tableName.getNameAsString() + " already exists.");
        } else {
            System.out.println("Table " + tableName.getNameAsString() + " not exists.");
        }
        return exists;
    }

    public void createTable(String table, String cf) throws IOException {
        TableName tableName = TableName.valueOf(table);
        System.out.println("To create table named " + table);
        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        HColumnDescriptor columnDesc = new HColumnDescriptor(cf);
        tableDesc.addFamily(columnDesc);

        admin.createTable(tableDesc);
    }

    public void putData(String table, String rowKey, String cf, String qualifier, Long value) {
        byte [] valueBytes = Bytes.toBytes(value);
        putData(table, rowKey, cf, qualifier, valueBytes);
    }

    public void putData(String table, String rowKey, String cf, String qualifier, String value) {
        byte [] valueBytes = Bytes.toBytes(value);
        putData(table, rowKey, cf, qualifier, valueBytes);
    }

    public void putData(String table, String rowKey, String cf, String qualifier, byte [] valueBytes) {
        try {
            byte [] cfBytes = Bytes.toBytes(cf);
            byte [] qualifierBytes = Bytes.toBytes(qualifier);

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(cfBytes, qualifierBytes, valueBytes);

            TableName tableName = TableName.valueOf(table);
            Table getTable = connection.getTable(tableName);
            getTable.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public byte[] getData(String table, String rowKey, String cf, String columnQualifier) {
        try {
            TableName tableName = TableName.valueOf(table);
            Table getTable = connection.getTable(tableName);

            byte[] row = Bytes.toBytes(rowKey);
            Get get = new Get(row);
            byte [] family = Bytes.toBytes(cf);
            get.addFamily(family);
            Result result = getTable.get(get);
            byte[] columnQualifierBytes = Bytes.toBytes(columnQualifier);
            return result.getValue(family, columnQualifierBytes);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public Long getDataLong(String table, String rowKey, String cf, String columnQualifier) {
        byte[] value = getData(table, rowKey, cf, columnQualifier);
        return value == null ? 0L : Bytes.toLong(value);
    }

    /**
     * 检索数据-单行获取
     * @throws IOException
     */
    private void getData(String table, String cf) throws IOException {
        System.out.println("Get data from table " + table + " by family.");
        TableName tableName = TableName.valueOf(table);
        byte [] family = Bytes.toBytes(cf);
        byte [] row = Bytes.toBytes("baidu.com_19991011_20151011");
        Table getTable = connection.getTable(tableName);

        Get get = new Get(row);
        get.addFamily(family);
        // 也可以通过addFamily或addColumn来限定查询的数据
        Result result = getTable.get(get);
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            String qualifier = new String(CellUtil.cloneQualifier(cell));
            String value = new String(CellUtil.cloneValue(cell), "UTF-8");
            System.out.println(qualifier + "\t" + value);
        }
    }

    private void scanTable(String name, String cf) {
        try {
            System.out.println("Scan table " + name + " to browse all datas.");
            TableName tableName = TableName.valueOf(name);
            byte [] family = Bytes.toBytes(cf);

            Scan scan = new Scan();
            scan.addFamily(family);

            Table table = connection.getTable(tableName);
            ResultScanner resultScanner = table.getScanner(scan);
            for (Iterator<Result> it = resultScanner.iterator(); it.hasNext(); ) {
                Result result = it.next();
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    String qualifier = new String(CellUtil.cloneQualifier(cell));
                    String value = new String(CellUtil.cloneValue(cell), "UTF-8");
                    // @Deprecated
                    // LOG.info(cell.getQualifier() + "\t" + cell.getValue());
                    System.out.println(qualifier + "\t" + value);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
