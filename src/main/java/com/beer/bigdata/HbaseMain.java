package com.beer.bigdata;

import lombok.extern.java.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.MessageFormat;


@Log
public class HbaseMain {

    private final Configuration conf;

    {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
    }

    /**
     * table exist
     *
     * @param tableName str
     */
    public void tableExists(String tableName) {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            Admin admin = conn.getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))) {
                log.info(MessageFormat.format("Table {0} exist", tableName));
            } else {
                log.info(MessageFormat.format("Table {0} not exist", tableName));
            }
        } catch (Exception ignore) {

        }
    }


    /**
     * createTable
     *
     * @param tableName str
     */
    public void createTable(String tableName) {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            Admin admin = conn.getAdmin();
            TableDescriptor tableDescriptor = new TableDescriptorBuilder.ModifyableTableDescriptor(TableName.valueOf(tableName));
            admin.createTable(tableDescriptor);
        } catch (Exception ignore) {

        }
    }

    /**
     * put data
     *
     * @param tableName str
     */
    public void putData(String tableName, String rowKey, String columnFamily, String columnName, String value) {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));
            table.put(put);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void main(String[] args) {
        HbaseMain hbaseMain = new HbaseMain();
        hbaseMain.tableExists("student");
        hbaseMain.putData("student", "1000", "name", "first", "zhang");
        hbaseMain.putData("student", "1000", "name", "last", "wuji");
    }

}
