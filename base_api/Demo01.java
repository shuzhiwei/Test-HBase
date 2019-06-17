package base_api;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class Demo01 {
	/*
	 * 创建表
	 */
	@Test
	public void testCreateTable() throws Exception{
	Configuration conf=HBaseConfiguration.create();
	//因为本demo是单机版hbase,其内置zookeeper
	conf.set("hbase.zookeeper.quorum","192.168.10.106:2181");
	
	HBaseAdmin admin = new HBaseAdmin(conf);
	//指定表名
	HTableDescriptor tab1=new HTableDescriptor(TableName.valueOf("tab1"));
	//指定列族名
	HColumnDescriptor colfam1=new HColumnDescriptor("colfam1".getBytes());
	HColumnDescriptor colfam2=new HColumnDescriptor("colfam2".getBytes());
	//指定历史版本存留上限
	colfam1.setMaxVersions(3);
	
	tab1.addFamily(colfam1);
	tab1.addFamily(colfam2);
	//创建表
	admin.createTable(tab1);
	admin.close();
			
	}
	/*
	 * 插入数据
	 */
	@Test
	public void testInsert() throws Exception{
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"linux101:2181");
		//尽量复用Htable对象
		HTable table=new HTable(conf,"tab1");
		Put put=new Put("row-1".getBytes());
		//列族，列,值
		put.add("colfam1".getBytes(),"col1".getBytes(),"aaa".getBytes());
		put.add("colfam1".getBytes(),"col2".getBytes(),"bbb".getBytes());
		table.put(put);
		table.close();
	}
	
	/*
	 * 实验: 100万条数据写入
	 */
	@Test
	public void testInsertMillion() throws Exception{
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"linux101:2181");
		
		HTable table=new HTable(conf,"tab1");
		
		List<Put> puts=new ArrayList<Put>();
		
		long begin=System.currentTimeMillis();
		
		for(int i=1;i<1000000;i++){
			Put put=new Put(("row"+i).getBytes());
			put.add("colfam1".getBytes(),"col".getBytes(),(""+i).getBytes());
			puts.add(put);
			
			//批处理，批大小为:10000
			if(i%10000==0){
				table.put(puts);
				puts=new ArrayList<>();
			}
		}
		long end=System.currentTimeMillis();
		System.out.println(end-begin);
	}

	/*
	 * 获取数据
	 */
	@Test
	public void testGet() throws Exception{
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"linux101:2181");
		
		HTable table=new HTable(conf,"tab1");
		Get get=new Get("row2".getBytes());
		Result result=table.get(get);
		byte[] col1_result=result.getValue("colfam1".getBytes(),"col".getBytes());
		System.out.println(new String(col1_result));
		table.close();
	}
	
	/*
	 * 获取数据集
	 */
	@Test
	public void testScan() throws Exception{
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"linux101:2181");
			
	HTable table = new HTable(conf,"tab1");
	//获取row100及以后的行键的值
	Scan scan = new Scan("row1000000".getBytes());
	ResultScanner scanner = table.getScanner(scan);
	Iterator it = scanner.iterator();
	while(it.hasNext()){
		Result result = (Result) it.next();
		byte [] bs = result.getValue(Bytes.toBytes("colfam1"),Bytes.toBytes("col"));
		String str = Bytes.toString(bs);
		System.out.println(str);
	}
	table.close();
		
	}

	/*
	 * 删除数据
	 */
	@Test
	public void testDelete() throws Exception{
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"linux101:2181");
	
		HTable table = new HTable(conf,"tab1");
		Delete delete=new Delete("row1".getBytes());
		table.delete(delete);
		table.close();
		
	}

	/*
	 * 删除表
	 */
	@Test
	public void testDeleteTable() throws Exception{
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"linux101:2181");
		HBaseAdmin admin=new HBaseAdmin(conf);
		admin.disableTable("tab1".getBytes());
		admin.deleteTable("tab1".getBytes());
		admin.close();
	}


}
