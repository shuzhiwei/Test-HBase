package filter_api;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.Test;


public class Demo {
	/*
	 * Scanner代码
	 */
	@Test
	public void testScanner() throws Exception{
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"linux101:2181");
		HTable table = new HTable(conf,"tab1".getBytes());
		Scan scan=new Scan();
		scan.setStartRow("row1".getBytes());
		scan.setStopRow("row50".getBytes());
		
		ResultScanner rs=table.getScanner(scan);
		
		Result r = null;
		while((r = rs.next())!=null){
			String rowKey=new String(r.getRow());
			String col1Value=new String(r.getValue("colfam1".getBytes(), "col".getBytes()));
			System.out.println(rowKey+":"+col1Value);
		}


	}
	
	/*
	 * 使用扫描器查询数据
	 */
	@Test
	public void scanData() throws Exception{
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"linux101:2181");
		HTable table=new HTable(conf, "tab1".getBytes());
		
		//--指定扫描行键的起始范围和终止范围
		Scan scan=new Scan();
		scan.setStartRow("row1".getBytes());
		scan.setStopRow("row30".getBytes());

		
		ResultScanner scanner= table.getScanner(scan);
		//--获取结果的迭代器
		Iterator<Result> it=scanner.iterator();
		while(it.hasNext()){
			Result result=it.next();
			//--通过result对象获取指定列族的列的数据
			byte[] value=result.getValue("colfam1".getBytes(),"col".getBytes());
			System.out.println(new String(value));
		}
		scanner.close();
	}
	
	/*
	 * 正则过滤器
	 */
	@Test
	public void regexpData() throws Exception{
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"linux101:2181");
		HTable table=new HTable(conf, "tab1".getBytes());
		
		Scan scan=new Scan();
		
		//--正则过滤器，匹配行键含3的行数据
		Filter filter=new RowFilter(CompareOp.EQUAL,new RegexStringComparator("^.*3.*$"));
		//--加入过滤器
		scan.setFilter(filter);
		
		ResultScanner scanner= table.getScanner(scan);
		//--获取结果的迭代器
		Iterator<Result> it=scanner.iterator();
		while(it.hasNext()){
			Result result=it.next();
			//--通过result对象获取指定列族的列的数据
			byte[] value=result.getValue("colfam1".getBytes(),"col".getBytes());
			System.out.println(new String(value));
		}
		scanner.close();

	}
	
	/*
	 * 行键比较过滤器
	 */
	@Test
	public void compareData() throws Exception{
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"linux101:2181");
		HTable table=new HTable(conf, "tab1".getBytes());
		
		Scan scan=new Scan();
		
		//--行键比较过滤器，下例是匹配小于或等于指定行键的行数据。
		Filter filter=new RowFilter(CompareOp.LESS_OR_EQUAL,new BinaryComparator("row90".getBytes()));
		scan.setFilter(filter);
		
		ResultScanner scanner= table.getScanner(scan);
		
		Iterator<Result> it=scanner.iterator();
		while(it.hasNext()){
			Result result=it.next();
			byte[] value=result.getValue("colfam1".getBytes(),"col".getBytes());
			System.out.println(new String(value));
		}
		scanner.close();
	}

	/*
	 * 行键前缀过滤器
	 */
	@Test
	public void preData() throws Exception{
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"linux101:2181");
		HTable table=new HTable(conf, "tab1".getBytes());
		
		Scan scan=new Scan();
		
		//--行键前缀过滤器
		Filter filter=new PrefixFilter("row3".getBytes());
		scan.setFilter(filter);
		
		ResultScanner scanner= table.getScanner(scan);
		
		Iterator<Result> it=scanner.iterator();
		while(it.hasNext()){
			Result result=it.next();
			byte[] value=result.getValue("colfam1".getBytes(),"col".getBytes());
			System.out.println(new String(value));
		}
		scanner.close();
	}

	/*
	 * 列值过滤器
	 */
	@Test
	public void columnData() throws Exception{
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"linux101:2181");
		HTable table=new HTable(conf, "tab1".getBytes());
		
		Scan scan=new Scan();
		
		//--列值过滤器
		Filter filter = new SingleColumnValueFilter("colfam1".getBytes(),"col".getBytes(), CompareOp.EQUAL, "10".getBytes());
		
		scan.setFilter(filter);
		
		ResultScanner scanner= table.getScanner(scan);
		//--获取结果的迭代器
		Iterator<Result> it=scanner.iterator();
		while(it.hasNext()){
			Result result=it.next();
			/*byte[] name=result.getValue("cf1".getBytes(),"name".getBytes());
			byte[] age=result.getValue("cf1".getBytes(),"age".getBytes());
			System.out.println(new String(name)+":"+new String(age));*/
			byte[] colValue = result.getValue("colfam1".getBytes(), "col".getBytes());
			System.out.println(new String(colValue));
		}
		scanner.close();
	}



}
