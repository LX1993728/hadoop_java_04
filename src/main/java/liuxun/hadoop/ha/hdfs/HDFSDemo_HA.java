package liuxun.hadoop.ha.hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSDemo_HA {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("dfs.nameservices", "ns1");
		conf.set("dfs.ha.namenodes.ns1", "nn1,nn2");
		conf.set("dfs.namenode.rpc-address.ns1.nn1", "hadoop1:9000");
		conf.set("dfs.namenode.rpc-address.ns1.nn2", "hadoop2:9000");
		conf.set("dfs.client.failover.proxy.provider.ns1",
				"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
		// FileSystem fs = FileSystem.get(new URI("hdfs://ns1"), conf);
		// 测试下载
		// InputStream in = fs.open(new Path("/profile"));
		// OutputStream out = new FileOutputStream("/Users/liuxun/Downloads/p.txt");
		// IOUtils.copyBytes(in, out, 4096, true);
	
		// 测试上传
		 FileSystem fs = FileSystem.get(new URI("hdfs://ns1"), conf,"root");
		InputStream in = new FileInputStream("/Users/liuxun/Downloads/a.txt");
		OutputStream out = fs.create(new Path("/a"));
		IOUtils.copyBytes(in, out, 4096, true);
	
	}
}
