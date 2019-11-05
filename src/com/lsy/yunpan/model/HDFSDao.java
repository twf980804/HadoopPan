package com.lsy.yunpan.model;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

public class HDFSDao {

	private final Log log = LogFactory.getLog(HDFSDao.class);
	// HDFS访问地址
	private static final String HDFS_PATH = "hdfs://192.168.74.102:9000";
	// hdfs路径
	private String hdfsPath;
	// Hadoop系统配置
	private Configuration conf;

	public HDFSDao(Configuration conf) {
		this(HDFS_PATH, conf);
	}

	public HDFSDao(String hdfs, Configuration conf) {
		this.hdfsPath = hdfs;
		this.conf = conf;
	}


	// 启动函数
	public static void main(String[] args) throws IOException {
		JobConf conf = getConfig();
		HDFSDao hdfs = new HDFSDao(conf);
		hdfs.ls("hdfs://192.168.74.102:9000/");
	}

	// 加载Hadoop配置文件
	public static JobConf getConfig() {
		JobConf conf = new JobConf(HDFSDao.class);
		conf.setJobName("HdfsDAO");
		conf.addResource("classpath:/hadoop/core-site.xml");
		conf.addResource("classpath:/hadoop/hdfs-site.xml");
		conf.addResource("classpath:/hadoop/mapred-site.xml");
		return conf;
	}

	// 在根目录下创建文件夹
	public void mkdirs(String folder) throws IOException {
		Path path = new Path(folder);
		FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(hdfsPath), conf,"root");
			if (!fs.exists(path)) {
			fs.mkdirs(path);
			log.debug("Create: " + folder);
		}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			if (fs != null) {
				fs.close();
			}
		}
	}

	// 某个文件夹的文件列表
	public FileStatus[] ls(String folder) throws IOException {
		Path path = new Path(folder);
		FileStatus[] list = null;
		FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(hdfsPath), conf,"root");
			list = fs.listStatus(path);
			log.debug("ls: " + folder);
			log.debug("==========================================================");
			if (list != null)
				for (FileStatus f : list) {
					log.debug(f.getPath().getName() + ", folder: " + (f.isDir() ? "目录" : "文件") + ", 大小: " + f.getLen()
							/ 1024 + "\n");
				}
			log.debug("==========================================================");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			if (fs != null) {
				fs.close();
			}
		}
		return list;
	}

	//上传文件
	public void copyFile(String local, String remote) throws IOException {
		FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(hdfsPath), conf,"root");
			// remote---/用户/用户下的文件或文件夹
		fs.copyFromLocalFile(new Path(local), new Path(remote));
		log.debug("copy from: " + local + " to " + remote);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			if (fs != null) {
				fs.close();
			}
		}
	}

	// 删除文件或文件夹
	public void rmr(String folder) throws IOException {
		Path path = new Path(folder);
		FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(hdfsPath), conf, "root");
			fs.delete(path,true);
			log.debug("Delete: " + folder);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			if (fs != null) {
				fs.close();
			}
		}
	}

	// 下载文件到本地系统
	public void download(String remote, String local) throws IOException {
		Path path = new Path(remote);
		FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(hdfsPath), conf,"root");
			fs.copyToLocalFile(false,path, new Path(local),true);
			log.debug("download: from" + remote + " to " + local);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			if (fs != null) {
				fs.close();
			}
		}
	}
}
