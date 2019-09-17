package com.atyp.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.yarn.api.records.URL;
import org.junit.Test;
import org.mortbay.thread.QueuedThreadPool;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class HDFSClient {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        //configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
        //1.获取hdfs客户端对象
        //FileSystem fs = FileSystem.get(configuration);
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

        //2.在hdfs上创建路径
        fs.mkdirs(new Path("/0529/cainiao/hanhuahua"));

        //3.关闭资源
        fs.close();

        System.out.println("over");
    }

    // 1.文件上传
    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        //1.获取fs对象
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

        //2.执行上传API
        //fs.copyFromLocalFile(new Path("D:\\settings.xml"), new Path("/setting3.xml"));
        fs.copyFromLocalFile(new Path("D:\\Develop\\BaiduYun\\BaiduNetdiskDownload\\2.资料\\01_jar包\\01_win10下编译过的hadoop jar包\\hadoop-2.7.2.zip"), new Path("/hadoop-2.7.2.zip"));

        //3.关闭资源
        fs.close();
    }

    // 2.文件下载
    @Test
    public void testCopyToLocalFile() throws URISyntaxException, IOException, InterruptedException {
        // 1.获取对象
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

        // 2.执行下载操作
//        fs.copyToLocalFile(new Path("/setting.xml"), new Path("D:\\WorkSpace\\settingFromHadoop.xml"));
        fs.copyToLocalFile(false, new Path("/setting.xml"), new Path("D:\\WorkSpace\\settingFromHadoopDelSrc.xml"), true);
        // 3.关闭资源
        fs.close();
    }

    // 3.文件删除
    @Test
    public void testDelete() throws URISyntaxException, IOException, InterruptedException {
        // 1.获取对象
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

        // 2.文件删除
        /**
         * path:要删除的文件|目录
         * recursive:如果是文件夹就需要设置位true，递归去删除，如果是文件那就无所谓true和false都可以
         */
        fs.delete(new Path("/0529"), true);

        // 3.关闭资源
        fs.close();
    }

    // 4.文件更名
    @Test
    public void testRename() throws URISyntaxException, IOException, InterruptedException {
        // 1.获取对象
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

        // 2.执行更名操作
        /**
         * 将 /setting.xml 更名为 /settingRename.xml
         */
        fs.rename(new Path("/setting.xml"), new Path("/settingRename.xml"));

        // 3.关闭资源
        fs.close();
    }

    // 5.文件详情查看
    @Test
    public void testListFiles() throws URISyntaxException, IOException, InterruptedException {
        // 1.获取对象
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

        // 2.查看文件详情
        /**
         * path:查看的目录
         * recursive:是否递归查看
         */
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            //查看文件名称、权限、长度、块信息
            System.out.println(fileStatus.getPath().getName());// 文件名称
            System.out.println(fileStatus.getPermission());//文件权限
            System.out.println(fileStatus.getLen());//文件长度

            /**
             * 数组：一个文件可能存储在多个文件快上，所以是数组
             */
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println("文件块大小：" + blockLocations.length);
            for (BlockLocation blockLocation : blockLocations) {
                //返回所有的主机名称
                String[] hosts = blockLocation.getHosts();
                System.out.println("每个文件快的ip数量:" + hosts.length);
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("-------------------------------------------");
        }
        // 3.关闭资源
        fs.close();
    }

    // 6.判断是文件还是文件夹
    @Test
    public void testListStatus() throws URISyntaxException, IOException, InterruptedException {
        // 1.获取对象
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

        // 2.判断操作
        //获取某一个文件的状态
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if(fileStatus.isFile()){
                //文件
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {
                //文件夹
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }
        // 3.关闭资源
        fs.close();
    }



}
