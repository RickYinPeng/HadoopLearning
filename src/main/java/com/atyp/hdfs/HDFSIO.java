package com.atyp.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSIO {

    //把本地d盘上的settings.xml文件上传到HDFS根目录
    @Test
    public void putFileToHDFS() throws URISyntaxException, IOException, InterruptedException {
        // 1.获取对象
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

        // 2.获取输入流
        FileInputStream fis = new FileInputStream(new File("D:\\settings.xml"));

        // 3.获取输出流
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/IOsettings.xml"));

        // 4.流的对拷
        IOUtils.copyBytes(fis, fsDataOutputStream, configuration);

        // 5.关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fsDataOutputStream);
        fs.close();
    }

    // 从HDFS上下载 IOsettings.xml 文件到本地d盘上
    @Test
    public void getFileFromHDFS() throws URISyntaxException, IOException, InterruptedException {
        // 1.获取对象
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

        // 2.获取输入流
        FSDataInputStream open = fs.open(new Path("/IOsettings.xml"));

        // 3.获取输出流
        FileOutputStream fos = new FileOutputStream(new File("D:\\IOsettings.xml"));

        // 4.流的对拷
        IOUtils.copyBytes(open, fos, configuration);

        // 5.关闭资源
        IOUtils.closeStream(open);
        IOUtils.closeStream(fos);
        fs.close();
    }

    // 下载第一块
    @Test
    public void readFileSeek1() throws URISyntaxException, IOException, InterruptedException {

        // 1.获取对象
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

        // 2.获取输入流
        FSDataInputStream open = fs.open(new Path("/hadoop-2.7.2.zip"));

        // 3.获取输出流
        FileOutputStream fos = new FileOutputStream(new File("D:\\hadoop-2.7.2.zip.part1"));

        // 4.流的对拷(只拷128M)
        /**
         * 这个时候如果还使用 IOUtils.copyBytes 相当于全部输出了,所以这个时候使用传统的方式
         */
        //IOUtils.copyBytes(open, fos, configuration);
        byte[] buf  = new byte[1024];
        for (int i = 0; i < 1024*128; i++) {
            open.read(buf);
            fos.write(buf);
        }

        // 5.关闭资源
        IOUtils.closeStream(open);
        IOUtils.closeStream(fos);
        fs.close();
    }

    // 下载第二块
    @Test
    public void readFileSeek2() throws IOException, URISyntaxException, InterruptedException {
        // 1.获取对象
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

        // 2.获取输入流
        FSDataInputStream open = fs.open(new Path("/hadoop-2.7.2.zip"));

        // 3.设置指定读取的起点
        open.seek(1024*1024*128);

        // 4.获取输出流
        FileOutputStream fos = new FileOutputStream(new File("D:\\hadoop-2.7.2.zip.part2"));

        // 5.流的对拷
        IOUtils.copyBytes(open, fos, configuration);

        // 6.关闭资源
        IOUtils.closeStream(open);
        IOUtils.closeStream(fos);
        fs.close();
    }
}
