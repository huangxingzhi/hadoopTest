package com.hadoop.utils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.fs.FsShell;
import java.util.Collection;
import java.util.Map;

/**
 * @Description: hdfs 操作工具类
 */
public class HdfsUtil {
    @Autowired
    FsShell fsShell;

    /**
     * 创建不定长度HDFS目录
     */
    public void mkdir(String... pathName) {
        fsShell.mkdir(pathName);
    }

    /**
     * 返回不定长度match对应的文件列表
     *
     * @param recursive 是否递归返回
     * @param match     文件路径
     * @return
     */
    public Collection<FileStatus> ls(boolean recursive, String... match) {
        return fsShell.ls(recursive, match);
    }

    /**
     * 查看文件内容
     *
     * @param uris
     */
    public Collection<Path> cat(String... uris) {
        return fsShell.cat(uris);
    }

    /**
     * 上传文件
     *
     * @param localsrc 本地文件
     * @param dst      远程目录
     */
    public void put(String localsrc, String dst) {
        fsShell.put(localsrc, dst);
    }

    /**
     * 下载文件
     *
     * @param src 远程目录
     * @param dst 本地文件
     */
    public void get(String src, String dst) {
        fsShell.get(src, dst);
    }

    /**
     * 移动文件
     *
     * @param src 源文件路径
     * @param dst 移动目标路径
     */
    public void move(String src, String dst) {
        fsShell.mv(src, dst);
    }

    /**
     * 复制文件
     *
     * @param src 源文件路径
     * @param dst 复制目标路径
     */
    public void copy(String src, String dst) {
        fsShell.cp(src, dst);
    }

    /**
     * 删除文件
     *
     * @param recursive 是否递归删除
     * @param uris      待删除文件列表
     */
    public void delete(boolean recursive, String... uris) {
        fsShell.rm(recursive, uris);
    }

    /**
     * 判断uri是否符合条件
     * @param exists 是否存在
     * @param zero 是否为空文件
     * @param directory 是否为目录
     * @param uri 判断的远程路径
     * @return
     */
    public boolean test(boolean exists, boolean zero, boolean directory, String uri){
        return fsShell.test( exists,  zero,  directory,  uri);
    }

    /**
     * 显示目录中所有文件总大小
     * @param uris
     * @return
     */
    public Map<Path, Long> du(String... uris) {
        return fsShell.du(uris);
    }

}
