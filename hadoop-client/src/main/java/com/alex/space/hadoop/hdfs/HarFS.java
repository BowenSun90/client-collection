package com.alex.space.hadoop.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;

/**
 * HarFileSystem
 *
 * @author Alex Created by Alex on 2018/7/10.
 */
@Slf4j
public class HarFS {

  private HarFileSystem fs = null;
  private static HarFS hfs = null;

  public static HarFS initHFS(String harPath) throws IOException, URISyntaxException {
    if (hfs == null) {
      hfs = new HarFS();
    }
    hfs.init(harPath);
    return hfs;
  }

  public void init(String harPath) throws IOException, URISyntaxException {
    if (fs == null) {
      fs = new HarFileSystem(HDFS.fs);
    }
    fs.initialize(new URI(harPath), fs.getConf());
  }

  private Path checkPath(String path) {
    if (path != null) {
      if (path.indexOf("/") == 0) {
        return new Path("har://" + path);
      } else if (path.indexOf("har://") == 0) {
        return new Path(path);
      }
    }
    return null;
  }

  /**
   * Check if exists.
   */
  public boolean exists(String path) throws IOException {
    return exists(checkPath(path));
  }

  /**
   * Check if exists.
   */
  public boolean exists(Path path) throws IOException {
    return fs.exists(path);
  }

  /**
   * Opens an InputStream at the indicated Path.
   */
  public InputStream open(String path) throws IOException {
    return open(checkPath(path));
  }

  /**
   * Opens an InputStream at the indicated Path.
   */
  public InputStream open(Path path) throws IOException {
    return open(path, fs.getConf().getInt("io.file.buffer.size", 4096));
  }

  /**
   * Opens an InputStream at the indicated Path.
   */
  public InputStream open(String path, int bufferSize) throws IOException {
    return open(checkPath(path), bufferSize);
  }

  /**
   * Opens an InputStream at the indicated Path.
   */
  public InputStream open(Path path, int bufferSize) throws IOException {
    return fs.open(path, bufferSize);
  }

  /**
   * 获取目标目录下所有文件
   *
   * @param dir 相对于baseDir的文件目录
   */
  private FileStatus[] listFiles(String dir) {
    Path path = checkPath(dir);
    FileStatus[] files = null;
    try {
      files = fs.listStatus(path);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return files;
  }

  public List<Path> loadFileList(String path, boolean recursive)
      throws IOException {
    List<Path> fileList = new ArrayList<>();
    loadFileList(path, recursive, fileList);
    return fileList;
  }

  private void loadFileList(String path, boolean recursive, List<Path> list) {
    FileStatus[] files = listFiles(path);
    if (files != null) {
      for (FileStatus file : files) {
        if (file.isDirectory()) {
          if (recursive) {
            loadFileList(file.getPath().toString(), recursive, list);
          }
        } else {
          list.add(file.getPath());
        }
      }
    }
  }
}
