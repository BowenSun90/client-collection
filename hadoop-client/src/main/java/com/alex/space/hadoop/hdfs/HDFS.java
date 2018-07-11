package com.alex.space.hadoop.hdfs;

import com.alex.space.common.CommonConstants;
import com.alex.space.hadoop.config.HadoopConfig;
import com.alex.space.hadoop.config.HadoopConstants;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * HDFS client
 *
 * @author Alex Created by Alex on 2018/6/6.
 */
@Slf4j
public class HDFS {

  private static HadoopConfig hadoopConfig = HadoopConfig.getInstance();

  static {
    initFileSystem();
  }

  /**
   * Init file system
   */
  private static void initFileSystem() {

    Configuration conf = new org.apache.hadoop.conf.Configuration();

    if (fs == null) {
      try {
        String hadoopConfPath = hadoopConfig.getProperty("hadoop.conf.path");
        conf.addResource(new Path(hadoopConfPath + "/hdfs-site.xml"));
        conf.addResource(new Path(hadoopConfPath + "/core-site.xml"));
        conf.addResource(new Path(hadoopConfPath + "/mapred-site.xml"));
        conf.addResource(new Path(hadoopConfPath + "/yarn-site.xml"));

        if (!login(conf)) {
          log.error("can not auth through kerberos.");
          return;

        }
        fs = FileSystem.get(conf);
      } catch (Exception e) {
        log.error(e.getMessage());
        e.printStackTrace();
      }
    }
  }

  /**
   * Kerberos auth
   *
   * @param conf hadoop configuration
   */
  public static Boolean login(Configuration conf) {
    boolean flag = false;

    try {
      if (CommonConstants.KERBEROS_NAME
          .equalsIgnoreCase(hadoopConfig.getProperty(CommonConstants.KERBEROS_NAME))) {

        conf.set(CommonConstants.KERBEROS_PRINCIPAL,
            hadoopConfig.getProperty(CommonConstants.KERBEROS_PRINCIPAL));
        conf.set(CommonConstants.KERBEROS_KEYTAB,
            hadoopConfig.getProperty(CommonConstants.KERBEROS_KEYTAB));

        System.setProperty(CommonConstants.KERBEROS_CONF_JAVA,
            hadoopConfig.getProperty(CommonConstants.KERBEROS_CONF_JAVA));

        flag = loginFromKeytab(conf);
      } else {
        return true;
      }

    } catch (Exception e) {
      log.error("AUTHENTICATION ERROR:", e);
    }
    return flag;

  }

  private static Boolean loginFromKeytab(Configuration conf) {
    boolean flag = false;
    UserGroupInformation.setConfiguration(conf);

    try {
      if (UserGroupInformation.isLoginKeytabBased()) {
        UserGroupInformation.getLoginUser().reloginFromKeytab();
        log.info("Login successfully.");
      } else {
        UserGroupInformation.loginUserFromKeytab(conf.get(CommonConstants.KERBEROS_PRINCIPAL),
            conf.get(CommonConstants.KERBEROS_KEYTAB));
        log.info("Login successfully.");
      }

      flag = true;
    } catch (IOException e) {
      log.error("AUTHENTICATION ERROR:", e);
    }
    return flag;
  }

  public static FileSystem fs;

  public static FileSystem getInstance() {
    return fs;
  }

  /**
   * Check if exists.
   *
   * @param path file path
   */
  public static boolean exists(String path) throws IOException {
    return exists(new Path(path));
  }

  /**
   * Check if exists.
   *
   * @param path file path
   */
  public static boolean exists(Path path) throws IOException {
    return fs.exists(path);
  }

  /**
   * Create an FSDataOutputStream at the indicated Path.
   *
   * @param path file path to create
   * @param overwrite overwrite or not if file exist
   * @param bufferSize write buffer size
   */
  public static OutputStream create(String path, boolean overwrite, int bufferSize)
      throws IOException {
    return create(new Path(path), overwrite, bufferSize);
  }

  /**
   * Create an FSDataOutputStream at the indicated Path.
   *
   * @param path file path to create
   * @param overwrite overwrite or not if file exist
   * @param bufferSize write buffer size
   */
  public static OutputStream create(Path path, boolean overwrite, int bufferSize)
      throws IOException {
    return fs.create(path, overwrite, bufferSize);
  }

  /**
   * Create a directory path at the indicated Path.
   *
   * @param path directory path
   */
  public static boolean mkdirs(String path) throws IOException {
    return mkdirs(new Path(path));
  }

  /**
   * Create a directory path at the indicated Path.
   *
   * @param path directory path
   */
  public static boolean mkdirs(Path path) throws IOException {
    return fs.mkdirs(path);
  }

  /**
   * Opens an InputStream at the indicated Path.
   *
   * @param path open file path
   */
  public static InputStream open(String path) throws IOException {
    return open(new Path(path));
  }

  /**
   * Opens an InputStream at the indicated Path.
   *
   * @param path open file path
   */
  public static InputStream open(Path path) throws IOException {
    return open(path, fs.getConf().getInt("io.file.buffer.size", 4096));
  }

  /**
   * Opens an InputStream at the indicated Path.
   *
   * @param path open file path
   * @param bufferSize read buffer size
   */
  public static InputStream open(String path, int bufferSize) throws IOException {
    return open(new Path(path), bufferSize);
  }

  /**
   * Opens an InputStream at the indicated Path.
   *
   * @param path open file path
   * @param bufferSize read buffer size
   */
  public static InputStream open(Path path, int bufferSize) throws IOException {
    return fs.open(path, bufferSize);
  }

  /**
   * Move file or directory
   *
   * @param srcPath source path
   * @param dstPath target path
   */
  public static boolean move(String srcPath, String dstPath) throws IOException {
    return fs.rename(new Path(srcPath), new Path(dstPath));
  }

  /**
   * copy file
   *
   * @param srcPath source path
   * @param dstPath target path
   * @param overwrite overwrite or not if path exist
   */
  public static void copyFile(String srcPath, String dstPath, boolean overwrite)
      throws IOException {
    InputStream is = fs.open(new Path(srcPath));
    OutputStream os = fs.create(new Path(dstPath), overwrite);
    IOUtils.copyBytes(is, os, HadoopConstants.BUFFER_SIZE, true);
  }

  /**
   * Copy folder as a file
   *
   * @param srcFolderPath source folder path
   * @param dstPath target path
   * @param overwrite overwrite or not if path exist
   */
  public static void copyFolderAsFile(String srcFolderPath, String dstPath, boolean overwrite)
      throws IOException {
    FileStatus[] files = listFiles(srcFolderPath);

    OutputStream os = fs.create(new Path(dstPath), overwrite);
    if (files != null) {
      for (FileStatus file : files) {
        if (file.isFile()) {
          InputStream is = fs.open(file.getPath());
          IOUtils.copyBytes(is, os, HadoopConstants.BUFFER_SIZE, true);
        }
      }
    }
  }

  /**
   * Copy folder as a zip file
   *
   * @param srcFolderPath source folder path
   * @param dstPath target path
   * @param overwrite overwrite or not if path exist
   */
  public static void copyFolderAsZipFile(String srcFolderPath, String dstPath, boolean overwrite)
      throws IOException {
    HdfsInputStream hdfsIn = new HdfsInputStream(srcFolderPath, true);
    HdfsWriter hw = new HdfsWriter(dstPath);
    while (true) {
      InputStream in = hdfsIn.next();
      if (in == null) {
        break;
      }
      hw.write(in, false);
    }
    hw.close();
  }

  /**
   * delete file
   *
   * @param filePath file path to delete.
   * @param recursive if path is a directory and set to true, the directory is deleted else throws
   * an exception. In case of a file the recursive can be set to either true or false
   */
  public static boolean delete(String filePath, boolean recursive) throws IOException {
    return fs.delete(new Path(filePath), recursive);
  }

  /**
   * Get list of file at the indicated Path.
   *
   * @param dir dir path
   */
  private static FileStatus[] listFiles(String dir) {
    Path path = new Path(dir);
    FileStatus[] files = null;
    try {
      files = fs.listStatus(path);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return files;
  }

  /**
   * load list of folder at the indicated Path.
   *
   * @param path folder path
   */
  public static List<Path> loadFolderList(String path) {
    List<Path> folderList = new ArrayList<>();
    FileStatus[] files = listFiles(path);
    if (files != null) {
      for (FileStatus file : files) {
        if (file.isDirectory()) {
          folderList.add(file.getPath());
        }
      }
    }
    return folderList;
  }

  /**
   * load list of file at the indicated Path.
   *
   * @param path folder path
   * @param recursive recursive or not
   */
  public static List<Path> loadFileList(String path, boolean recursive) throws IOException {
    List<Path> fileList = new ArrayList<>();
    loadFileList(path, recursive, fileList);
    return fileList;
  }

  /**
   * load list of file at the indicated Paths.
   *
   * @param path folder path
   * @param recursive recursive or not
   * @param list list of folder
   */
  private static void loadFileList(String path, boolean recursive, List<Path> list) {
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

  /**
   * Copy folder
   *
   * @param srcFolderPath source folder path
   * @param dstFolderPath target folder path
   * @param overwrite overwrite or not if folder exist
   */
  public static void copyFolder(String srcFolderPath, String dstFolderPath, boolean overwrite)
      throws IOException {
    FileStatus[] files = listFiles(srcFolderPath);
    if (files != null) {
      for (FileStatus file : files) {
        if (file.isFile()) {
          copyFile(srcFolderPath + "/" + file.getPath().getName(),
              dstFolderPath + "/" + file.getPath().getName(), overwrite);
        }
      }
    }
  }

}