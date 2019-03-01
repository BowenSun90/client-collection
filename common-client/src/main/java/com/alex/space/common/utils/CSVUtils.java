package com.alex.space.common.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

/**
 * @author Alex Created by Alex on 2019/3/1.
 */
@Slf4j
public class CSVUtils {

  private static final String CHARSET_NAME = "UTF-8";

  /**
   * 需要调用方负责关闭输出流, 适合多长调用
   */
  public static void writeCsvFile(CSVPrinter csvPrinter,
      List<List<String>> list) throws IOException {

    if (list != null) {
      printList(list, csvPrinter);
    }
  }

  public static void writeCsvFile(String filePath, String[] fileHeaders,
      List<List<String>> list) {
    File file = new File(filePath);
    if (file.exists()) {
      writeCsvFile(filePath, null, list, true);
    } else {
      writeCsvFile(filePath, fileHeaders, list, false);
    }
  }

  public static void writeCsvFile(String filePath, String[] fileHeaders,
      List<List<String>> list, boolean append) {
    CSVFormat csvFileFormat;
    if (fileHeaders == null) {
      csvFileFormat = CSVFormat.DEFAULT;
    } else {
      csvFileFormat = CSVFormat.DEFAULT.withHeader(fileHeaders);
    }

    try (FileOutputStream fileOutputStream = new FileOutputStream(filePath, append);
        BufferedWriter bufferedWriter = new BufferedWriter(
            new OutputStreamWriter(fileOutputStream, CHARSET_NAME));
        CSVPrinter csvPrinter = new CSVPrinter(bufferedWriter, csvFileFormat)
    ) {

      if (list != null) {
        printList(list, csvPrinter);
      }
    } catch (IOException ie) {
      log.error("write csv file:{} ,Error:", filePath, ie);
    }
  }

  /**
   * 读取csv文件 传参数 文件 表头 从第几行开始
   */
  public static List readCsvFile(String filePath, String[] fileHeaders, Integer num) {
    List list = null;
    CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(fileHeaders);
    try (
        BufferedReader br = new BufferedReader(
            new InputStreamReader(new FileInputStream(filePath), CHARSET_NAME));//解决乱码问题
        // 初始化 CSVParser object
        CSVParser csvFileParser = new CSVParser(br, csvFileFormat)
    ) {
      List<CSVRecord> csvRecords = csvFileParser.getRecords();
      List data;
      list = new ArrayList();
      for (int i = num; i < csvRecords.size(); i++) {
        CSVRecord record = csvRecords.get(i);
        data = new ArrayList();
        for (int j = 0; j < fileHeaders.length; j++) {
          data.add(record.get(fileHeaders[j]));
        }
        list.add(data);
      }
    } catch (IOException ie) {
      log.error("write csv file:{} ,Error:", filePath, ie);
    }
    return list;
  }

  private static void printList(List<List<String>> list, CSVPrinter csvPrinter)
      throws IOException {
    List<String> ls;
    for (int i = 0; i < list.size(); i++) {
      ls = list.get(i);
      for (int j = 0; j < ls.size(); j++) {
        csvPrinter.print(ls.get(j));
      }
      csvPrinter.println();// 换行
    }
  }

}
