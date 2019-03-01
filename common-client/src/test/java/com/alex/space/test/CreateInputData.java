package com.alex.space.test;

import com.alex.space.common.utils.CSVUtils;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

/**
 * @author Alex Created by Alex on 2019/3/1.
 */
@Slf4j
public class CreateInputData {

  public static void main(String[] args) {
    int lineNum = 1000;
    List<String> ipList = generateIpList(lineNum / 20);
    List<String> urlList = generateUrlList(lineNum / 20);

    writeHitsLog(lineNum, ipList, urlList);

    writeIpSource(ipList);

  }

  private static void writeHitsLog(int lineNum, List<String> ipList, List<String> urlList) {

    int i = 0;
    String fileOutput = "data/hits-log.csv";

    FileOutputStream fileOutputStream = null;
    BufferedWriter bufferedWriter = null;
    CSVPrinter csvPrinter = null;

    List<List<String>> lines = new ArrayList<>();

    long currentTs = new Date().getTime();
    long nextTs = currentTs + 300000L;

    try {
      fileOutputStream = new FileOutputStream(fileOutput, true);
      bufferedWriter = new BufferedWriter(
          new OutputStreamWriter(fileOutputStream, "UTF-8"));
      csvPrinter = new CSVPrinter(bufferedWriter, CSVFormat.DEFAULT.withIgnoreHeaderCase());

      while (i < lineNum) {
        String ip = ipList.get(ThreadLocalRandom.current().nextInt(lineNum / 20));
        String sessionId = String.valueOf(ThreadLocalRandom.current().nextInt(lineNum / 20));
        String url = urlList.get(ThreadLocalRandom.current().nextInt(lineNum / 20));
        String ts = String.valueOf(ThreadLocalRandom.current().nextLong(currentTs, nextTs));

        lines.add(Arrays.asList(ip, sessionId, url, ts));
        i++;
      }

      CSVUtils.writeCsvFile(csvPrinter, lines);
    } catch (Exception ex) {
      log.error(ex.getMessage());
    } finally {
      close(fileOutputStream, bufferedWriter, csvPrinter);
    }
  }

  private static void writeIpSource(List<String> ipList) {
    int i = 0;
    String fileOutput = "data/ip-src.csv";

    FileOutputStream fileOutputStream = null;
    BufferedWriter bufferedWriter = null;
    CSVPrinter csvPrinter = null;

    List<List<String>> lines = new ArrayList<>();

    List<String> srcList = Arrays.asList("北京市", "上海市", "杭州市", "广州市", "深圳市");

    try {
      fileOutputStream = new FileOutputStream(fileOutput, true);
      bufferedWriter = new BufferedWriter(
          new OutputStreamWriter(fileOutputStream, "UTF-8"));
      csvPrinter = new CSVPrinter(bufferedWriter, CSVFormat.DEFAULT.withIgnoreHeaderCase());

      for (String ip : ipList) {
        String src = srcList.get(ThreadLocalRandom.current().nextInt(5));

        lines.add(Arrays.asList(ip, src));
        i++;
      }

      CSVUtils.writeCsvFile(csvPrinter, lines);
    } catch (Exception ex) {
      log.error(ex.getMessage());
    } finally {
      close(fileOutputStream, bufferedWriter, csvPrinter);
    }
  }

  private static void close(final FileOutputStream fileOutputStream,
      final BufferedWriter bufferedWriter, final CSVPrinter csvPrinter) {
    try {
      if (csvPrinter != null) {
        csvPrinter.close();
      }
      if (bufferedWriter != null) {
        bufferedWriter.close();
      }
      if (fileOutputStream != null) {
        fileOutputStream.close();
      }
    } catch (Exception e) {
      log.error(e.getMessage());
    }
  }

  private static List<String> generateIpList(int num) {
    List<String> result = new ArrayList<>(num * 2);

    for (int i = 0; i < num; i++) {

      result.add(ThreadLocalRandom.current().nextInt(1, 255)
          + "." + ThreadLocalRandom.current().nextInt(1, 255)
          + "." + ThreadLocalRandom.current().nextInt(1, 255)
          + "." + ThreadLocalRandom.current().nextInt(1, 255)
      );
    }

    return result;

  }

  private static List<String> generateUrlList(int num) {
    List<String> result = new ArrayList<>(num * 2);

    List<String> hosts = Arrays.asList(
        "www.test1.com", "www.test2.org", "www.test3.cn", "test4.site");

    for (int i = 0; i < num; i++) {

      result.add(hosts.get(num % 4)
          + "/" + UUID.randomUUID().toString().replace("-", "").substring(5, 10)
          + "-" + UUID.randomUUID().toString().replace("-", "").substring(3, 6)
          + "/" + UUID.randomUUID().toString().replace("-", "").substring(3, 6)
      );
    }

    return result;

  }

}
