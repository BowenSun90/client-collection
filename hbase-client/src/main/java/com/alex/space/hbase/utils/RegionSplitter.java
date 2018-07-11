package com.alex.space.hbase.utils;

import com.google.common.base.Preconditions;
import java.math.BigInteger;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase API
 *
 * @author Alex Created by Alex on 2018/7/10.
 */
public interface RegionSplitter {

  /**
   * split rowkey with special number of region number.
   */
  byte[][] split(int regionNum) throws Exception;

  @Getter
  @AllArgsConstructor
  enum RegionSplitterStrategy {
    HexStringStrategy(HexStringSplitter.class);

    private Class<? extends RegionSplitter> clazz;
  }

  /**
   * Hex String Splitter
   */
  final class HexStringSplitter implements RegionSplitter {

    final static String DEFAULT_MIN_HEX = "00000000";
    final static String DEFAULT_MAX_HEX = "FFFFFFFF";

    private String firstRow = DEFAULT_MIN_HEX;
    private BigInteger firstRowInt = BigInteger.ZERO;
    private String lastRow = DEFAULT_MAX_HEX;
    private BigInteger lastRowInt = new BigInteger(lastRow, 16);
    private int rowComparisonLength = lastRow.length();

    /**
     * split with 2 region number
     */
    public byte[] split(byte[] start, byte[] end) {
      BigInteger s = convertToBigInteger(start);
      BigInteger e = convertToBigInteger(end);
      Preconditions.checkArgument(!e.equals(BigInteger.ZERO));
      return convertToByte(split2(s, e));
    }

    @Override
    public byte[][] split(int n) {
      Preconditions.checkArgument(lastRowInt.compareTo(firstRowInt) > 0,
          "last row (%s) is configured less than first row (%s)", lastRow,
          firstRow);
      // +1 to range because the last row is inclusive
      BigInteger range = lastRowInt.subtract(firstRowInt).add(BigInteger.ONE);
      Preconditions.checkState(range.compareTo(BigInteger.valueOf(n)) >= 0,
          "split granularity (%s) is greater than the range (%s)", n, range);

      BigInteger[] splits = new BigInteger[n - 1];
      BigInteger sizeOfEachSplit = range.divide(BigInteger.valueOf(n));
      for (int i = 1; i < n; i++) {
        // NOTE: this means the last region gets all the slop.
        // This is not a big deal if we're assuming n << MAXHEX
        splits[i - 1] = firstRowInt.add(sizeOfEachSplit.multiply(BigInteger
            .valueOf(i)));
      }
      return convertToBytes(splits);
    }

    public byte[] firstRow() {
      return convertToByte(firstRowInt);
    }

    public byte[] lastRow() {
      return convertToByte(lastRowInt);
    }

    public void setFirstRow(String userInput) {
      firstRow = userInput;
      firstRowInt = new BigInteger(firstRow, 16);
    }

    public void setLastRow(String userInput) {
      lastRow = userInput;
      lastRowInt = new BigInteger(lastRow, 16);
      // Precondition: lastRow > firstRow, so last's length is the greater
      rowComparisonLength = lastRow.length();
    }

    public byte[] strToRow(String in) {
      return convertToByte(new BigInteger(in, 16));
    }

    public String rowToStr(byte[] row) {
      return Bytes.toStringBinary(row);
    }

    public String separator() {
      return " ";
    }

    public void setFirstRow(byte[] userInput) {
      firstRow = Bytes.toString(userInput);
    }

    public void setLastRow(byte[] userInput) {
      lastRow = Bytes.toString(userInput);
    }

    /**
     * Divide 2 numbers in half (for split algorithm)
     *
     * @param a number #1
     * @param b number #2
     * @return the midpoint of the 2 numbers
     */
    public BigInteger split2(BigInteger a, BigInteger b) {
      return a.add(b).divide(BigInteger.valueOf(2)).abs();
    }

    /**
     * Returns an array of bytes corresponding to an array of BigIntegers
     *
     * @param bigIntegers numbers to convert
     * @return bytes corresponding to the bigIntegers
     */
    public byte[][] convertToBytes(BigInteger[] bigIntegers) {
      byte[][] returnBytes = new byte[bigIntegers.length][];
      for (int i = 0; i < bigIntegers.length; i++) {
        returnBytes[i] = convertToByte(bigIntegers[i]);
      }
      return returnBytes;
    }

    /**
     * Returns the bytes corresponding to the BigInteger
     *
     * @param bigInteger number to convert
     * @param pad padding length
     * @return byte corresponding to input BigInteger
     */
    public static byte[] convertToByte(BigInteger bigInteger, int pad) {
      String bigIntegerString = bigInteger.toString(16);
      bigIntegerString = StringUtils.leftPad(bigIntegerString, pad, '0');
      return Bytes.toBytes(bigIntegerString);
    }

    /**
     * Returns the bytes corresponding to the BigInteger
     *
     * @param bigInteger number to convert
     * @return corresponding bytes
     */
    public byte[] convertToByte(BigInteger bigInteger) {
      return convertToByte(bigInteger, rowComparisonLength);
    }

    /**
     * Returns the BigInteger represented by the byte array
     *
     * @param row byte array representing row
     * @return the corresponding BigInteger
     */
    public BigInteger convertToBigInteger(byte[] row) {
      return (row.length > 0) ? new BigInteger(Bytes.toString(row), 16)
          : BigInteger.ZERO;
    }

    @Override
    public String toString() {
      return this.getClass().getSimpleName() + " [" + rowToStr(firstRow())
          + "," + rowToStr(lastRow()) + "]";
    }
  }
}
