package com.mapr.db.tools.hfile;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

import org.apache.hadoop.hbase.util.Bytes;

public class Util {
  public static final byte[] KEY_PREFIX = "user".getBytes();
  public static final long FNV_offset_basis_64 = 0xCBF29CE484222325L;
  public static final long FNV_prime_64 = 1099511628211L;
  public static final int ZERO_UNDEF = 0;

  /**
   * 64 bit FNV hash. Produces more "random" hashes than (say) String.hashCode(). Kunal: Converted from static to member
   * function to allow for possibly better speed
   * @param val The value to hash.
   * @return The hash value
   */
  public static long FNVhash64(long val) {
    // from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
    long hashval = FNV_offset_basis_64;

    for (int i = ZERO_UNDEF; i < 8; i++) {
      long octet = val & 0x00ff;
      val = val >> 8;

      hashval = hashval ^ octet;
      hashval = hashval * FNV_prime_64;
      // hashval = hashval ^ octet;
    }
    return Math.abs(hashval);
  }

  static final String CharSpace = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  static final int CharSpaceRange = CharSpace.length();

  public static String randomString(Random randomizer, int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++)
      sb.append(CharSpace.charAt(randomizer.nextInt(CharSpaceRange)));

    return sb.toString();
  }

  static int KEY_SIZE = KEY_PREFIX.length + (Long.SIZE/Byte.SIZE);
  public static byte[] key(long startKey) {
    ByteBuffer buff = ByteBuffer.allocate(KEY_SIZE).order(ByteOrder.BIG_ENDIAN);
    buff.put(KEY_PREFIX).putLong(startKey);
    return buff.array();
    //return (KEY_PREFIX + String.valueOf(FNVhash64(startKey))).getBytes();
  }

}
