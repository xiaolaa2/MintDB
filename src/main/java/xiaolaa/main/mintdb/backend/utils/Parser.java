package xiaolaa.main.mintdb.backend.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.primitives.Bytes;

public class Parser {

    public static byte[] short2Byte(short value) {
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
    }

    public static short parseShort(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 2);
        return buffer.getShort();
    }

    public static byte[] int2Byte(int value) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    }

    public static int parseInt(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 4);
        return buffer.getInt();
    }

    public static long parseLong(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
        return buffer.getLong();
    }

    public static byte[] long2Byte(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }

    public static ParseStringRes parseString(byte[] raw) {
        int length = parseInt(Arrays.copyOf(raw, 4));
        String str = new String(Arrays.copyOfRange(raw, 4, 4+length));
        return new ParseStringRes(str, length+4);
    }

    public static byte[] string2Byte(String str) {
        byte[] l = int2Byte(str.length());
        return Bytes.concat(l, str.getBytes());
    }

     /**
     * 将字符串转换为唯一的长整型标识符（UID）
     * 该方法通过对字符串中的每个字符进行迭代计算，生成一个长整型数值，用作UID
     *
     * @param key 要转换为UID的字符串
     * @return 生成的UID
     */
    public static long str2Uid(String key) {
        // 初始化种子值，用于在迭代计算中乘以当前结果
        long seed = 13331;
        long res = 0;
        for(byte b : key.getBytes()) {
            // 计算新的结果值：前一个结果 * 种子值 + 当前字符的字节值
            res = res * seed + (long)b;
        }
        // 返回最终计算出的UID
        return res;
    }


}
