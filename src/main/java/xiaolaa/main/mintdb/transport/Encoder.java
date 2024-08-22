package xiaolaa.main.mintdb.transport;

import java.util.Arrays;

import com.google.common.primitives.Bytes;

import xiaolaa.main.mintdb.common.Error;

public class Encoder {

    /**
     * 编码结构[Flag][data]
     * 若 flag 为 0，表示发送的是数据，那么 data 即为这份数据本身；
     * 如果 flag 为 1，表示发送的是错误，data 是 Exception.getMessage() 的错误提示信息。如下：
     * @param pkg
     * @return
     */
    public byte[] encode(Package pkg) {
        if(pkg.getErr() != null) {
            // 存在错误
            Exception err = pkg.getErr();
            String msg = "Intern server error!";
            if(err.getMessage() != null) {
                msg = err.getMessage();
            }
            return Bytes.concat(new byte[]{1}, msg.getBytes());
        } else {
            return Bytes.concat(new byte[]{0}, pkg.getData());
        }
    }

    public Package decode(byte[] data) throws Exception {
        if(data.length < 1) {
            throw Error.InvalidPkgDataException;
        }
        if(data[0] == 0) {
            return new Package(Arrays.copyOfRange(data, 1, data.length), null);
        } else if(data[0] == 1) {
            // 获取错误
            return new Package(null, new RuntimeException(new String(Arrays.copyOfRange(data, 1, data.length))));
        } else {
            throw Error.InvalidPkgDataException;
        }
    }

}
