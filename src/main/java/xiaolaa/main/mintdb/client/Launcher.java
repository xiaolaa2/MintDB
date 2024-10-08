package xiaolaa.main.mintdb.client;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import xiaolaa.main.mintdb.transport.Encoder;
import xiaolaa.main.mintdb.transport.Packager;
import xiaolaa.main.mintdb.transport.Transporter;

public class Launcher {
    public static void main(String[] args) throws UnknownHostException, IOException {
        // 建立连接
        Socket socket = new Socket("127.0.0.1", 9999);
        Encoder e = new Encoder();
        Transporter t = new Transporter(socket);
        Packager packager = new Packager(t, e);

        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();
    }
}
