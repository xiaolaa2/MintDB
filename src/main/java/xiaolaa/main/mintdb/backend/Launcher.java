package xiaolaa.main.mintdb.backend;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import xiaolaa.main.mintdb.backend.dm.DataManager;
import xiaolaa.main.mintdb.backend.server.Server;
import xiaolaa.main.mintdb.backend.tbm.TableManager;
import xiaolaa.main.mintdb.backend.tm.TransactionManager;
import xiaolaa.main.mintdb.backend.utils.Panic;
import xiaolaa.main.mintdb.backend.vm.VersionManager;
import xiaolaa.main.mintdb.backend.vm.VersionManagerImpl;
import xiaolaa.main.mintdb.common.Error;

/**
 * 启动入口
 */
public class Launcher {

    public static final int port = 9999;

    public static final long DEFALUT_MEM = (1<<20)*64;
    public static final long KB = 1 << 10;
	public static final long MB = 1 << 20;
	public static final long GB = 1 << 30;

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        // 获取数据存储文件地址
        options.addOption("open", true, "-open DBPath");
        // 创建数据库文件
        options.addOption("create", true, "-create DBPath");
        options.addOption("mem", true, "-mem 64MB");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options,args);

        if(cmd.hasOption("open")) {
            openDB(cmd.getOptionValue("open"), parseMem(cmd.getOptionValue("mem")));
            return;
        }
        if(cmd.hasOption("create")) {
            createDB(cmd.getOptionValue("create"));
            return;
        }
        System.out.println("Usage: launcher (open|create) DBPath");
    }

    private static void createDB(String path) {
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, DEFALUT_MEM, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager.create(path, vm, dm);
        tm.close();
        dm.close();
    }

    /**
     * 打开数据库并启动服务器。
     * <p>
     * 该方法负责初始化数据库连接、数据管理器、版本管理器、表管理器，并启动服务器监听。
     * 使用指定的数据库路径和内存配额来配置数据库环境。
     * </p>
     *
     * @param path 数据库路径，指定数据库文件的位置。
     * @param mem 数据库内存配额，用于指定数据库在内存中使用的配额。
     */
    private static void openDB(String path, long mem) {
        // 打开事务管理器
        TransactionManager tm = TransactionManager.open(path);
        // 打开数据管理器，初始化数据管理器并关联事务管理器
        DataManager dm = DataManager.open(path, mem, tm);
        // 创建版本管理器，用于管理数据库的版本
        VersionManager vm = new VersionManagerImpl(tm, dm);
        // 打开表管理器，初始化表管理器并关联版本管理和数据管理器
        TableManager tbm = TableManager.open(path, vm, dm);
        // 启动服务器，指定表管理器以提供数据库服务
        new Server(port, tbm).start(); // 启动服务器
    }

    private static long parseMem(String memStr) {
        if(memStr == null || "".equals(memStr)) {
            return DEFALUT_MEM;
        }
        if(memStr.length() < 2) {
            Panic.panic(Error.InvalidMemException);
        }
        String unit = memStr.substring(memStr.length()-2);
        long memNum = Long.parseLong(memStr.substring(0, memStr.length()-2));
        switch(unit) {
            case "KB":
                return memNum*KB;
            case "MB":
                return memNum*MB;
            case "GB":
                return memNum*GB;
            default:
                Panic.panic(Error.InvalidMemException);
        }
        return DEFALUT_MEM;
    }
}
