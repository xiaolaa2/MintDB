package xiaolaa.main.mintdb.backend.dm;

import xiaolaa.main.mintdb.backend.dm.dataItem.DataItem;
import xiaolaa.main.mintdb.backend.dm.logger.Logger;
import xiaolaa.main.mintdb.backend.dm.page.PageOne;
import xiaolaa.main.mintdb.backend.dm.pageCache.PageCache;
import xiaolaa.main.mintdb.backend.tm.TransactionManager;

public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();

    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;
    }

    /**
     * 启动数据库
     * @param path
     * @param mem
     * @param tm
     * @return
     */
    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);

        // 如果数据库上一次没有正常关闭，则恢复数据
        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        dm.fillPageIndex();
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);

        return dm;
    }
}
