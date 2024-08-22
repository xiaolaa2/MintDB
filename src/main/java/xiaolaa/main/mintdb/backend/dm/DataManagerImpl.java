package xiaolaa.main.mintdb.backend.dm;

import xiaolaa.main.mintdb.backend.common.AbstractCache;
import xiaolaa.main.mintdb.backend.dm.dataItem.DataItem;
import xiaolaa.main.mintdb.backend.dm.dataItem.DataItemImpl;
import xiaolaa.main.mintdb.backend.dm.logger.Logger;
import xiaolaa.main.mintdb.backend.dm.page.Page;
import xiaolaa.main.mintdb.backend.dm.page.PageOne;
import xiaolaa.main.mintdb.backend.dm.page.PageX;
import xiaolaa.main.mintdb.backend.dm.pageCache.PageCache;
import xiaolaa.main.mintdb.backend.dm.pageIndex.PageIndex;
import xiaolaa.main.mintdb.backend.dm.pageIndex.PageInfo;
import xiaolaa.main.mintdb.backend.tm.TransactionManager;
import xiaolaa.main.mintdb.backend.utils.Panic;
import xiaolaa.main.mintdb.backend.utils.Types;
import xiaolaa.main.mintdb.common.Error;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl)super.get(uid);
        if(!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        PageInfo pi = null;
        for(int i = 0; i < 5; i ++) {
            // 尝试获取空闲页
            pi = pIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                // 如果没有空闲页，则新建一个页面
                int newPgno = pc.newPage(PageX.initRaw());
                // 将新创建的页面插入pIndex
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if(pi == null) {
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            pg = pc.getPage(pi.pgno);
            // 获取一个insertLog
            byte[] log = Recover.insertLog(xid, pg, raw);

            // 将log写入日志文件
            logger.log(log);

            // 获得数据插入后的位置
            short offset = PageX.insert(pg, raw);

            // 持久化页面
            pg.release();
            // 使用uid映射pgno和offset
            return Types.addressToUid(pi.pgno, offset);

        } finally {
            // 将取出的pg重新插入pIndex
            if(pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    // 为xid生成update日志
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    // 在创建文件时初始化PageOne
    void initPageOne() {
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    // 在打开已有文件时时读入PageOne，并验证正确性
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        // 检测上一次数据库有没有正常关闭
        return PageOne.checkVc(pageOne);
    }

    // 初始化pageIndex
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }
    
}
