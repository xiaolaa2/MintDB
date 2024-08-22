package xiaolaa.main.mintdb.backend.vm;

import xiaolaa.main.mintdb.backend.tm.TransactionManager;

public class Visibility {

    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if (t.level == 0) {
            return false;
        } else {
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if (t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    /**
     * 检查事务是否已提交。
     * <p>
     * 此函数用于判断一个事务对于某个操作是否已经提交。它主要根据事务的开始标识（xmin）和结束标识（xmax）来判断。
     * 如果事务还未结束（xmax为0），且当前事务标识（xid）与操作的开始事务标识（xmin）相同，则认为该事务已提交。
     * 如果事务已结束（xmax不为0），但结束事务标识（xmax）对应的事务未提交，也认为当前事务已提交。
     * </p>
     *
     * @param tm 事务管理器，用于查询事务的提交状态。
     * @param t 当前事务对象，包含事务的标识（xid）。
     * @param e 操作对应的日志条目，包含操作开始的事务标识（xmin）和结束的事务标识（xmax）。
     * @return 如果当前事务已提交，则返回true；否则返回false。
     */
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid; // 当前事务的标识
        long xmin = e.getXmin(); // 操作开始的事务标识
        long xmax = e.getXmax(); // 操作结束的事务标识

        // 如果操作开始和结束的事务标识都指向当前事务，并且操作尚未被删除过，则认为事务已提交
        if (xmin == xid && xmax == 0) return true;

        // 如果操作开始的事务已提交
        if (tm.isCommitted(xmin)) {
            // 如果记录未被删除
            if (xmax == 0) return true;
            // 如果操作结束的事务不是当前事务，并且结束的事务未提交，则认为当前事务已提交
            if (xmax != xid) {

                // 保证读到的记录是最新的
                if (!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 根据事务的读一致性需求判断是否可以重复读取某个条目。
     *
     * @param tm 事务管理器，用于查询事务状态和提交信息。
     * @param t 当前事务对象。
     * @param e 要读取的条目对象。
     * @return 如果满足重复读的条件，返回true；否则返回false。
     */
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid; // 当前事务的事务标识符
        long xmin = e.getXmin(); // 条目最早被修改的事务标识符
        long xmax = e.getXmax(); // 条目最晚被修改的事务标识符
        // 如果条目仅被当前事务修改过且未被提交，允许重复读取
        if(xmin == xid && xmax == 0) return true;

        // 检查xmin代表的事务是否已提交，且在当前事务的快照之外
        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            // 如果条目未被再次修改（xmax为0），或已被其他事务修改（xmax不为xid）
            // 但新修改的事务要么未提交，要么在当前事务的快照内，允许重复读取
            if(xmax == 0) return true;  // 确保记录没有被删除
            if(xmax != xid) {
                // 判断新修改的事务是否满足特定条件
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    // 当 xmin < xid < xmax 且 xid不是快照里的活跃事务
                    return true;
                }
            }
        }
        // xid >= xmax
        return false;
    }


}
