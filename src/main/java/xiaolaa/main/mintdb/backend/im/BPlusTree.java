package xiaolaa.main.mintdb.backend.im;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import xiaolaa.main.mintdb.backend.common.SubArray;
import xiaolaa.main.mintdb.backend.dm.DataManager;
import xiaolaa.main.mintdb.backend.dm.dataItem.DataItem;
import xiaolaa.main.mintdb.backend.im.Node.InsertAndSplitRes;
import xiaolaa.main.mintdb.backend.im.Node.LeafSearchRangeRes;
import xiaolaa.main.mintdb.backend.im.Node.SearchNextRes;
import xiaolaa.main.mintdb.backend.tm.TransactionManagerImpl;
import xiaolaa.main.mintdb.backend.utils.Parser;

/**
 * B+树
 */
public class BPlusTree {
    DataManager dm;
    long bootUid; // 根节点的uid
    DataItem bootDataItem;
    Lock bootLock;

    public static long create(DataManager dm) throws Exception {
        // 创建一个空节点
        byte[] rawRoot = Node.newNilRootRaw();
        // 节点持久化到磁盘里
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
    }

    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }

    private long rootUid() {
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start+8));
        } finally {
            bootLock.unlock();
        }
    }

    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }

    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if(isLeaf) {
            return nodeUid;
        } else {
            // 找到下一个我应该去哪一个节点内找这个key
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }

    /**
     * 寻找给定节点UID之后的下一个节点UID。
     * 该方法通过循环查找给定节点的下一个兄弟节点或其子节点，直到找到一个具有非0 UID的节点为止。
     *
     * @param nodeUid 当前节点的UID（资源标识符）。
     * @param key 用于搜索下一个节点的关键字。
     * @return 找到的下一个节点的UID。如果没有找到下一个节点，则返回0。
     * @throws Exception 如果加载节点或执行搜索操作时出现错误，则抛出异常。
     */
    private long searchNext(long nodeUid, long key) throws Exception {
        while(true) {
            // 加载指定UID的节点
            Node node = Node.loadNode(this, nodeUid);
            // 在当前节点中搜索下一个匹配key的节点，并获取搜索结果
            SearchNextRes res = node.searchNext(key);
            // 释放当前节点的资源
            node.release();
            // 如果搜索结果中的uid不为0，则返回这个uid，表示找到了下一个节点
            if(res.uid != 0) return res.uid;
            // 如果搜索结果中的uid为0，表示未找到下一个节点，于是尝试寻找当前节点的下一个兄弟节点
            nodeUid = res.siblingUid;
        }
    }


    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = rootUid();
        // 搜索到根节点
        long leafUid = searchLeaf(rootUid, leftKey);
        List<Long> uids = new ArrayList<>();
        while(true) {
            Node leaf = Node.loadNode(this, leafUid);
            LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            if(res.siblingUid == 0) {
                break;
            } else {
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }

    /**
     * 为一个数据以key值插入
     * @param key
     * @param uid
     * @throws Exception
     */
    public void insert(long key, long uid) throws Exception {
        // 获取根节点的uid
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;
        if(res.newNode != 0) {
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    class InsertRes {
        long newNode, newKey;
    }

    /**
     * 插入一个数据
     * @param nodeUid
     * @param uid 要插入的记录对应的uid，执行一行记录
     * @param key 对应字段的实际值
     * @return
     * @throws Exception
     */
    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        // 是否是叶子节点
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res = null;
        if(isLeaf) {
            // 在叶子节点才插入数据
            res = insertAndSplit(nodeUid, uid, key);
        } else {
            // 获取下一个节点的uid
            long next = searchNext(nodeUid, key);
            InsertRes ir = insert(next, uid, key);
            if(ir.newNode != 0) {
                // 分裂出的新节点的指针插入
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            } else {
                res = new InsertRes();
            }
        }
        return res;
    }

    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while(true) {
            // 如果没找到key对应的节点，则往兄弟节点继续找
            Node node = Node.loadNode(this, nodeUid);
            InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();
            if(iasr.siblingUid != 0) {
                nodeUid = iasr.siblingUid;
            } else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }

    public void close() {
        bootDataItem.release();
    }
}
