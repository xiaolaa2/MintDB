package xiaolaa.main.mintdb.backend.im;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import xiaolaa.main.mintdb.backend.common.SubArray;
import xiaolaa.main.mintdb.backend.dm.dataItem.DataItem;
import xiaolaa.main.mintdb.backend.tm.TransactionManagerImpl;
import xiaolaa.main.mintdb.backend.utils.Parser;

/**
 * Node结构如下：
 * [LeafFlag][KeyNumber][SiblingUid]
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 */
public class Node {
    static final int IS_LEAF_OFFSET = 0;
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET+1;
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET+2;
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET+8;

    static final int BALANCE_NUMBER = 32;
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2*8)*(BALANCE_NUMBER*2+2);

    BPlusTree tree;
    DataItem dataItem;
    SubArray raw;
    long uid;

    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if(isLeaf) {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)1;
        } else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)0;
        }
    }

    static boolean getRawIfLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte)1;
    }

    static void setRawNoKeys(SubArray raw, int noKeys) {
        // 设置两个字节长度的键数量
        System.arraycopy(Parser.short2Byte((short)noKeys), 0, raw.raw, raw.start+NO_KEYS_OFFSET, 2);
    }

    static int getRawNoKeys(SubArray raw) {
        return (int)Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start+NO_KEYS_OFFSET, raw.start+NO_KEYS_OFFSET+2));
    }

    static void setRawSibling(SubArray raw, long sibling) {
        // 设置兄弟节点的uid
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start+SIBLING_OFFSET, 8);
    }

    static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start+SIBLING_OFFSET, raw.start+SIBLING_OFFSET+8));
    }

    static void setRawKthSon(SubArray raw, long uid, int kth) {
        // 跳过开头的leafFlag、keynumber、siblingUID
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }

    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }

    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(from.raw, offset, to.raw, to.start+NODE_HEADER_SIZE, from.end-offset);
    }

    /**
     * 该方法用于将给定SubArray中的第k个元素之后的所有元素向后移动一个位置。
     * 重要的是，此方法会修改原始数组的内容。
     *
     * @param raw 表示要操作的原始子数组。它包含实际要操作的字节数据。
     * @param kth 表示要从哪个位置开始移动元素。kth元素本身不被移动，而是从kth之后的元素开始移动。
     */
    static void shiftRawKth(SubArray raw, int kth) {
        // 计算需要移动数据的起始位置和结束位置
        int begin = raw.start+NODE_HEADER_SIZE+(kth+1)*(8*2);
        int end = raw.start+NODE_SIZE-1;
        // 从结束位置向前遍历，将每个位置的元素向后移动一个位置
        for(int i = end; i >= begin; i --) {
            raw.raw[i] = raw.raw[i-(8*2)];
        }
    }

    static byte[] newRootRaw(long left, long right, long key)  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, false);
        setRawNoKeys(raw, 2);
        setRawSibling(raw, 0);
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, key, 0);
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, Long.MAX_VALUE, 1);

        return raw.raw;
    }

    static byte[] newNilRootRaw()  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, true);
        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);

        return raw.raw;
    }

    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    public void release() {
        dataItem.release();
    }

    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIfLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }

    class SearchNextRes {
        long uid;
        long siblingUid;
    }

    /**
     * 搜索给定键值之后的下一个键值对应的资源。
     *
     * @param key 搜索的起始键值。
     * @return SearchNextRes 返回搜索结果，包含找到的键的uid和其后续兄弟节点的uid。
     */
    public SearchNextRes searchNext(long key) {
        dataItem.rLock(); // 获取读锁以确保线程安全
        try {
            SearchNextRes res = new SearchNextRes();
            int noKeys = getRawNoKeys(raw); // 获取当前raw中键的数量
            // 遍历所有键来查找第一个大于给定key的键
            for(int i = 0; i < noKeys; i ++) {
                long ik = getRawKthKey(raw, i); // 获取第i个键的值
                if(key < ik) {
                    // 如果找到第一个大于key的键，则返回其后续兄弟节点的uid和当前节点的uid
                    res.uid = getRawKthSon(raw, i);
                    res.siblingUid = 0; // 默认后续兄弟节点uid为0
                    return res;
                }
            }
            // 如果所有键都比key小，则返回后续兄弟节点的uid，当前节点没有后续兄弟节点
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;

        } finally {
            dataItem.rUnLock(); // 释放读锁
        }
    }


    class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }

    /**
     * 在叶子节点中范围查找
     * @param leftKey
     * @param rightKey
     * @return
     */
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();
        try {
            int noKeys = getRawNoKeys(raw);
            int kth = 0;
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik >= leftKey) {
                    break;
                }
                kth ++;
            }
            // 找到左侧起点
            List<Long> uids = new ArrayList<>();
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik <= rightKey) {
                    uids.add(getRawKthSon(raw, kth));
                    kth ++;
                } else {
                    break;
                }
            }
            long siblingUid = 0;
            if(kth == noKeys) {
                siblingUid = getRawSibling(raw);
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    class InsertAndSplitRes {
        long siblingUid, newSon, newKey;
    }

    /**
     * 插入数据，如果插入成功则返回true，否则返回false。
     * @param uid
     * @param key
     * @return
     * @throws Exception
     */
    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();

        dataItem.before();
        try {
            success = insert(uid, key);
            if(!success) {
                // 插入失败，去邻居节点继续找
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            // 插入之后判断是否需要拆分，kth大于BALANCE_NUMBER*2，则需要拆分
            if(needSplit()) {
                // node的key数量太多，页分裂
                try {
                    SplitRes r = split();
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch(Exception e) {
                    err = e;
                    throw e;
                }
            } else {
                return res;
            }
        } finally {
            if(err == null && success) {
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            } else {
                dataItem.unBefore();
            }
        }
    }

    private boolean insert(long uid, long key) {
        int noKeys = getRawNoKeys(raw);
        int kth = 0;
        while(kth < noKeys) {
            long ik = getRawKthKey(raw, kth);
            if(ik < key) {
                kth ++;
            } else {
                break;
            }
        }
        // 如果插入的键值大于所有键值，且当前节点有后续兄弟节点，则返回，在兄弟节点中继续插入
        if(kth == noKeys && getRawSibling(raw) != 0) return false;

        if(getRawIfLeaf(raw)) {
            // 如果为叶子节点
            // 将原来在这个位置的元素往后挪一个位置
            shiftRawKth(raw, kth);
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            setRawNoKeys(raw, noKeys+1);
        } else {
            long kk = getRawKthKey(raw, kth);
            setRawKthKey(raw, key, kth);
            shiftRawKth(raw, kth+1);
            setRawKthKey(raw, kk, kth+1);
            setRawKthSon(raw, uid, kth+1);
            setRawNoKeys(raw, noKeys+1);
        }
        return true;
    }

    private boolean needSplit() {
        return BALANCE_NUMBER*2 == getRawNoKeys(raw);
    }

    class SplitRes {
        long newSon, newKey;
    }

    /**
     * 执行节点分裂操作。
     * 该操作用于处理当一个节点的键数量超过预设的最大值时的情况。它会将当前节点分裂成两个节点，
     * 并将其中一个作为新节点插入到树中。
     *
     * @return SplitRes 包含新儿子节点的ID和新插入的键。
     * @throws Exception 如果操作失败，抛出异常。
     */
    private SplitRes split() throws Exception {
        // 创建一个新的子数组，用于存放即将分裂出的节点数据
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        // 设置新节点的是否为叶子节点属性，以及初始键数量和兄弟节点ID
        setRawIsLeaf(nodeRaw, getRawIfLeaf(raw)); // 原来就是叶子节点，那么现在也就是叶子节点
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        setRawSibling(nodeRaw, getRawSibling(raw)); // 继承源节点的兄弟节点

        // 从原始节点复制数据到新节点，从第BALANCE_NUMBER个键开始复制
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);

        // 向数据树中插入新节点，并获取新节点的UID
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);

        // 更新原始节点的键数量和兄弟节点ID
        setRawNoKeys(raw, BALANCE_NUMBER);
        setRawSibling(raw, son);

        // 准备分裂结果
        SplitRes res = new SplitRes();
        res.newSon = son; // 新儿子节点的ID
        res.newKey = getRawKthKey(nodeRaw, 0); // 新插入的键

        return res;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIfLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for(int i = 0; i < KeyNumber; i ++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }

}
