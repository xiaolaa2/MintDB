package xiaolaa.main.mintdb.backend.tbm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.primitives.Bytes;

import xiaolaa.main.mintdb.backend.parser.statement.Create;
import xiaolaa.main.mintdb.backend.parser.statement.Delete;
import xiaolaa.main.mintdb.backend.parser.statement.Insert;
import xiaolaa.main.mintdb.backend.parser.statement.Select;
import xiaolaa.main.mintdb.backend.parser.statement.Update;
import xiaolaa.main.mintdb.backend.parser.statement.Where;
import xiaolaa.main.mintdb.backend.tbm.Field.ParseValueRes;
import xiaolaa.main.mintdb.backend.tm.TransactionManagerImpl;
import xiaolaa.main.mintdb.backend.utils.Panic;
import xiaolaa.main.mintdb.backend.utils.ParseStringRes;
import xiaolaa.main.mintdb.backend.utils.Parser;
import xiaolaa.main.mintdb.common.Error;

/**
 * Table 维护了表结构
 * 二进制结构如下：
 * [TableName][NextTable]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 */
public class Table {
    TableManager tbm;
    long uid;
    String name;
    byte status;
    long nextUid;
    List<Field> fields = new ArrayList<>();

    public static Table loadTable(TableManager tbm, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl)tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        Table tb = new Table(tbm, uid);
        return tb.parseSelf(raw);
    }

    public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception {
        Table tb = new Table(tbm, create.tableName, nextUid);
        // 依次创建每一个field
        for(int i = 0; i < create.fieldName.length; i ++) {
            String fieldName = create.fieldName[i];
            String fieldType = create.fieldType[i];
            boolean indexed = false;
            // 给需要创建索引的字段创建索引
            for(int j = 0; j < create.index.length; j ++) {
                if(fieldName.equals(create.index[j])) {
                    indexed = true;
                    break;
                }
            }
            tb.fields.add(Field.createField(tb, xid, fieldName, fieldType, indexed));
        }

        return tb.persistSelf(xid);
    }

    public Table(TableManager tbm, long uid) {
        this.tbm = tbm;
        this.uid = uid;
    }

    public Table(TableManager tbm, String tableName, long nextUid) {
        this.tbm = tbm;
        this.name = tableName;
        this.nextUid = nextUid;
    }

    private Table parseSelf(byte[] raw) {
        int position = 0;
        // 获取表名
        ParseStringRes res = Parser.parseString(raw);
        name = res.str;
        position += res.next;
        // 获取下一张表的uid
        nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
        position += 8;

        while(position < raw.length) {
            long uid = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
            position += 8;
            // 顺序加载每个field，字段
            fields.add(Field.loadField(this, uid));
        }
        return this;
    }

    private Table persistSelf(long xid) throws Exception {
        // 表名
        byte[] nameRaw = Parser.string2Byte(name);
        // 下一张表
        byte[] nextRaw = Parser.long2Byte(nextUid);
        byte[] fieldRaw = new byte[0];
        for(Field field : fields) {
            fieldRaw = Bytes.concat(fieldRaw, Parser.long2Byte(field.uid));
        }
        // 持久化表table
        uid = ((TableManagerImpl)tbm).vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
        return this;
    }

    /**
     * MintDB中并不会直接在硬盘中删除对应的记录，而是设置记录的xmax字段为当前xid，配合MVCC使其不可见
     * @param xid
     * @param delete
     * @return
     * @throws Exception
     */
    public int delete(long xid, Delete delete) throws Exception {
        List<Long> uids = parseWhere(delete.where);
        int count = 0;
        for (Long uid : uids) {
            if(((TableManagerImpl)tbm).vm.delete(xid, uid)) {
                count ++;
            }
        }
        return count;
    }

    /**
     * 更新满足特定条件的记录的指定字段值，并返回更新的记录数量。
     *
     * @param xid 事务ID，用于标识当前操作的事务。
     * @param update 包含更新操作的详细信息，如要更新的字段名、新值以及筛选条件。
     * @return 返回更新的记录数量。
     * @throws Exception 如果操作过程中出现错误，则抛出异常。
     */
    public int update(long xid, Update update) throws Exception {
        // 解析更新条件中的筛选标识符
        List<Long> uids = parseWhere(update.where);
        Field fd = null;
        // 查找要更新的字段
        for (Field f : fields) {
            if(f.fieldName.equals(update.fieldName)) {
                fd = f;
                break;
            }
        }
        // 如果未找到对应的字段，则抛出字段未找到异常
        if(fd == null) {
            throw Error.FieldNotFoundException;
        }
        // 将字符串形式的值转换为字段期待的数据类型
        Object value = fd.string2Value(update.value);
        int count = 0; // 用于记录更新的记录数量
        // 遍历所有满足更新条件的记录，包括记录历史数据
        for (Long uid : uids) {
            // 读取记录的原始数据，假如mvcc不可见，继续寻找下一条历史记录
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if(raw == null) continue; // 如果记录不存在，则跳过当前循环

            // 删除原始记录
            ((TableManagerImpl)tbm).vm.delete(xid, uid);

            // 解析原始数据，以获取当前记录的所有字段值
            Map<String, Object> entry = parseEntry(raw);
            // 更新指定字段的值
            entry.put(fd.fieldName, value);
            // 重新构建更新后的记录数据
            raw = entry2Raw(entry);
            // 在数据库中插入更新后的记录，并获取新记录的UUID
            long uuid = ((TableManagerImpl)tbm).vm.insert(xid, raw);

            count ++; // 更新计数器

            // 对于所有需要建立索引的字段，插入新的索引项
            for (Field field : fields) {
                if(field.isIndexed()) {
                    field.insert(entry.get(field.fieldName), uuid);
                }
            }
        }
        return count; // 返回更新的记录数量
    }


    /**
     * select
     * TODO: 1.建立索引后怎么去查找？2.没建立索引又怎么找
     * @param xid
     * @param read
     * @return
     * @throws Exception
     */
    public String read(long xid, Select read) throws Exception {
        // 解析where
        List<Long> uids = parseWhere(read.where);
        StringBuilder sb = new StringBuilder();
        // uids为行记录集合
        // 所有版本的记录都会扫描出来
        for (Long uid : uids) {
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if(raw == null) continue;  // 说明当前事务无法访问该版本的记录 MVCC
            // 解析出行记录
            Map<String, Object> entry = parseEntry(raw);
            sb.append(printEntry(entry)).append("\n");
        }
        return sb.toString();
    }

    public void insert(long xid, Insert insert) throws Exception {
        Map<String, Object> entry = string2Entry(insert.values);
        // 表一行所有字段连接后的记录二进制
        byte[] raw = entry2Raw(entry);
        long uid = ((TableManagerImpl)tbm).vm.insert(xid, raw);
        for (Field field : fields) {
            // 如果字段是索引字段，则插入索引
            if(field.isIndexed()) {
                field.insert(entry.get(field.fieldName), uid);
            }
        }
    }

    private Map<String, Object> string2Entry(String[] values) throws Exception {
        // insert语句传入的值与字段个数不匹配
        if(values.length != fields.size()) {
            throw Error.InvalidValuesException;
        }
        Map<String, Object> entry = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            // 根据field对对应的值进行类型转换
            Object v = f.string2Value(values[i]);
            entry.put(f.fieldName, v);
        }
        return entry;
    }

    /**
     * 解析查询条件，并根据条件返回匹配的UID列表。
     *
     * @param where 包含单个或多个查询表达式的对象。如果为null，则自动搜索第一个索引字段。
     * @return 返回匹配条件的UID列表。
     * @throws Exception 如果字段未被索引或查询中指定的字段不存在，则抛出异常。
     */
    private List<Long> parseWhere(Where where) throws Exception {
        long l0=0, r0=0, l1=0, r1=0;
        boolean single = false;
        Field fd = null;
        // 当where参数为null时，自动选择第一个索引字段进行全范围查询
        if(where == null) {
            for (Field field : fields) {
                if(field.isIndexed()) {
                    fd = field;
                    break;
                }
            }
            l0 = 0;
            r0 = Long.MAX_VALUE;
            single = true;
        } else {
            // 根据where参数中的字段名查找对应的字段
            for (Field field : fields) {
                if(field.fieldName.equals(where.singleExp1.field)) {
                    if(!field.isIndexed()) {
                        throw Error.FieldNotIndexedException;
                    }
                    fd = field;
                    break;
                }
            }
            // 如果where参数中的字段不存在，则抛出异常
            if(fd == null) {
                throw Error.FieldNotFoundException;
            }
            // 计算查询条件的边界值
            CalWhereRes res = calWhere(fd, where);
            // 第一个查询条件
            l0 = res.l0; r0 = res.r0;
            // 第二个查询条件
            l1 = res.l1; r1 = res.r1;
            single = res.single;
        }
        // 根据计算得到的边界值查询匹配的UID列表
        List<Long> uids = fd.search(l0, r0);
        // 如果不是单个查询条件，则合并第二个查询范围的结果到UID列表中
        if(!single) {
            List<Long> tmp = fd.search(l1, r1);
            uids.addAll(tmp); // 合并查询结果
        }
        return uids;
    }


    class CalWhereRes {
        long l0, r0, l1, r1;
        boolean single;
    }

    /**
     * 根据给定的字段和条件计算筛选结果。
     *
     * @param fd 字段对象，用于执行计算。
     * @param where 包含筛选条件的对象，可以是单个条件或逻辑组合。
     * @return CalWhereRes 对象，包含计算后的结果。
     * @throws Exception 如果逻辑操作符不合法，则抛出异常。
     */
    private CalWhereRes calWhere(Field fd, Where where) throws Exception {
        CalWhereRes res = new CalWhereRes();
        switch(where.logicOp) {
            case "":
                // 当逻辑操作为空时，只处理单个条件
                res.single = true;
                FieldCalRes r = fd.calExp(where.singleExp1);
                // 计算左右边界值
                res.l0 = r.left; res.r0 = r.right;
                break;
            case "or":
                // 处理“或”逻辑操作
                res.single = false; // 将两个条件分别视作一个条件来处理
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left; res.r1 = r.right;
                break;
            case "and":
                // 处理“与”逻辑操作
                res.single = true;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left; res.r1 = r.right;
                // 逻辑“与”操作，合并计算结果
                if(res.l1 > res.l0) res.l0 = res.l1;
                if(res.r1 < res.r0) res.r0 = res.r1;
                break;
            default:
                // 如果逻辑操作符不合法，则抛出异常
                throw Error.InvalidLogOpException;
        }
        return res;
    }


    private String printEntry(Map<String, Object> entry) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            sb.append(field.printValue(entry.get(field.fieldName)));
            if(i == fields.size()-1) {
                sb.append("]");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    private Map<String, Object> parseEntry(byte[] raw) {
        int pos = 0;
        Map<String, Object> entry = new HashMap<>();
        // 根据表定义的Field顺序解析出每个字段的值
        for (Field field : fields) {
            ParseValueRes r = field.parserValue(Arrays.copyOfRange(raw, pos, raw.length));
            entry.put(field.fieldName, r.v);
            // 根据字段解析的类型，计算出需要跳过的字节数
            pos += r.shift;
        }
        return entry;
    }

    /**
     * 将一个表中的一行记录转换为Field的二进制数组按表定义的field字段顺序连接，[字段一记录][字段二记录]..
     * @param entry
     * @return
     */
    private byte[] entry2Raw(Map<String, Object> entry) {
        byte[] raw = new byte[0];
        for (Field field : fields) {
            raw = Bytes.concat(raw, field.value2Raw(entry.get(field.fieldName)));
        }
        return raw;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append(name).append(": ");
        for(Field field : fields) {
            sb.append(field.toString());
            if(field == fields.get(fields.size()-1)) {
                sb.append("}");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
