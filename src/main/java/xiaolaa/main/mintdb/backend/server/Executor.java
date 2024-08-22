package xiaolaa.main.mintdb.backend.server;

import xiaolaa.main.mintdb.backend.parser.Parser;
import xiaolaa.main.mintdb.backend.parser.statement.Abort;
import xiaolaa.main.mintdb.backend.parser.statement.Begin;
import xiaolaa.main.mintdb.backend.parser.statement.Commit;
import xiaolaa.main.mintdb.backend.parser.statement.Create;
import xiaolaa.main.mintdb.backend.parser.statement.Delete;
import xiaolaa.main.mintdb.backend.parser.statement.Insert;
import xiaolaa.main.mintdb.backend.parser.statement.Select;
import xiaolaa.main.mintdb.backend.parser.statement.Show;
import xiaolaa.main.mintdb.backend.parser.statement.Update;
import xiaolaa.main.mintdb.backend.tbm.BeginRes;
import xiaolaa.main.mintdb.backend.tbm.TableManager;
import xiaolaa.main.mintdb.common.Error;

public class Executor {
    // 当前事务
    private long xid;
    public TableManager tbm;

    public Executor(TableManager tbm) {
        this.tbm = tbm;
        this.xid = 0;
    }

    public void close() {
        if(xid != 0) {
            System.out.println("Abnormal Abort: " + xid);
            tbm.abort(xid);
        }
    }

    /**
     * 执行事务
     * 执行给定的SQL命令。
     * @param sql 字节型数组，代表待执行的SQL命令。
     * @return 执行结果，作为字节型数组返回。
     * @throws Exception 如果执行过程中出现错误，则抛出异常。
     */
    public byte[] execute(byte[] sql) throws Exception {
        // 打印执行的SQL命令
        System.out.println("Execute: " + new String(sql));
        Object stat = Parser.Parse(sql); // 解析SQL命令

        // 根据解析后的命令类型执行相应的操作
        if(Begin.class.isInstance(stat)) {
            // 如果是开始事务命令
            if(xid != 0) {
                // 如果已经在一个事务中，则抛出嵌套事务异常
                throw Error.NestedTransactionException;
            }
            BeginRes r = tbm.begin((Begin)stat); // 开始事务
            xid = r.xid; // 保存事务ID
            return r.result; // 返回事务开始结果
        } else if(Commit.class.isInstance(stat)) {
            // 如果是提交事务命令
            if(xid == 0) {
                // 如果不在事务中，则抛出无事务异常
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.commit(xid); // 提交事务
            xid = 0; // 重置事务ID
            return res; // 返回事务提交结果
        } else if(Abort.class.isInstance(stat)) {
            // 如果是回滚事务命令
            if(xid == 0) {
                // 如果不在事务中，则抛出无事务异常
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.abort(xid); // 回滚事务
            xid = 0; // 重置事务ID
            return res; // 返回事务回滚结果
        } else {
            // 如果是其他类型的命令，则调用execute2方法执行
            return execute2(stat);
        }
    }


    /**
     * 普通sql执行
     * @param stat
     * @return
     * @throws Exception
     */
    private byte[] execute2(Object stat) throws Exception {
        boolean tmpTransaction = false;
        Exception e = null;
        if(xid == 0) {
            tmpTransaction = true;
            BeginRes r = tbm.begin(new Begin());
            xid = r.xid;
        }
        try {
            byte[] res = null;
            if(Show.class.isInstance(stat)) {
                res = tbm.show(xid);
            } else if(Create.class.isInstance(stat)) {
                res = tbm.create(xid, (Create)stat);
            } else if(Select.class.isInstance(stat)) {
                res = tbm.read(xid, (Select)stat);
            } else if(Insert.class.isInstance(stat)) {
                res = tbm.insert(xid, (Insert)stat);
            } else if(Delete.class.isInstance(stat)) {
                res = tbm.delete(xid, (Delete)stat);
            } else if(Update.class.isInstance(stat)) {
                res = tbm.update(xid, (Update)stat);
            }
            return res;
        } catch(Exception e1) {
            e = e1;
            throw e;
        } finally {
            if(tmpTransaction) {
                if(e != null) {
                    // 如果发生异常，标记事务需要回滚
                    tbm.abort(xid);
                } else {
                    tbm.commit(xid);
                }
                xid = 0;
            }
        }
    }
}
