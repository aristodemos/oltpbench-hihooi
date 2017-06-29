package com.oltpbenchmark.benchmarks.ycsb.procedures;

import com.oltpbenchmark.api.HihConnection;
import com.oltpbenchmark.api.Procedure;

import java.sql.SQLException;
import java.util.Map;

/**
 * Created by ilvoladore on 03/05/17.
 */
public class hihReadRecord extends Procedure {

    public final String readStmt = "SELECT * FROM USERTABLE WHERE ( YCSB_KEY=%d )";

    //FIXME: The value in ysqb is a byteiterator
    public void run(HihConnection conn, int keyname, Map<Integer,String> results) throws SQLException {

        conn.hih.EXEC_QUERY(String.format(readStmt, keyname));
        while (conn.hih.delivery()){
            for (int i = 1; i < 11; i++)
                results.put(i, conn.hih.getColumn(i));
        }
    }



}
