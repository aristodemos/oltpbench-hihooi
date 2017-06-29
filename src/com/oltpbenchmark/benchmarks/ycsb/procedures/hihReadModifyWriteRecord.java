package com.oltpbenchmark.benchmarks.ycsb.procedures;

import com.oltpbenchmark.api.HihConnection;
import com.oltpbenchmark.api.Procedure;

import java.sql.SQLException;
import java.util.Map;

/**
 * Created by ilvoladore on 01/05/17.
 */
public class hihReadModifyWriteRecord extends Procedure {

    public final String selectStmt = "Select * from USERTABLE where ( YCSB_KEY=%d ) for update";

    public final String updateAllStmt =
            "UPDATE USERTABLE SET FIELD1=%1$s," +
                    "               FIELD2=%2$s," +
                    "               FIELD3=%3$s," +
                    "               FIELD4=%4$s," +
                    "               FIELD5=%5$s," +
                    "               FIELD6=%6$s," +
                    "               FIELD7=%7$s," +
                    "               FIELD8=%8$s," +
                    "               FIELD9=%9$s," +
                    "               FIELD10=%10$s " +
                    "WHERE ( YCSB_KEY=%11$d )";


    public void run(HihConnection conn, int keyname, String fields[], Map<Integer,String> results) throws SQLException {

        conn.hih.EXEC_QUERY(String.format(selectStmt, keyname));
        conn.hih.getColumnMetadata();
        while (conn.hih.delivery()){
            for (int i = 1; i < 11; i++)
                results.put(i, conn.hih.getColumn(i));
        }
        Object[] params = new Object[11];
        System.arraycopy(fields, 0, params, 0, fields.length);
        params[10]=keyname;

        String updateQStr = String.format(updateAllStmt, params);
        conn.DML(updateQStr);
    }

}
