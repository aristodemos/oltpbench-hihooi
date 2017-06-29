package com.oltpbenchmark.benchmarks.ycsb.procedures;

import com.oltpbenchmark.api.HihConnection;
import com.oltpbenchmark.api.Procedure;

import java.sql.SQLException;
import java.util.Map;

/**
 * Created by ilvoladore on 03/05/17.
 */
public class hihUpdateRecord extends Procedure {

    public final String updateAllStmt =
            "UPDATE USERTABLE SET FIELD1 =  '%1$s'," +
                    "               FIELD2 = '%2$s'," +
                    "               FIELD3 = '%3$s'," +
                    "               FIELD4 = '%4$s'," +
                    "               FIELD5 = '%5$s'," +
                    "               FIELD6 = '%6$s'," +
                    "               FIELD7 = '%7$s'," +
                    "               FIELD8 = '%8$s'," +
                    "               FIELD9 = '%9$s'," +
                    "               FIELD10 = '%10$s' " +
                    "WHERE ( YCSB_KEY=%11$d )";

    public void run(HihConnection conn, int keyname, Map<Integer,String> vals) throws SQLException {

        assert(vals.size()==10);
        Object[] params = new Object[11];
        //System.arraycopy(fields, 0, params, 0, fields.length);
        params[10]=keyname;
        for(Map.Entry<Integer, String> s:vals.entrySet())
        {
            params[s.getKey()]=s.getValue();
        }
        String updateQStr = String.format(updateAllStmt, params);
        conn.DML(updateQStr);
    }

}
