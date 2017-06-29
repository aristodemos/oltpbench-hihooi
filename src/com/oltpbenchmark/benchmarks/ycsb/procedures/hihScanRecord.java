package com.oltpbenchmark.benchmarks.ycsb.procedures;

import com.oltpbenchmark.api.HihConnection;
import com.oltpbenchmark.api.Procedure;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ilvoladore on 03/05/17.
 */
public class hihScanRecord extends Procedure {

    public final String scanStmt = "SELECT * FROM USERTABLE WHERE YCSB_KEY>%1$d AND YCSB_KEY<%2$d";

    public void run(HihConnection conn, int start, int count, List<Map<Integer,String>> results) throws SQLException {
        conn.hih.EXEC_QUERY(String.format(scanStmt, start, count));
        while(conn.hih.delivery())
        {
            HashMap<Integer,String> m=new HashMap<Integer,String>();
            for(int i=1;i<11;i++)
                m.put(i, conn.hih.getColumn(i));
            results.add(m);
        }
    }

}
