package com.oltpbenchmark.benchmarks.ycsb.procedures;

import com.oltpbenchmark.api.HihConnection;
import com.oltpbenchmark.api.Procedure;

import java.sql.SQLException;
import java.util.Map;

/**
 * Created by ilvoladore on 28/04/17.
 */
public class hihInsertRecord extends Procedure {
    //public final String insertStmt = "INSERT INTO \"USERTABLE\" VALUES (%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)";
    //1: Key
    //+10 values
    public final String insertStmt = "INSERT INTO USERTABLE VALUES (%d";
    //rest: %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    private String retQuery;

    public void run(HihConnection conn, int keyname, Map<Integer,String> vals) throws SQLException {

        retQuery = String.format(this.insertStmt, keyname);
        for(Map.Entry<Integer,String> s:vals.entrySet()) {
            //stmt.setString(s.getKey()+1, s.getValue());
            retQuery += ", '"+ s.getValue() + "'";
        }
        retQuery += ")";
        conn.DML(String.format(retQuery, keyname));
    }

}
