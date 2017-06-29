package com.oltpbenchmark.benchmarks.ycsb.procedures;

/**
 * Created by ilvoladore on 20/07/2016.
 */

import com.oltpbenchmark.api.HihConnection;
import com.oltpbenchmark.api.Procedure;

public class hihDeleteRecord extends Procedure {

    public final String deleteStmt = "DELETE FROM USERTABLE where ( YCSB_KEY=%d )";

    public void run(HihConnection conn, int keyname) {

        conn.DML(String.format(this.deleteStmt, keyname));

    }
}