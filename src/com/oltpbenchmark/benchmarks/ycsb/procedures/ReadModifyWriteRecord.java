/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.ycsb.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.oltpbenchmark.api.HihConnection;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class ReadModifyWriteRecord extends Procedure {
    public final SQLStmt selectStmt = new SQLStmt(
        "Select * from USERTABLE where YCSB_KEY=? for update"
    );
    public final SQLStmt updateAllStmt = new SQLStmt(
        "UPDATE USERTABLE SET FIELD1=?,FIELD2=?,FIELD3=?,FIELD4=?,FIELD5=?," +
        "FIELD6=?,FIELD7=?,FIELD8=?,FIELD9=?,FIELD10=? WHERE YCSB_KEY=?"
    );
	//FIXME: The value in ysqb is a byteiterator
    public void run(Connection conn, int keyname, String fields[], Map<Integer,String> results) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, selectStmt);
        stmt.setInt(1, keyname);          
        ResultSet r = stmt.executeQuery();
        while (r.next()) {
        	for (int i = 1; i < 11; i++)
        	    results.put(i, r.getString(i));
        }
        r.close();
        stmt= this.getPreparedStatement(conn, updateAllStmt);
        stmt.setInt(11, keyname);
        
        for (int i = 0; i < fields.length; i++) {
        	stmt.setString(i+1, fields[i]);
        }
        stmt.executeUpdate();
    }

    public final String selectStmtQ = "Select * from USERTABLE where ( YCSB_KEY=%d ) "; //for update

    public final String updateAllStmtQ =
            "UPDATE USERTABLE SET FIELD1 = '%1$s'," +
                    "               FIELD2 = '%2$s'," +
                    "               FIELD3 = '%3$s'," +
                    "               FIELD4 = '%4$s'," +
                    "               FIELD5 = '%5$s'," +
                    "               FIELD6 = '%6$s'," +
                    "               FIELD7 = '%7$s'," +
                    "               FIELD8 = '%8$s'," +
                    "               FIELD9 = '%9$s'," +
                    "               FIELD10 = '%10$s' " +
                    "WHERE ( YCSB_KEY = %11$d )";


    public void run(HihConnection conn, int keyname, String fields[], Map<Integer,String> results) throws SQLException {

        conn.hih.EXEC_QUERY(String.format(selectStmtQ, keyname));
        conn.hih.getColumnMetadata();
        while (conn.hih.delivery()){
            for (int i = 1; i < 11; i++)
                results.put(i, conn.hih.getColumn(i));
        }
        Object[] params = new Object[11];
        System.arraycopy(fields, 0, params, 0, fields.length);
        params[10]=keyname;

        String updateQStr = String.format(updateAllStmtQ, params);
        //System.out.println(updateQStr);
        conn.DML(updateQStr);
    }

}
