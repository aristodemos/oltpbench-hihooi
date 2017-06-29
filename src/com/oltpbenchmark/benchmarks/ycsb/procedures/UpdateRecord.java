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
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;

import com.oltpbenchmark.api.HihConnection;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class UpdateRecord extends Procedure {
    
    public final SQLStmt updateAllStmt = new SQLStmt(
        "UPDATE USERTABLE SET FIELD1=?,FIELD2=?,FIELD3=?,FIELD4=?,FIELD5=?," +
        "FIELD6=?,FIELD7=?,FIELD8=?,FIELD9=?,FIELD10=? WHERE YCSB_KEY=?"
    );
    
    public void run(Connection conn, int keyname, Map<Integer,String> vals) throws SQLException {
    	PreparedStatement stmt = this.getPreparedStatement(conn, updateAllStmt);
		assert(vals.size()==10);       
		stmt.setInt(11,keyname); 
        for(Entry<Integer, String> s:vals.entrySet())
        {
        	stmt.setString(s.getKey(), s.getValue());
        }
        stmt.executeUpdate();
    }

    public final String updateAllStmtQ =
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
            params[s.getKey()-1]=s.getValue();
        }
        String updateQStr = String.format(updateAllStmtQ, params);
        /*System.out.println("PARAMS: " + params);
        for (int i=0;i<params.length;i++){
            System.out.println(i +" -> " + params[i]);
        }
        System.out.println(updateQStr);*/
        conn.DML(updateQStr);
    }

}
