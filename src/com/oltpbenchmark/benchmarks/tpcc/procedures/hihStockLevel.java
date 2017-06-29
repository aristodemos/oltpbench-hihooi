package com.oltpbenchmark.benchmarks.tpcc.procedures;

/**
 * Created by ilvoladore on 14/06/2016.
 */
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

import com.oltpbenchmark.api.*;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

import com.oltpbenchmark.api.HihConnection;
import org.apache.log4j.Logger;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import org.apache.log4j.varia.StringMatchFilter;

import java.util.Map;

public class hihStockLevel extends TPCCProcedure {

    private static final Logger LOG = Logger.getLogger(StockLevel.class);
    private HihConnection conn = null;

    public String stockGetDistOrderIdSQL = "SELECT D_NEXT_O_ID FROM DISTRICT WHERE ( D_W_ID = %d AND D_ID = %d )";

    public String stockGetCountStockSQL = "SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT"
            + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + ", " + TPCCConstants.TABLENAME_STOCK
            + " WHERE OL_W_ID = %d"
            + " AND OL_D_ID = %d"
            + " AND OL_O_ID < %d"
            + " AND OL_O_ID >= %d - 20"
            + " AND S_W_ID = %d"
            + " AND S_I_ID = OL_I_ID" + " AND S_QUANTITY < %d";

    // Stock Level Txn
    private PreparedStatement stockGetDistOrderId = null;
    private PreparedStatement stockGetCountStock = null;

    public ResultSet run(HihConnection util, Random gen,
                         int terminalWarehouseID, int numWarehouses,
                         int terminalDistrictLowerID, int terminalDistrictUpperID,
                         TPCCWorker w) {

        this.conn = util;

        int threshold = TPCCUtil.randomNumber(10, 20, gen);

        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);

        stockLevelTransaction(terminalWarehouseID, districtID, threshold,conn,w);

        return null;
    }

    public ResultSet run(Connection conn, Random gen,
                         int terminalWarehouseID, int numWarehouses,
                         int terminalDistrictLowerID, int terminalDistrictUpperID,
                         TPCCWorker w) throws SQLException{
        return null;
    }

    public ResultSet run(HihActorConnection conn, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w)  {
        return null;
    }



    private void stockLevelTransaction(int w_id, int d_id, int threshold, HihConnection conn,TPCCWorker w){
        int o_id = 0;
        // XXX int i_id = 0;
        int stock_count = 0;

        /*Map<String, String> rs = conn.QUERY2MAP(String.format(stockGetDistOrderIdSQL, w_id, d_id));
        if (rs.isEmpty())
            throw new RuntimeException("D_W_ID="+ w_id +" D_ID="+ d_id+" not found!");

        o_id = Integer.parseInt(rs.get("D_NEXT_O_ID"));
        rs = null;*/

        //NEW HIHOOI
        conn.hih.EXEC_QUERY(String.format(stockGetDistOrderIdSQL, w_id, d_id));
        conn.hih.getColumnMetadata();
        if(conn.hih.delivery()){
            o_id = Integer.parseInt(conn.hih.getColumn(1));
        }
        else{
            throw new RuntimeException("D_W_ID="+ w_id +" D_ID="+ d_id+" not found!");
        }



        /*rs = conn.QUERY2MAP(String.format(stockGetCountStockSQL, w_id, d_id, o_id, o_id, w_id, threshold));
        if (rs.isEmpty())
            throw new RuntimeException("OL_W_ID="+w_id +" OL_D_ID="+d_id+" OL_O_ID="+o_id+" not found!");

        stock_count = Integer.parseInt(rs.get("STOCK_COUNT"));
        //conn.commit();
        rs = null;*/

        conn.hih.EXEC_QUERY(String.format(stockGetCountStockSQL, w_id, d_id, o_id, o_id, w_id, threshold));
        conn.hih.getColumnMetadata();
        if (conn.hih.delivery()) {
            stock_count = Integer.parseInt(conn.hih.getColumn(1));
        }
        else{
            throw new RuntimeException("OL_W_ID="+w_id +" OL_D_ID="+d_id+" OL_O_ID="+o_id+" not found!");
        }

        StringBuilder terminalMessage = new StringBuilder();
        terminalMessage
                .append("\n+-------------------------- STOCK-LEVEL --------------------------+");
        terminalMessage.append("\n Warehouse: ");
        terminalMessage.append(w_id);
        terminalMessage.append("\n District:  ");
        terminalMessage.append(d_id);
        terminalMessage.append("\n\n Stock Level Threshold: ");
        terminalMessage.append(threshold);
        terminalMessage.append("\n Low Stock Count:       ");
        terminalMessage.append(stock_count);
        terminalMessage
                .append("\n+-----------------------------------------------------------------+\n\n");
        if(LOG.isTraceEnabled())LOG.trace(terminalMessage.toString());
    }

}
