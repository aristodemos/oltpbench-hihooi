package com.oltpbenchmark.benchmarks.tpcc.procedures;

/**
 * Created by ilvoladore on 13/06/2016.
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

import java.sql.*;
import java.util.Random;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Random;

import com.oltpbenchmark.api.HihConnection;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;

import java.util.Map;
import java.util.List;

public class hihDelivery extends TPCCProcedure {

    /*
    String s = "hello %s!";
    s = String.format(s, "world" );
    assertEquals(s, "hello world!"); // true
     */

    public String delivGetOrderIdSQL = "SELECT NO_O_ID FROM " + TPCCConstants.TABLENAME_NEWORDER + " WHERE NO_D_ID = %d"
            + " AND NO_W_ID = %d ORDER BY NO_O_ID ASC LIMIT 1";
    public String delivDeleteNewOrderSQL = "DELETE FROM " + TPCCConstants.TABLENAME_NEWORDER + ""
            + " WHERE ( NO_O_ID = %d AND NO_D_ID = %d"
            + " AND NO_W_ID = %d )";
    public String delivGetCustIdSQL = "SELECT O_C_ID"
            + " FROM " + TPCCConstants.TABLENAME_OPENORDER + " WHERE ( O_ID = %d"
            + " AND O_D_ID = %d" + " AND O_W_ID = %d )";
    public String delivUpdateCarrierIdSQL = "UPDATE " + TPCCConstants.TABLENAME_OPENORDER + " SET O_CARRIER_ID = %d"
            + " WHERE ( O_ID = %d" + " AND O_D_ID = %d"
            + " AND O_W_ID = %d )";
    public String delivUpdateDeliveryDateSQL = "UPDATE " + TPCCConstants.TABLENAME_ORDERLINE + " SET OL_DELIVERY_D = '%s' "
            + " WHERE OL_O_ID = %d"
            + " AND OL_D_ID = %d"
            + " AND OL_W_ID = %d";
    public String delivSumOrderAmountSQL = "SELECT SUM(OL_AMOUNT) AS OL_TOTAL"
            + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + "" + " WHERE OL_O_ID = %d"
            + " AND OL_D_ID = %d" + " AND OL_W_ID = %d";
    public String delivUpdateCustBalDelivCntSQL = "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET C_BALANCE = C_BALANCE + %s "
            + ", C_DELIVERY_CNT = C_DELIVERY_CNT + 1"
            + " WHERE ( C_W_ID = %d"
            + " AND C_D_ID = %d"
            + " AND C_ID = %d )";



    public ResultSet run(HihConnection conn, Random gen,
                         int terminalWarehouseID, int numWarehouses,
                         int terminalDistrictLowerID, int terminalDistrictUpperID,
                         TPCCWorker w){
        int orderCarrierID = TPCCUtil.randomNumber(1, 10, gen);

        deliveryTransaction(terminalWarehouseID, orderCarrierID, conn, w);
        return null;
    }

    public ResultSet run(Connection conn, Random gen,
                         int terminalWarehouseID, int numWarehouses,
                         int terminalDistrictLowerID, int terminalDistrictUpperID,
                         TPCCWorker w) throws SQLException {
        return null;
    }

    public ResultSet run(HihActorConnection conn, Random gen,
                         int terminalWarehouseID, int numWarehouses,
                         int terminalDistrictLowerID, int terminalDistrictUpperID,
                         TPCCWorker w) {
        return null;
    }


    private int deliveryTransaction(int w_id, int o_carrier_id, HihConnection conn, TPCCWorker w) {

        int d_id, c_id;
        float ol_total;
        int[] orderIDs;
        orderIDs = new int[10];

        //START_TX
        conn.START_TX();

        for (d_id = 1; d_id <= 10; d_id++) {

            conn.hih.EXEC_QUERY(String.format(delivGetOrderIdSQL, d_id, w_id));
            conn.hih.getColumnMetadata();
            int no_o_id;
            if (conn.hih.delivery()){
                no_o_id = Integer.parseInt(conn.hih.getColumn(1));
            }
            else{
                // This district has no new orders; this can happen but should
                // be rare
                continue;
            }

            orderIDs[d_id - 1] = no_o_id;

            String result = conn.DML(String.format(delivDeleteNewOrderSQL, no_o_id, d_id, w_id));
            //System.out.println(Thread.currentThread().getName() + String.format(delivDeleteNewOrderSQL, no_o_id, d_id, w_id));
            if (Integer.parseInt(result.substring(14)) <= 0) {
                /*System.out.println(Thread.currentThread().getName() + "\t DELETE:");
                System.out.println(String.format(delivDeleteNewOrderSQL, no_o_id, d_id, w_id));
                System.out.println(result);
                System.out.println("-------");*/
                throw new UserAbortException(
                        "New order w_id="
                                + w_id
                                + " d_id="
                                + d_id
                                + " no_o_id="
                                + no_o_id
                                + " delete failed (not running with SERIALIZABLE isolation?)");
            }

            conn.hih.EXEC_QUERY(String.format(delivGetCustIdSQL, no_o_id, d_id, w_id));
            conn.hih.getColumnMetadata();
            if (conn.hih.delivery()){
                c_id = Integer.parseInt(conn.hih.getColumn(1));
            }
            else{
                throw new RuntimeException("O_ID=" + no_o_id + " O_D_ID="
                        + d_id + " O_W_ID=" + w_id + " not found!");
            }
            result = conn.DML(String.format(delivUpdateCarrierIdSQL, o_carrier_id, no_o_id, d_id, w_id));

            if (Integer.parseInt(result.substring(14)) <= 0)
                throw new RuntimeException("O_ID=" + no_o_id + " O_D_ID="
                        + d_id + " O_W_ID=" + w_id + " not found!");

            result = conn.DML(String.format(delivUpdateDeliveryDateSQL, new Timestamp(System.currentTimeMillis()).toString(),
                    no_o_id, d_id, w_id));

            if (Integer.parseInt(result.substring(14)) <= 0){
                throw new RuntimeException("OL_O_ID=" + no_o_id + " OL_D_ID="
                        + d_id + " OL_W_ID=" + w_id + " not found!");
            }
            conn.hih.EXEC_QUERY(String.format(delivSumOrderAmountSQL, no_o_id, d_id, w_id));
            conn.hih.getColumnMetadata();
            if (conn.hih.delivery()){
                ol_total = Float.parseFloat(conn.hih.getColumn(1));
            }else{
                throw new RuntimeException("OL_O_ID=" + no_o_id + " OL_D_ID="
                        + d_id + " OL_W_ID=" + w_id + " not found!");
            }

            result = conn.DML(String.format(delivUpdateCustBalDelivCntSQL,
                    Float.toString(ol_total), w_id, d_id, c_id));

            if (Integer.parseInt(result.substring(14)) <= 0)
                throw new RuntimeException("C_ID=" + c_id + " C_W_ID=" + w_id
                        + " C_D_ID=" + d_id + " not found!");
        }
        //COMMIT
        conn.TCL("commit");


        //TODO: This part is not used
        StringBuilder terminalMessage = new StringBuilder();
        terminalMessage
                .append("\n+---------------------------- DELIVERY ---------------------------+\n");
        terminalMessage.append(" Date: ");
        terminalMessage.append(TPCCUtil.getCurrentTime());
        terminalMessage.append("\n\n Warehouse: ");
        terminalMessage.append(w_id);
        terminalMessage.append("\n Carrier:   ");
        terminalMessage.append(o_carrier_id);
        terminalMessage.append("\n\n Delivered Orders\n");
        int skippedDeliveries = 0;
        for (int i = 1; i <= 10; i++) {
            if (orderIDs[i - 1] >= 0) {
                terminalMessage.append("  District ");
                terminalMessage.append(i < 10 ? " " : "");
                terminalMessage.append(i);
                terminalMessage.append(": Order number ");
                terminalMessage.append(orderIDs[i - 1]);
                terminalMessage.append(" was delivered.\n");
            } else {
                terminalMessage.append("  District ");
                terminalMessage.append(i < 10 ? " " : "");
                terminalMessage.append(i);
                terminalMessage.append(": No orders to be delivered.\n");
                skippedDeliveries++;
            }
        }
        terminalMessage.append("+-----------------------------------------------------------------+\n\n");

        return skippedDeliveries;
    }



}

