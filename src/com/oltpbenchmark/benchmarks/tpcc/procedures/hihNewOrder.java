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
import com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Map;
import java.util.Random;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Random;

import com.oltpbenchmark.api.HihConnection;
import org.apache.log4j.Logger;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig;

public class hihNewOrder extends TPCCProcedure {

    private static final Logger LOG = Logger.getLogger(NewOrder.class);

    private HihConnection conn;

    public String stmtGetCustWhseSQL =
            "SELECT C_DISCOUNT, C_LAST, C_CREDIT, W_TAX"
                    + "  FROM " + TPCCConstants.TABLENAME_CUSTOMER + ", " + TPCCConstants.TABLENAME_WAREHOUSE
                    + " WHERE ( W_ID = %d AND C_W_ID = %d"
                    + " AND C_D_ID = %d AND C_ID = %d )";

    public String stmtGetDistSQL =
            "SELECT D_NEXT_O_ID, D_TAX FROM " + TPCCConstants.TABLENAME_DISTRICT
                    + " WHERE ( D_W_ID = %d AND D_ID = %d )"; //Aris: FOR UPDATE

    public String  stmtInsertNewOrderSQL = "INSERT INTO "+ TPCCConstants.TABLENAME_NEWORDER + " (NO_O_ID, NO_D_ID, NO_W_ID) VALUES ( %d, %d, %d)";

    public String  stmtUpdateDistSQL = "UPDATE " + TPCCConstants.TABLENAME_DISTRICT + " SET D_NEXT_O_ID = D_NEXT_O_ID + 1 WHERE ( D_W_ID = %d AND D_ID = %d )";

    public String  stmtInsertOOrderSQL = "INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER
            + " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)"
            + " VALUES (%d, %d, %d, %d, '%s', %d, %d)";

    public String  stmtGetItemSQL = "SELECT I_PRICE,I_NAME ,I_DATA FROM " + TPCCConstants.TABLENAME_ITEM + " WHERE ( I_ID=%d )";

    public String  stmtGetStockSQL = "SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, "
            + "       S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10"
            + " FROM " + TPCCConstants.TABLENAME_STOCK + " WHERE ( S_I_ID = %d AND S_W_ID = %d )"; //ARIS:FOR UPDATE

    public String  stmtUpdateStockSQL = "UPDATE " + TPCCConstants.TABLENAME_STOCK + " SET S_QUANTITY = %d , S_YTD = S_YTD + %d, S_ORDER_CNT = S_ORDER_CNT + 1, S_REMOTE_CNT = S_REMOTE_CNT + %d "
            + " WHERE ( S_I_ID = %d AND S_W_ID = %d )";

    public String  stmtInsertOrderLineSQL = "INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE + " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID,"
            + "  OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (%d,%d,%d,%d,%d,%d,%d,%s,'%s')";


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

    //Method overriding for Hihooi
    public ResultSet run(HihConnection conn, Random gen,
                         int terminalWarehouseID, int numWarehouses,
                         int terminalDistrictLowerID, int terminalDistrictUpperID,
                         TPCCWorker w){

        this.conn=conn;
        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);
        int customerID = TPCCUtil.getCustomerID(gen);

        int numItems = (int) TPCCUtil.randomNumber(5, 15, gen);
        int[] itemIDs = new int[numItems];
        int[] supplierWarehouseIDs = new int[numItems];
        int[] orderQuantities = new int[numItems];
        int allLocal = 1;
        for (int i = 0; i < numItems; i++) {
            itemIDs[i] = TPCCUtil.getItemID(gen);
            if (TPCCUtil.randomNumber(1, 100, gen) > 1) {
                supplierWarehouseIDs[i] = terminalWarehouseID;
            } else {
                do {
                    supplierWarehouseIDs[i] = TPCCUtil.randomNumber(1,
                            numWarehouses, gen);
                } while (supplierWarehouseIDs[i] == terminalWarehouseID
                        && numWarehouses > 1);
                allLocal = 0;
            }
            orderQuantities[i] = TPCCUtil.randomNumber(1, 10, gen);
        }

        // we need to cause 1% of the new orders to be rolled back.
        if (TPCCUtil.randomNumber(1, 100, gen) == 1)
            itemIDs[numItems - 1] = jTPCCConfig.INVALID_ITEM_ID;

        newOrderTransaction(terminalWarehouseID, districtID,
                customerID, numItems, allLocal, itemIDs,
                supplierWarehouseIDs, orderQuantities, w);

        return null;
    }


    private void newOrderTransaction(int w_id, int d_id, int c_id,
                                     int o_ol_cnt, int o_all_local, int[] itemIDs,
                                     int[] supplierWarehouseIDs, int[] orderQuantities,  TPCCWorker w)
             {
        float c_discount, w_tax, d_tax = 0, i_price;
        int d_next_o_id, o_id = -1, s_quantity;
        String c_last = null, c_credit = null, i_name, i_data, s_data;
        String s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05;
        String s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, ol_dist_info = null;
        float[] itemPrices = new float[o_ol_cnt];
        float[] orderLineAmounts = new float[o_ol_cnt];
        String[] itemNames = new String[o_ol_cnt];
        int[] stockQuantities = new int[o_ol_cnt];
        char[] brandGeneric = new char[o_ol_cnt];
        int ol_supply_w_id, ol_i_id, ol_quantity;
        int s_remote_cnt_increment;
        float ol_amount, total_amount = 0;
        try
        {

            //START_TX
            conn.START_TX();

            conn.hih.EXEC_QUERY(String.format(stmtGetCustWhseSQL, w_id, w_id, d_id, c_id));
            conn.hih.getColumnMetadata();
            if (conn.hih.delivery()){
            }
            else{
                throw new RuntimeException("W_ID=" + w_id + " C_D_ID=" + d_id
                        + " C_ID=" + c_id + " not found!");
            }
            /*c_discount = 2;
            c_last = rs.get("C_LAST");
            c_credit = rs.get("C_CREDIT");
            w_tax = Float.parseFloat(rs.get("W_TAX"));
            rs = null;*/


            conn.hih.EXEC_QUERY(String.format(stmtGetDistSQL, w_id, d_id));
            conn.hih.getColumnMetadata();
            if (conn.hih.delivery()){
                d_next_o_id = Integer.parseInt(conn.hih.getColumn(1).trim());
                d_tax = Float.parseFloat(conn.hih.getColumn(2).trim());
            }else{
                throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id
                        + " not found!");
            }

            String result = conn.DML(String.format(stmtUpdateDistSQL, w_id, d_id));
            if (result.equalsIgnoreCase("affected rows -1"))
                throw new RuntimeException(
                        "Error!! Cannot update next_order_id on district for D_ID="
                                + d_id + " D_W_ID=" + w_id);

            o_id = d_next_o_id;

            result = conn.DML(String.format(stmtInsertOOrderSQL,
                    o_id, d_id, w_id, c_id, new Timestamp(System.currentTimeMillis()).toString(), o_ol_cnt, o_all_local));
            if (result.equalsIgnoreCase("affected rows -1")){
                System.out.println("RESULT: "+result);
                throw new RuntimeException(
                        "Aris Error!! Cannot INSERT stmtInsertOOrderSQL: "+String.format(stmtInsertOOrderSQL,
                                o_id, d_id, w_id, c_id, new Timestamp(System.currentTimeMillis()).toString(), o_ol_cnt, o_all_local));
            }

            result = conn.DML(String.format(stmtInsertNewOrderSQL, o_id, d_id, w_id));
            if (result.equalsIgnoreCase("affected rows -1"))
                throw new RuntimeException(
                        "Aris Error!! Cannot INSERT stmtInsertNewOrderSQL: "+String.format(stmtInsertNewOrderSQL, o_id, d_id, w_id));

            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
                ol_i_id = itemIDs[ol_number - 1];
                ol_quantity = orderQuantities[ol_number - 1];

                conn.hih.EXEC_QUERY(String.format(stmtGetItemSQL, ol_i_id));
                conn.hih.getColumnMetadata();
                if (conn.hih.delivery()){
                    i_price = Float.parseFloat(conn.hih.getColumn(1).trim());
                    i_name = conn.hih.getColumn(2);
                    i_data = conn.hih.getColumn(3);
                    itemPrices[ol_number - 1] = i_price;
                    itemNames[ol_number - 1] = i_name;

                }else{
                    // This is (hopefully) an expected error: this is an
                    // expected new order rollback
                    assert ol_number == o_ol_cnt;
                    assert ol_i_id == jTPCCConfig.INVALID_ITEM_ID;
                    //rs.close();
                    throw new UserAbortException(
                            "EXPECTED new order rollback: I_ID=" + ol_i_id
                                    + " not found!");
                }
                conn.hih.EXEC_QUERY(String.format(stmtGetStockSQL, ol_i_id, ol_supply_w_id));
                conn.hih.getColumnMetadata();
                if (conn.hih.delivery()){
                    s_quantity = Integer.parseInt(conn.hih.getColumn(1).trim());
                    s_data = conn.hih.getColumn(2);
                    s_dist_01 = conn.hih.getColumn(3);
                    s_dist_02 = conn.hih.getColumn(4);
                    s_dist_03 = conn.hih.getColumn(5);
                    s_dist_04 = conn.hih.getColumn(6);
                    s_dist_05 = conn.hih.getColumn(7);
                    s_dist_06 = conn.hih.getColumn(8);
                    s_dist_07 = conn.hih.getColumn(9);
                    s_dist_08 = conn.hih.getColumn(10);
                    s_dist_09 = conn.hih.getColumn(11);
                    s_dist_10 = conn.hih.getColumn(12);

                }
                else{
                    throw new RuntimeException("I_ID=" + ol_i_id
                            + " not found!");
                }

                stockQuantities[ol_number - 1] = s_quantity;

                if (s_quantity - ol_quantity >= 10) {
                    s_quantity -= ol_quantity;
                } else {
                    s_quantity += -ol_quantity + 91;
                }

                if (ol_supply_w_id == w_id) {
                    s_remote_cnt_increment = 0;
                } else {
                    s_remote_cnt_increment = 1;
                }
                String updateQ = String.format(stmtUpdateStockSQL, s_quantity, ol_quantity, s_remote_cnt_increment,
                        ol_i_id, ol_supply_w_id);
                /*result = conn.DML(updateQ);
                if (Integer.parseInt(result.substring(14)) <= 0)
                    throw new RuntimeException(
                            "Aris Error!! Cannot UPDATE stmtUpdateStockSQL: "+updateQ);*/

                ol_amount = ol_quantity * i_price;
                orderLineAmounts[ol_number - 1] = ol_amount;
                total_amount += ol_amount;

                if (i_data.indexOf("GENERIC") != -1
                        && s_data.indexOf("GENERIC") != -1) {
                    brandGeneric[ol_number - 1] = 'B';
                } else {
                    brandGeneric[ol_number - 1] = 'G';
                }

                switch ((int) d_id) {
                    case 1:
                        ol_dist_info = s_dist_01;
                        break;
                    case 2:
                        ol_dist_info = s_dist_02;
                        break;
                    case 3:
                        ol_dist_info = s_dist_03;
                        break;
                    case 4:
                        ol_dist_info = s_dist_04;
                        break;
                    case 5:
                        ol_dist_info = s_dist_05;
                        break;
                    case 6:
                        ol_dist_info = s_dist_06;
                        break;
                    case 7:
                        ol_dist_info = s_dist_07;
                        break;
                    case 8:
                        ol_dist_info = s_dist_08;
                        break;
                    case 9:
                        ol_dist_info = s_dist_09;
                        break;
                    case 10:
                        ol_dist_info = s_dist_10;
                        break;
                }

                result = conn.DML(String.format(stmtInsertOrderLineSQL, o_id, d_id, w_id,
                        ol_number, ol_i_id, ol_supply_w_id, ol_quantity, Float.toString(ol_amount), ol_dist_info));
                if (result.equalsIgnoreCase("affected rows -1"))
                    throw new RuntimeException(
                            "Aris Error!! Cannot INSERT stmtInsertOrderLineSQL: "+
                                    String.format(stmtInsertOrderLineSQL, o_id, d_id, w_id,
                                            ol_number, ol_i_id, ol_supply_w_id, ol_quantity, Float.toString(ol_amount), ol_dist_info));

                result = conn.DML(updateQ);
                if (result.equalsIgnoreCase("affected rows -1"))
                    throw new RuntimeException(
                            "Aris Error!! Cannot UPDATE stmtUpdateStockSQL: "+updateQ);

            } // end-for

            //total_amount *= (1 + w_tax + d_tax) * (1 - c_discount);

            conn.TCL("commit");

        } catch(UserAbortException userEx)
        {
            LOG.debug("Caught an expected error in New Order");
            throw userEx;
        }
        /*
        finally {
            if (stmtInsertOrderLine != null)
                stmtInsertOrderLine.clearBatch();
            if (stmtUpdateStock != null)
                stmtUpdateStock.clearBatch();
        }*/

    }

}

