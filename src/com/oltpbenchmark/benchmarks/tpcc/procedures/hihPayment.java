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
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.oltpbenchmark.api.HihConnection;
import org.apache.log4j.Logger;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import org.apache.log4j.net.SyslogAppender;

public class hihPayment extends TPCCProcedure {

    private static final Logger LOG = Logger.getLogger(Payment.class);
    private HihConnection conn = null;

    public String payUpdateWhseSQL = "UPDATE " + TPCCConstants.TABLENAME_WAREHOUSE + " SET W_YTD = W_YTD + %s  WHERE ( W_ID = %d )";
    public String payGetWhseSQL = "SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME"
            + " FROM " + TPCCConstants.TABLENAME_WAREHOUSE + " WHERE ( W_ID = %d )";
    public String payUpdateDistSQL = "UPDATE " + TPCCConstants.TABLENAME_DISTRICT + " SET D_YTD = D_YTD + %s WHERE ( D_W_ID = %d AND D_ID = %d )";
    public String payGetDistSQL = "SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME"
            + " FROM " + TPCCConstants.TABLENAME_DISTRICT + " WHERE ( D_W_ID = %d AND D_ID = %d )";
    public String payGetCustSQL = "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, "
            + "C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, "
            + "C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE "
            + "( C_W_ID = %d AND C_D_ID = %d AND C_ID = %d )";
    public String payGetCustCdataSQL = "SELECT C_DATA FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE ( C_W_ID = %d AND C_D_ID = %d AND C_ID = %d )";
    public String payUpdateCustBalCdataSQL = "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET C_BALANCE = %s, C_YTD_PAYMENT = %s, "
            + "C_PAYMENT_CNT = %d, C_DATA = '%s' "
            + "WHERE ( C_W_ID = %d AND C_D_ID = %d AND C_ID = %d )";
    public String payUpdateCustBalSQL = "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET C_BALANCE = %s, C_YTD_PAYMENT = %s, "
            + "C_PAYMENT_CNT = %d WHERE ( C_W_ID = %d AND C_D_ID = %d AND C_ID = %d )";
    public String payInsertHistSQL = "INSERT INTO " + TPCCConstants.TABLENAME_HISTORY + " (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) "
            + " VALUES (%d,%d,%d,%d,%d,'%s',%s,'%s')";
    public String customerByNameSQL = "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, "
            + "C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, "
            + "C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + TPCCConstants.TABLENAME_CUSTOMER + " "
            + "WHERE C_W_ID = %d AND C_D_ID = %d AND C_LAST = '%s' ORDER BY C_FIRST";


    public ResultSet run(Connection conn, Random gen,
                         int terminalWarehouseID, int numWarehouses,
                         int terminalDistrictLowerID, int terminalDistrictUpperID,
                         TPCCWorker w){
        return null;
    }

    public ResultSet run(HihActorConnection conn, Random gen,
                         int terminalWarehouseID, int numWarehouses,
                         int terminalDistrictLowerID, int terminalDistrictUpperID,
                         TPCCWorker w){
        return null;
    }

    public ResultSet run(HihConnection util, Random gen, int terminalWarehouseID, int numWarehouses,
                         int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w)
    {

        // payUpdateWhse =this.getPreparedStatement(conn, payUpdateWhseSQL);
        this.conn=util;

        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);
        int customerID; //TPCCUtil.getCustomerID(gen);

        int x = TPCCUtil.randomNumber(1, 100, gen);
        int customerDistrictID;
        int customerWarehouseID;
        if (x <= 85) {
            customerDistrictID = districtID;
            customerWarehouseID = terminalWarehouseID;
        } else {
            customerDistrictID = TPCCUtil.randomNumber(1,
                    jTPCCConfig.configDistPerWhse, gen);
            do {
                customerWarehouseID = TPCCUtil.randomNumber(1,
                        numWarehouses, gen);
            } while (customerWarehouseID == terminalWarehouseID
                    && numWarehouses > 1);
        }

        long y = TPCCUtil.randomNumber(1, 100, gen);
        boolean customerByName;
        String customerLastName = null;
        customerID = -1;
        if (y <= 60) {
            // 60% lookups by last name
            customerByName = true;
            customerLastName = TPCCUtil
                    .getNonUniformRandomLastNameForRun(gen);
        } else {
            // 40% lookups by customer ID
            customerByName = false;
            customerID = TPCCUtil.getCustomerID(gen);
        }

        float paymentAmount = (float) (TPCCUtil.randomNumber(100, 500000, gen) / 100.0);

        paymentTransaction(terminalWarehouseID,
                customerWarehouseID, paymentAmount, districtID,
                customerDistrictID, customerID,
                customerLastName, customerByName, conn, w);

        return null;

    }



    private void paymentTransaction(int w_id, int c_w_id, float h_amount,
                                    int d_id, int c_d_id, int c_id, String c_last, boolean c_by_name, HihConnection conn, TPCCWorker w)
    {
        //START
        conn.START_TX();
        String w_street_1, w_street_2, w_city, w_state, w_zip, w_name;
        String d_street_1, d_street_2, d_city, d_state, d_zip, d_name;


        String result = conn.DML(String.format(payUpdateWhseSQL, Float.toString(h_amount), w_id));
        if (result.equals("affected rows 0"))
            throw new RuntimeException("W_ID=" + w_id + " not found!");

        conn.hih.EXEC_QUERY(String.format(payGetWhseSQL, w_id));
        conn.hih.getColumnMetadata();
        if (conn.hih.delivery()){
            w_street_1 = conn.hih.getColumn(1);
            w_street_2 = conn.hih.getColumn(2);
            w_city  = conn.hih.getColumn(3);
            w_state = conn.hih.getColumn(4);
            w_zip   = conn.hih.getColumn(5);
            w_name  = conn.hih.getColumn(6);
        }else{
            throw new RuntimeException("W_ID=" + w_id + " not found!");
        }


        result = conn.DML(String.format(payUpdateDistSQL, Float.toString(h_amount), w_id, d_id));
        if (Integer.parseInt(result.substring(14)) <= 0)
            throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id
                    + " not found!");


        conn.hih.EXEC_QUERY(String.format(payGetDistSQL, w_id, d_id));
        conn.hih.getColumnMetadata();
        if (conn.hih.delivery()){
            d_street_1 = conn.hih.getColumn(1);
            d_street_2 = conn.hih.getColumn(2);
            d_city  = conn.hih.getColumn(3);
            d_state = conn.hih.getColumn(4);
            d_zip   = conn.hih.getColumn(5);
            d_name  = conn.hih.getColumn(6);
        }else{
            throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id
                    + " not found!");
        }


        Customer c;
        if (c_by_name) {
            assert c_id <= 0;
            c = getCustomerByName(c_w_id, c_d_id, c_last, conn);
        } else {
            assert c_last == null;
            c = getCustomerById(c_w_id, c_d_id, c_id, conn);
        }

        c.c_balance -= h_amount;
        c.c_ytd_payment += h_amount;
        c.c_payment_cnt += 1;
        String c_data = null;
        if (c.c_credit.equals("BC")) { // bad credit

            conn.hih.EXEC_QUERY(String.format(payGetCustCdataSQL, c_w_id, c_d_id, c.c_id));
            conn.hih.getColumnMetadata();
            if (conn.hih.delivery()){
                c_data = conn.hih.getColumn(1);
            }else{
                throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID="
                        + c_w_id + " C_D_ID=" + c_d_id + " not found!");
            }

            c_data = c.c_id + " " + c_d_id + " " + c_w_id + " " + d_id + " "
                    + w_id + " " + h_amount + " : " + c_data;
            if (c_data.length() > 500)
                c_data = c_data.substring(0, 500);

            result = conn.DML(String.format(payUpdateCustBalCdataSQL, Float.toString(c.c_balance),
                    Float.toString(c.c_ytd_payment), c.c_payment_cnt, c_data, c_w_id, c_d_id, c.c_id));
            if (Integer.parseInt(result.substring(14)) <= 0)
                throw new RuntimeException(
                        "Error in PAYMENT Txn updating Customer C_ID=" + c.c_id
                                + " C_W_ID=" + c_w_id + " C_D_ID=" + c_d_id);

        } else {
            // GoodCredit
            result = conn.DML(String.format(payUpdateCustBalSQL, Float.toString(c.c_balance),
                    Float.toString(c.c_ytd_payment), c.c_payment_cnt, c_w_id, c_d_id, c.c_id));
            if (Integer.parseInt(result.substring(14)) <= 0)
                throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID="
                        + c_w_id + " C_D_ID=" + c_d_id + " not found!");
        }

        if (w_name.length() > 10)
            w_name = w_name.substring(0, 10);
        if (d_name.length() > 10)
            d_name = d_name.substring(0, 10);
        String h_data = w_name + "    " + d_name;

        result = conn.DML(String.format(payInsertHistSQL, c_d_id, c_w_id, c.c_id,
                d_id, w_id, new Timestamp(System.currentTimeMillis()).toString(), Float.toString(h_amount), h_data));
        if (Integer.parseInt(result.substring(14)) <= 0)
            throw new RuntimeException("Aris Error_Cannot INSERT payInsertHistSQL: "+String.format(payInsertHistSQL, c_d_id, c_w_id, c.c_id,
                    d_id, w_id, new Timestamp(System.currentTimeMillis()).toString(), Float.toString(h_amount), h_data));

        //conn.commit();
        //Hihooi Commit
        conn.TCL("commit");

        /*StringBuilder terminalMessage = new StringBuilder();
        terminalMessage
                .append("\n+---------------------------- PAYMENT ----------------------------+");
        terminalMessage.append("\n Date: " + TPCCUtil.getCurrentTime());
        terminalMessage.append("\n\n Warehouse: ");
        terminalMessage.append(w_id);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(w_street_1);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(w_street_2);
        terminalMessage.append("\n   City:    ");
        terminalMessage.append(w_city);
        terminalMessage.append("   State: ");
        terminalMessage.append(w_state);
        terminalMessage.append("  Zip: ");
        terminalMessage.append(w_zip);
        terminalMessage.append("\n\n District:  ");
        terminalMessage.append(d_id);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(d_street_1);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(d_street_2);
        terminalMessage.append("\n   City:    ");
        terminalMessage.append(d_city);
        terminalMessage.append("   State: ");
        terminalMessage.append(d_state);
        terminalMessage.append("  Zip: ");
        terminalMessage.append(d_zip);
        terminalMessage.append("\n\n Customer:  ");
        terminalMessage.append(c.c_id);
        terminalMessage.append("\n   Name:    ");
        terminalMessage.append(c.c_first);
        terminalMessage.append(" ");
        terminalMessage.append(c.c_middle);
        terminalMessage.append(" ");
        terminalMessage.append(c.c_last);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(c.c_street_1);
        terminalMessage.append("\n   Street:  ");
        terminalMessage.append(c.c_street_2);
        terminalMessage.append("\n   City:    ");
        terminalMessage.append(c.c_city);
        terminalMessage.append("   State: ");
        terminalMessage.append(c.c_state);
        terminalMessage.append("  Zip: ");
        terminalMessage.append(c.c_zip);
        terminalMessage.append("\n   Since:   ");
        if (c.c_since != null) {
            terminalMessage.append(c.c_since.toString());
        } else {
            terminalMessage.append("");
        }
        terminalMessage.append("\n   Credit:  ");
        terminalMessage.append(c.c_credit);
        terminalMessage.append("\n   %Disc:   ");
        terminalMessage.append(c.c_discount);
        terminalMessage.append("\n   Phone:   ");
        terminalMessage.append(c.c_phone);
        terminalMessage.append("\n\n Amount Paid:      ");
        terminalMessage.append(h_amount);
        terminalMessage.append("\n Credit Limit:     ");
        terminalMessage.append(c.c_credit_lim);
        terminalMessage.append("\n New Cust-Balance: ");
        terminalMessage.append(c.c_balance);
        if (c.c_credit.equals("BC")) {
            if (c_data.length() > 50) {
                terminalMessage.append("\n\n Cust-Data: "
                        + c_data.substring(0, 50));
                int data_chunks = c_data.length() > 200 ? 4
                        : c_data.length() / 50;
                for (int n = 1; n < data_chunks; n++)
                    terminalMessage.append("\n            "
                            + c_data.substring(n * 50, (n + 1) * 50));
            } else {
                terminalMessage.append("\n\n Cust-Data: " + c_data);
            }
        }
        terminalMessage.append("\n+-----------------------------------------------------------------+\n\n");

        if(LOG.isTraceEnabled())LOG.trace(terminalMessage.toString());*/

    }

    // attention duplicated code across trans... ok for now to maintain separate prepared statements
    public Customer getCustomerById(int c_w_id, int c_d_id, int c_id, HihConnection conn){

        conn.hih.EXEC_QUERY(String.format(payGetCustSQL, c_w_id, c_d_id, c_id));
        conn.hih.getColumnMetadata();
        if (conn.hih.delivery()) {
            Map<String, String> customer = new HashMap<>();
            for (int q = 1; q <= conn.hih.getColumnCount(); q++) {
                customer.put(conn.hih.getColumnName(q).trim().toLowerCase(), conn.hih.getColumn(q));
            }
            Customer c = TPCCUtil.newCustomerFromResults(customer);
            c.c_id = c_id;
            c.c_last = conn.hih.getColumn("C_LAST");
            return c;
        }else{
            throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + c_d_id
                    + " C_W_ID=" + c_w_id + " not found!");
        }
    }

    //attention this code is repeated in other transacitons... ok for now to allow for separate statements.
    public Customer getCustomerByName(int c_w_id, int c_d_id, String c_last, HihConnection conn ) {

        ArrayList<Customer> customers = new ArrayList<Customer>();
        conn.hih.EXEC_QUERY(String.format(customerByNameSQL, c_w_id, c_d_id, c_last));
        conn.hih.getColumnMetadata();
        while (conn.hih.delivery()) {
            Map<String, String> customer = new HashMap<>();
            for (int q = 1; q <= conn.hih.getColumnCount(); q++) {
                customer.put(conn.hih.getColumnName(q).trim().toLowerCase(), conn.hih.getColumn(q));
            }
            Customer c = TPCCUtil.newCustomerFromResults(customer);
            c.c_id = Integer.parseInt(conn.hih.getColumn("C_ID").trim());
            c.c_last = c_last;
            customers.add(c);
        }

        if (customers.size() == 0) {
            throw new RuntimeException("C_LAST=" + c_last + " C_D_ID=" + c_d_id
                    + " C_W_ID=" + c_w_id + " not found!");
        }

        // TPC-C 2.5.2.2: Position n / 2 rounded up to the next integer, but
        // that
        // counts starting from 1.
        int index = customers.size() / 2;
        if (customers.size() % 2 == 0) {
            index -= 1;
        }
        return customers.get(index);
    }


}

