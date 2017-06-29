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
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.*;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Random;

import com.oltpbenchmark.api.HihConnection;
import org.apache.log4j.Logger;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;

public class hihOrderStatus extends TPCCProcedure {

    private static final Logger LOG = Logger.getLogger(OrderStatus.class);

    public String ordStatGetNewestOrdSQL = "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM " + TPCCConstants.TABLENAME_OPENORDER
            + " WHERE O_W_ID = %d"
            + " AND O_D_ID = %d AND O_C_ID = %d ORDER BY O_ID DESC LIMIT 1";

    public String ordStatGetOrderLinesSQL = "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY,"
            + " OL_AMOUNT, OL_DELIVERY_D"
            + " FROM " + TPCCConstants.TABLENAME_ORDERLINE
            + " WHERE OL_O_ID = %d"
            + " AND OL_D_ID =%d"
            + " AND OL_W_ID = %d";

    public String payGetCustSQL = "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, "
            + "C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, "
            + "C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE "
            + "( C_W_ID = %d AND C_D_ID = %d AND C_ID = %d )";

    public String customerByNameSQL = "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, "
            + "C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, "
            + "C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + TPCCConstants.TABLENAME_CUSTOMER
            + " WHERE C_W_ID = %d AND C_D_ID = %d AND C_LAST = '%s' ORDER BY C_FIRST";

    private HihConnection conn;

    public ResultSet run(Connection conn, Random gen,
                         int terminalWarehouseID, int numWarehouses,
                         int terminalDistrictLowerID, int terminalDistrictUpperID,
                         TPCCWorker w) throws SQLException  {
        return null;
    }

    public ResultSet run(HihActorConnection conn, Random gen,
                         int terminalWarehouseID, int numWarehouses,
                         int terminalDistrictLowerID, int terminalDistrictUpperID,
                         TPCCWorker w) {
        return null;
    }


    public ResultSet run(HihConnection util, Random gen,
                         int terminalWarehouseID, int numWarehouses,
                         int terminalDistrictLowerID, int terminalDistrictUpperID,
                         TPCCWorker w) {

        this.conn=util;
        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);
        boolean isCustomerByName=false;
        int y = TPCCUtil.randomNumber(1, 100, gen);
        String customerLastName = null;
        int customerID = -1;
        if (y <= 60) {
            isCustomerByName = true;
            customerLastName = TPCCUtil
                    .getNonUniformRandomLastNameForRun(gen);
        } else {
            isCustomerByName = false;
            customerID = TPCCUtil.getCustomerID(gen);
        }

        orderStatusTransaction(terminalWarehouseID, districtID,
                customerID, customerLastName, isCustomerByName, conn, w);
        return null;
    }

    // attention duplicated code across trans... ok for now to maintain separate prepared statements
    public Customer getCustomerById(int c_w_id, int c_d_id, int c_id, HihConnection conn) {

        conn.hih.EXEC_QUERY(String.format(payGetCustSQL, c_w_id, c_d_id, c_id));
        conn.hih.getColumnMetadata();
        if (conn.hih.delivery()) {
            Map<String, String> customer = new HashMap<>();
            for (int q=1; q<=conn.hih.getColumnCount(); q++) {
                customer.put(conn.hih.getColumnName(q).trim().toLowerCase(), conn.hih.getColumn(q));
            }
            Customer c = TPCCUtil.newCustomerFromResults(customer);
            c.c_id = c_id;
            c.c_last = conn.hih.getColumn("C_LAST");
            return c;
        }
        else{
            throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + c_d_id
                    + " C_W_ID=" + c_w_id + " not found!");
        }
    }

    private void orderStatusTransaction(int w_id, int d_id, int c_id,
                                        String c_last, boolean c_by_name, HihConnection conn, TPCCWorker w) {
        int o_id = -1, o_carrier_id = -1;
        Timestamp entdate;
        ArrayList<String> orderLines = new ArrayList<String>();

        Customer c;
        if (c_by_name) {
            assert c_id <= 0;
            // TODO: This only needs c_balance, c_first, c_middle, c_id
            // only fetch those columns?
            c = getCustomerByName(w_id, d_id, c_last);
        } else {
            assert c_last == null;
            c = getCustomerById(w_id, d_id, c_id,conn);
        }

        // find the newest order for the customer
        // retrieve the carrier & order date for the most recent order.
        /*Map<String, String> rs = conn.QUERY2MAP(String.format(ordStatGetNewestOrdSQL, w_id, d_id, c.c_id));
        if (rs.isEmpty()) {
            throw new RuntimeException("No orders for O_W_ID=" + w_id
                    + " O_D_ID=" + d_id + " O_C_ID=" + c.c_id);
        }
        o_id = Integer.parseInt(rs.get("O_ID"));
        if (!rs.get("O_CARRIER_ID").toString().equals("null")){
            o_carrier_id = Integer.parseInt(rs.get("O_CARRIER_ID"));
        }
        entdate = Timestamp.valueOf(rs.get("O_ENTRY_D"));
        rs = null;*/

        //NEW HIHOOI
        conn.hih.EXEC_QUERY(String.format(ordStatGetNewestOrdSQL, w_id, d_id, c.c_id));
        conn.hih.getColumnMetadata();
        if(conn.hih.delivery()) {
            o_id = Integer.parseInt(conn.hih.getColumn(1).trim());
            o_carrier_id = (conn.hih.getColumn(2).trim().equals("null")) ? -1 : Integer.parseInt(conn.hih.getColumn(2).trim());
            entdate = Timestamp.valueOf(conn.hih.getColumn(3).trim());
        }
        else{
            throw new RuntimeException("No orders for O_W_ID=" + w_id
                    + " O_D_ID=" + d_id + " O_C_ID=" + c.c_id);
        }

        // retrieve the order lines for the most recent order
        /*List<Map<String, String>> result = conn.QUERY2LST(String.format(ordStatGetOrderLinesSQL, o_id, d_id, w_id));
        Iterator<Map<String, String>> r_it = result.iterator();
        while (r_it.hasNext()) {
            Map<String, String> t_res = r_it.next();
            StringBuilder orderLine = new StringBuilder();
            orderLine.append("[");
            orderLine.append(t_res.get("OL_SUPPLY_W_ID"));
            orderLine.append(" - ");
            orderLine.append(t_res.get("OL_I_ID"));
            orderLine.append(" - ");
            orderLine.append(t_res.get("OL_QUANTITY"));
            orderLine.append(" - ");
            orderLine.append(TPCCUtil.formattedDouble(Double.parseDouble(t_res.get("OL_AMOUNT"))));
            orderLine.append(" - ");
            if (t_res.get("OL_DELIVERY_D") != null)
                orderLine.append(t_res.get("OL_DELIVERY_D"));
            else
                orderLine.append("99-99-9999");
            orderLine.append("]");
            orderLines.add(orderLine.toString());
        }
        rs = null;*/

        //NEW HIHOOI
        conn.hih.EXEC_QUERY(String.format(ordStatGetOrderLinesSQL, o_id, d_id, w_id));
        conn.hih.getColumnMetadata();
        while (conn.hih.delivery()){
            StringBuilder orderLine = new StringBuilder();
            orderLine.append("[");
            //orderLine.append(t_res.get("OL_SUPPLY_W_ID"));
            orderLine.append(conn.hih.getColumn(2).trim());
            orderLine.append(" - ");
            //orderLine.append(t_res.get("OL_I_ID"));
            orderLine.append(conn.hih.getColumn(1).trim());
            orderLine.append(" - ");
            //orderLine.append(t_res.get("OL_QUANTITY"));
            orderLine.append(conn.hih.getColumn(3).trim());
            orderLine.append(" - ");
            orderLine.append(TPCCUtil.formattedDouble(Double.parseDouble(conn.hih.getColumn(4).trim())));
            orderLine.append(" - ");
            if (conn.hih.getColumn(5).trim() != null)
                orderLine.append(conn.hih.getColumn(5).trim());
            else
                orderLine.append("99-99-9999");
            orderLine.append("]");
            orderLines.add(orderLine.toString());
        }

        StringBuilder terminalMessage = new StringBuilder();
        terminalMessage.append("\n");
        terminalMessage
                .append("+-------------------------- ORDER-STATUS -------------------------+\n");
        terminalMessage.append(" Date: ");
        terminalMessage.append(TPCCUtil.getCurrentTime());
        terminalMessage.append("\n\n Warehouse: ");
        terminalMessage.append(w_id);
        terminalMessage.append("\n District:  ");
        terminalMessage.append(d_id);
        terminalMessage.append("\n\n Customer:  ");
        terminalMessage.append(c.c_id);
        terminalMessage.append("\n   Name:    ");
        terminalMessage.append(c.c_first);
        terminalMessage.append(" ");
        terminalMessage.append(c.c_middle);
        terminalMessage.append(" ");
        terminalMessage.append(c.c_last);
        terminalMessage.append("\n   Balance: ");
        terminalMessage.append(c.c_balance);
        terminalMessage.append("\n\n");
        if (o_id == -1) {
            terminalMessage.append(" Customer has no orders placed.\n");
        } else {
            terminalMessage.append(" Order-Number: ");
            terminalMessage.append(o_id);
            terminalMessage.append("\n    Entry-Date: ");
            terminalMessage.append(entdate);
            terminalMessage.append("\n    Carrier-Number: ");
            terminalMessage.append(o_carrier_id);
            terminalMessage.append("\n\n");
            if (orderLines.size() != 0) {
                terminalMessage
                        .append(" [Supply_W - Item_ID - Qty - Amount - Delivery-Date]\n");
                for (String orderLine : orderLines) {
                    terminalMessage.append(" ");
                    terminalMessage.append(orderLine);
                    terminalMessage.append("\n");
                }
            } else {
                if(LOG.isTraceEnabled()) LOG.trace(" This Order has no Order-Lines.\n");
            }
        }
        terminalMessage.append("+-----------------------------------------------------------------+\n\n");
        if(LOG.isTraceEnabled()) LOG.trace(terminalMessage.toString());
    }

    //attention this code is repeated in other transacitons... ok for now to allow for separate statements.
    public Customer getCustomerByName(int c_w_id, int c_d_id, String c_last) {
        ArrayList<Customer> customers = new ArrayList<Customer>();
        conn.hih.EXEC_QUERY(String.format(customerByNameSQL, c_w_id, c_d_id, c_last));
        conn.hih.getColumnMetadata();
        while (conn.hih.delivery()) {
            Map<String, String> customer = new HashMap<>();
            for (int q=1; q<=conn.hih.getColumnCount(); q++) {
                customer.put(conn.hih.getColumnName(q).trim().toLowerCase(), conn.hih.getColumn(q));
            }
            Customer c = TPCCUtil.newCustomerFromResults(customer);
            c.c_id = Integer.parseInt(conn.hih.getColumn(3).trim());
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



