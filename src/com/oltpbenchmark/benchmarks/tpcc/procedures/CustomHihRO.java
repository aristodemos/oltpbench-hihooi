package com.oltpbenchmark.benchmarks.tpcc.procedures;

import com.oltpbenchmark.api.HihActorConnection;
import com.oltpbenchmark.api.HihConnection;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import hah.HihMessages;

/**
 * Created by ilvoladore on 26/01/17.
 */
public class CustomHihRO extends TPCCProcedure {

    public String ordStatGetNewestOrdSQL = "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM " + TPCCConstants.TABLENAME_OPENORDER
            + " WHERE O_W_ID = %d"
            + " AND O_D_ID = %d AND O_C_ID = %d ORDER BY O_ID DESC LIMIT 1";

    /*public String ordStatGetOrderLinesSQL = "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY,"
            + " OL_AMOUNT, OL_DELIVERY_D"
            + " FROM " + TPCCConstants.TABLENAME_ORDERLINE
            + " WHERE OL_O_ID = %d"
            + " AND OL_D_ID =%d"
            + " AND OL_W_ID = %d";*/

    public String payGetCustSQL = "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, "
            + "C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, "
            + "C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE "
            + "C_W_ID = %d AND C_D_ID = %d AND C_ID = %d";

    public String customerByNameSQL = "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, "
            + "C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, "
            + "C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + TPCCConstants.TABLENAME_CUSTOMER
            + " WHERE C_W_ID = %d AND C_D_ID = %d AND C_LAST = '%s' ORDER BY C_FIRST";

    private HihConnection conn;

    private HihActorConnection connAct;


    public ResultSet run(HihActorConnection conn, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) {
        this.connAct=conn;
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

        customReadOnlyTxn(terminalWarehouseID, districtID,
                customerID, customerLastName, isCustomerByName, connAct, w);

        return null;
    }

    @Override
    public ResultSet run(Connection conn, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) throws SQLException {
        return null;
    }

    @Override
    public ResultSet run(HihConnection util, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) {
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

        customReadOnlyTxn(terminalWarehouseID, districtID,
                customerID, customerLastName, isCustomerByName, conn, w);

        return null;
    }

    private void customReadOnlyTxn(int w_id, int d_id, int c_id,
                                    String c_last, boolean c_by_name, HihConnection conn, TPCCWorker w){
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
    }

    //Overload
    private void customReadOnlyTxn(int w_id, int d_id, int c_id,
                                   String c_last, boolean c_by_name, HihActorConnection conn, TPCCWorker w){
        int o_id = -1, o_carrier_id = -1;
        Timestamp entdate;
        ArrayList<String> orderLines = new ArrayList<String>();

        Customer c;
        if (c_by_name) {
            assert c_id <= 0;
            // TODO: This only needs c_balance, c_first, c_middle, c_id
            // only fetch those columns?
            c = getCustomerByName(w_id, d_id, c_last, conn);
        } else {
            assert c_last == null;
            c = getCustomerById(w_id, d_id, c_id,conn);
        }

        HihMessages.HihQueryResultSet ans = conn.execQuery(String.format(ordStatGetNewestOrdSQL, w_id, d_id, c.c_id));
        if (ans.getRowsCount()>0){
            o_id = Integer.parseInt(ans.getRows(0).getValue(0));
            try{
                o_carrier_id = Integer.parseInt(ans.getRows(0).getValue(1));
            }catch (NumberFormatException e){
                o_carrier_id = 0;
            }

            entdate = Timestamp.valueOf(ans.getRows(0).getValue(2));
        }
        else{
            throw new RuntimeException("No orders for O_W_ID=" + w_id
                    + " O_D_ID=" + d_id + " O_C_ID=" + c.c_id);
        }
    }

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

    //Override
    public Customer getCustomerById(int c_w_id, int c_d_id, int c_id, HihActorConnection conn) {

        HihMessages.HihQueryResultSet ans = conn.execQuery(String.format(payGetCustSQL, c_w_id, c_d_id, c_id));
        if (ans.getRowsCount()>0){
            Map<String, String> customer = new HashMap<>();
            for (int q=0; q<ans.getColumns().getNameCount(); q++) {
                //customer.put(conn.hih.getColumnName(q).trim().toLowerCase(), conn.hih.getColumn(q));
                customer.put(ans.getColumns().getName(q).trim().toLowerCase(), ans.getRows(0).getValue(q));
            }
            Customer c = TPCCUtil.newCustomerFromResults(customer);
            c.c_id = c_id;
            c.c_last = ans.getRows(0).getValue(2); //"C_LAST"
            return c;
        }
        else{
            throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + c_d_id
                    + " C_W_ID=" + c_w_id + " not found!");
        }
    }

    //Overrde
    public Customer getCustomerByName(int c_w_id, int c_d_id, String c_last, HihActorConnection conn) {
        ArrayList<Customer> customers = new ArrayList<Customer>();
        HihMessages.HihQueryResultSet ans = conn.execQuery(String.format(customerByNameSQL, c_w_id, c_d_id, c_last));
        //conn.hih.getColumnMetadata();
        for(HihMessages.HihRowSet row: ans.getRowsList()){
            Map<String, String> customer = new HashMap<>();
            for (int i=0;i<ans.getColumns().getNameCount();i++){
                customer.put(ans.getColumns().getName(i), row.getValue(i));
            }
            Customer c = TPCCUtil.newCustomerFromResults(customer);
            c.c_id = Integer.parseInt(ans.getRows(0).getValue(2).trim()); //C_ID
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
