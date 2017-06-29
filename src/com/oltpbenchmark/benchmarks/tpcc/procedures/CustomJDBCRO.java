package com.oltpbenchmark.benchmarks.tpcc.procedures;

import com.oltpbenchmark.api.HihActorConnection;
import com.oltpbenchmark.api.HihConnection;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import com.sun.rowset.CachedRowSetImpl;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by ilvoladore on 22/02/17.
 */
public class CustomJDBCRO extends TPCCProcedure {


    public String ordStatGetNewestOrdSQL = "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM " + TPCCConstants.TABLENAME_OPENORDER
            + " WHERE O_W_ID = %d"
            + " AND O_D_ID = %d AND O_C_ID = %d ORDER BY O_ID DESC LIMIT 1";

    public String payGetCustSQL = "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, "
            + "C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, "
            + "C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE "
            + "C_W_ID = %d AND C_D_ID = %d AND C_ID = %d";

    public String customerByNameSQL = "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, "
            + "C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, "
            + "C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + TPCCConstants.TABLENAME_CUSTOMER
            + " WHERE C_W_ID = %d AND C_D_ID = %d AND C_LAST = '%s' ORDER BY C_FIRST";

    @Override
    public ResultSet run(Connection conn, Random gen,
                         int terminalWarehouseID, int numWarehouses,
                         int terminalDistrictLowerID, int terminalDistrictUpperID,
                         TPCCWorker w) throws SQLException {
        //this.conn=util;
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

    @Override
    public ResultSet run(HihConnection conn, Random gen,
                         int terminalWarehouseID, int numWarehouses,
                         int terminalDistrictLowerID, int terminalDistrictUpperID,
                         TPCCWorker w){
        return null;
    }

    public ResultSet run(HihActorConnection conn, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w)  {
        return null;
    }


    //Helper Methods
    private void customReadOnlyTxn(int w_id, int d_id, int c_id,
                                   String c_last, boolean c_by_name, Connection conn, TPCCWorker w) {
        try {
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
                c = getCustomerById(w_id, d_id, c_id, conn);
            }
            //CachedRowSet crs = new CachedRowSetImpl();
            ResultSet rs = conn.createStatement().executeQuery(String.format(ordStatGetNewestOrdSQL, w_id, d_id, c.c_id));
            //crs.populate(rs2);
            if (rs.next()) {
                o_id = rs.getInt(1);
                //o_carrier_id = (rs.getString(2).equals("null")) ? -1 : rs.getInt(2);
                o_carrier_id = rs.getInt(2);
                entdate = rs.getTimestamp(3);
            } else {
                throw new RuntimeException("No orders for O_W_ID=" + w_id
                        + " O_D_ID=" + d_id + " O_C_ID=" + c.c_id);
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    public Customer getCustomerById(int c_w_id, int c_d_id, int c_id, Connection conn) {
        try {
            //CachedRowSet crs = new CachedRowSetImpl();
            ResultSet rs = conn.createStatement().executeQuery(String.format(payGetCustSQL, c_w_id, c_d_id, c_id));
            //crs.populate(rs2);
            if (!rs.next()) {
                throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + c_d_id
                        + " C_W_ID=" + c_w_id + " not found!");
            }

            Customer c = TPCCUtil.newCustomerFromResults(rs);
            c.c_id = c_id;
            c.c_last = rs.getString("C_LAST");
            rs.close();
            return c;
        }catch (SQLException e){
            e.printStackTrace();
            return null;
        }
    }

    public Customer getCustomerByName(int c_w_id, int c_d_id, String c_last, Connection conn) {
        try {
            ArrayList<Customer> customers = new ArrayList<Customer>();
            //CachedRowSet crs = new CachedRowSetImpl();
            ResultSet rs = conn.createStatement().executeQuery(String.format(customerByNameSQL, c_w_id, c_d_id, c_last));
            //crs.populate(rs2);
            while (rs.next()) {
                Customer c = TPCCUtil.newCustomerFromResults(rs);
                c.c_id = rs.getInt("C_ID");
                c.c_last = c_last;
                customers.add(c);
            }
            rs.close();

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
        }catch (SQLException r){
            r.printStackTrace();
            return null;
        }
    }
}
