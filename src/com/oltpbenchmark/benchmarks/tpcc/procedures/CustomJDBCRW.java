package com.oltpbenchmark.benchmarks.tpcc.procedures;

import com.oltpbenchmark.api.HihActorConnection;
import com.oltpbenchmark.api.HihConnection;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Random;

/**
 * Created by ilvoladore on 22/02/17.
 */
public class CustomJDBCRW extends TPCCProcedure {

    //Statements
    public String  stmtUpdateDistSQL = "UPDATE " + TPCCConstants.TABLENAME_DISTRICT + " SET D_NEXT_O_ID = D_NEXT_O_ID + 1 WHERE D_W_ID = %d AND D_ID = %d";
    public String payUpdateWhseSQL = "UPDATE " + TPCCConstants.TABLENAME_WAREHOUSE + " SET W_YTD = W_YTD + %s  WHERE W_ID = %d ";
    public String delivGetOrderIdSQL = "SELECT NO_O_ID FROM " + TPCCConstants.TABLENAME_NEWORDER + " WHERE NO_D_ID = %d"
            + " AND NO_W_ID = %d ORDER BY NO_O_ID ASC LIMIT 1";
    public String delivUpdateDeliveryDateSQL = "UPDATE " + TPCCConstants.TABLENAME_ORDERLINE + " SET OL_DELIVERY_D = '%s' "
            + " WHERE OL_O_ID = %d"
            + " AND OL_D_ID = %d"
            + " AND OL_W_ID = %d";

    @Override
    public ResultSet run(Connection conn, Random gen,
                         int terminalWarehouseID, int numWarehouses,
                         int terminalDistrictLowerID, int terminalDistrictUpperID,
                         TPCCWorker w) throws SQLException {
        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);
        float paymentAmount = (float) (TPCCUtil.randomNumber(100, 500000, gen) / 100.0);
        customReadWriteTxn(terminalWarehouseID, districtID, paymentAmount, conn);
        return null;
    }

    public ResultSet run(HihActorConnection conn, Random gen,
                         int terminalWarehouseID, int numWarehouses,
                         int terminalDistrictLowerID, int terminalDistrictUpperID,
                         TPCCWorker w) {

        return null;
    }

    @Override
    public ResultSet run(HihConnection conn, Random gen,
                         int terminalWarehouseID, int numWarehouses,
                         int terminalDistrictLowerID, int terminalDistrictUpperID,
                         TPCCWorker w){
        return null;
    }
    private void customReadWriteTxn(int w_id, int d_id, float h_amount, Connection conn) {
        try {
            conn.setAutoCommit(false);
            int rs = conn.createStatement().executeUpdate(String.format(stmtUpdateDistSQL, w_id, d_id));
            //String result = conn.DML(String.format(stmtUpdateDistSQL, w_id, d_id));
            if (rs < 1)
                throw new RuntimeException(
                        "Error!! Cannot update next_order_id on district for D_ID="
                                + d_id + " D_W_ID=" + w_id);

            rs = conn.createStatement().executeUpdate(String.format(payUpdateWhseSQL, Float.toString(h_amount), w_id));
            if (rs < 1)
                throw new RuntimeException("W_ID=" + w_id + " not found!");

            ResultSet result = conn.createStatement().executeQuery(String.format(delivGetOrderIdSQL, d_id, w_id));
            int no_o_id;
            if (result.next()) {
                no_o_id = result.getInt(1);
            } else {
                // This district has no new orders; this can happen but should
                // be rare
                return;
            }

            rs = conn.createStatement().executeUpdate(String.format(
                    delivUpdateDeliveryDateSQL, new Timestamp(System.currentTimeMillis()).toString(),
                    no_o_id, d_id, w_id));
            if (rs < 1) {
                throw new RuntimeException("OL_O_ID=" + no_o_id + " OL_D_ID="
                        + d_id + " OL_W_ID=" + w_id + " not found!");

            }
            conn.commit();
        }catch (SQLException r){
            r.printStackTrace();
        }
    }
}
