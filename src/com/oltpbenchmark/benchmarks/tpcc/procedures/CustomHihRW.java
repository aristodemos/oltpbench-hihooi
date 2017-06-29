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
 * Created by ilvoladore on 26/01/17.
 */
public class CustomHihRW extends TPCCProcedure{

    private HihConnection conn;
    //public String  stmtInsertNewOrderSQL = "INSERT INTO "+ TPCCConstants.TABLENAME_NEWORDER + " (NO_O_ID, NO_D_ID, NO_W_ID) VALUES ( %d, %d, %d)";
    public String  stmtUpdateDistSQL = "UPDATE " + TPCCConstants.TABLENAME_DISTRICT + " SET D_NEXT_O_ID = D_NEXT_O_ID + 1 WHERE D_W_ID = %d AND D_ID = %d";
    public String payUpdateWhseSQL = "UPDATE " + TPCCConstants.TABLENAME_WAREHOUSE + " SET W_YTD = W_YTD + %s  WHERE W_ID = %d ";
    public String delivGetOrderIdSQL = "SELECT NO_O_ID FROM " + TPCCConstants.TABLENAME_NEWORDER + " WHERE NO_D_ID = %d"
            + " AND NO_W_ID = %d ORDER BY NO_O_ID ASC LIMIT 1";
    public String delivUpdateDeliveryDateSQL = "UPDATE " + TPCCConstants.TABLENAME_ORDERLINE + " SET OL_DELIVERY_D = '%s' "
            + " WHERE OL_O_ID = %d"
            + " AND OL_D_ID = %d"
            + " AND OL_W_ID = %d";


    @Override
    public ResultSet run(Connection conn, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) throws SQLException {
        return null;
    }

    public ResultSet run(HihActorConnection conn, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w)  {
        return null;
    }


    @Override
    public ResultSet run(HihConnection util, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) {
        this.conn = util;
        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);
        float paymentAmount = (float) (TPCCUtil.randomNumber(100, 500000, gen) / 100.0);
        customReadWriteTxn(terminalWarehouseID, districtID, paymentAmount);
        return null;
    }

    private void customReadWriteTxn(int w_id, int d_id, float h_amount) {
        conn.START_TX();
        String result = conn.DML(String.format(stmtUpdateDistSQL, w_id, d_id));
        if (result.equalsIgnoreCase("affected rows -1"))
            throw new RuntimeException(
                    "Error!! Cannot update next_order_id on district for D_ID="
                            + d_id + " D_W_ID=" + w_id);

        result = conn.DML(String.format(payUpdateWhseSQL, Float.toString(h_amount), w_id));
        if (result.equals("affected rows 0"))
            throw new RuntimeException("W_ID=" + w_id + " not found!");

        conn.hih.EXEC_QUERY(String.format(delivGetOrderIdSQL, d_id, w_id));
        conn.hih.getColumnMetadata();
        int no_o_id;
        if (conn.hih.delivery()) {
            no_o_id = Integer.parseInt(conn.hih.getColumn(1));
        } else {
            // This district has no new orders; this can happen but should
            // be rare
            return;
        }

        result = conn.DML(String.format(delivUpdateDeliveryDateSQL, new Timestamp(System.currentTimeMillis()).toString(),
                no_o_id, d_id, w_id));
        if (Integer.parseInt(result.substring(14)) <= 0) {
            throw new RuntimeException("OL_O_ID=" + no_o_id + " OL_D_ID="
                    + d_id + " OL_W_ID=" + w_id + " not found!");

        }

        conn.TCL("commit");
    }
}
