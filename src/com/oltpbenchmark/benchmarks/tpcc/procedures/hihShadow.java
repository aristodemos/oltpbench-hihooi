package com.oltpbenchmark.benchmarks.tpcc.procedures;

import com.oltpbenchmark.api.HihActorConnection;
import com.oltpbenchmark.api.HihConnection;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

/**
 * Created by ilvoladore on 24/01/17.
 */
public class hihShadow extends TPCCProcedure {

    public ResultSet run(HihConnection conn, Random gen,
                         int terminalWarehouseID, int numWarehouses,
                         int terminalDistrictLowerID, int terminalDistrictUpperID,
                         TPCCWorker w){
        int orderCarrierID = TPCCUtil.randomNumber(1, 10, gen);
        shadowTransaction(terminalWarehouseID, orderCarrierID, conn);
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

    private int shadowTransaction(int w_id, int o_c_id, HihConnection conn){


        return 0;
    }
}
