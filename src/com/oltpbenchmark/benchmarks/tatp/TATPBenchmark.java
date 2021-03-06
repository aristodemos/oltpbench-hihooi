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


package com.oltpbenchmark.benchmarks.tatp;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tatp.procedures.DeleteCallForwarding;

public class TATPBenchmark extends BenchmarkModule {

	public TATPBenchmark(WorkloadConfiguration workConf) {
		super("tatp", workConf, true);
	}
	
	@Override
	protected Package getProcedurePackageImpl() {
		return (DeleteCallForwarding.class.getPackage());
	}

	@Override
	protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
		List<Worker> workers = new ArrayList<Worker>();
		for (int i = 0; i < workConf.getTerminals(); ++i) {
			workers.add(new TATPWorker(i, this));
		} // FOR
		return (workers);
	}
	
	@Override
	protected Loader makeLoaderImpl(Connection conn) throws SQLException {
		return (new TATPLoader(this, conn));
	}
}
