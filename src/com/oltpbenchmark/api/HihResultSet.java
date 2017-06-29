package com.oltpbenchmark.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ilvoladore on 07/07/2016.
 */
public class HihResultSet {

    public int n_rows = 0;
    public int n_cols = 0;
    List<String> column_names ;
    List<String> rows;

    public List<Map<String, String>> data;

    HihResultSet(List<String> col_names, List<String> rows){
        this.n_rows=rows.size();
        this.n_cols=col_names.size();
        this.column_names = col_names;
        this.rows=rows;
        this.data=ResultsToListofMaps();
    }

    public List<Map<String, String>> ResultsToListofMaps(){
        List<Map<String, String>> retval = new ArrayList<>();
        Map<String, String> keys = new HashMap<>();

        for (String r : rows){
            for (int i=0; i<column_names.size(); i++){
                keys.put(column_names.get(i), r.split("\\|")[i]);
            }
            retval.add(new HashMap<>(keys));
        }
        return retval;
    }
}
