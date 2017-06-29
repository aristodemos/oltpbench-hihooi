package com.oltpbenchmark.api;


import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by ilvoladore on 09/06/2016.
 */
public class HihConnection {


    public static PrintWriter logWriter = null;

    private static boolean  DEBUG   = false; //print transactions to file and other msgs on system.out
    private static boolean  D_DEBUG = false;
    public HiHListenerClient hih;

    private static Properties p = new Properties();

    //mafia
    private Map<String, String> emptyMap = new HashMap<String, String>();

    public HihConnection(){
        this.hih = new HiHListenerClient();
        if (DEBUG || D_DEBUG){
            String timeStamp = new SimpleDateFormat("yy.MM.dd.HH.mm.ss").format(new java.util.Date());
            try{
                logWriter = new PrintWriter("LOG_"+timeStamp+".txt", "UTF-8");
            }catch (FileNotFoundException |UnsupportedEncodingException err){
                System.out.println("FileNOTfound" + err.getMessage());
            }
        }
    }

    public static Random testRndGen = new Random();

    //On DICL Cluster use:
    //private String LISTENER="dicl09";
    private String LISTENER="prm1";
    ///TO DEBUG through IntelliJ  USE THIS in terminal (ssh tunnel):
    //ssh -L 7777:dicl09.cut.ac.cy:7777 hihooi@dicl09.cut.ac.cy
    //and set LISTENER to localhost:
    //private String LISTENER="localhost";





    public String CONNECT() {
        p.setProperty("server",this.LISTENER);
        p.setProperty("port","7788");
        //p.setProperty("port","7777");
        p.setProperty("username","user01");
        p.setProperty("password","12345678");
        p.setProperty("identifier","client01");
        p.setProperty("service_name","TESTSRV");
        return hih.connect(p);   /// Retun SESSION-ID e.g  21703567-1ed7-4f59-aeac-39686ea9c2b1
    }

    public String shutdown() {
        return hih.shutdown(p);
        //return null;
    }

    public String resetConnection(){
        this.DISCONNECT();
        return this.CONNECT();
    }

    public String DISCONNECT() {
        if(DEBUG)logWriter.close();
        //hih.disconnect();
        //return hih.shutdown(p);
        return hih.disconnect();
    }


    public String setConsistency(int setCMD) {
		/*
		hih.set(“set consistency level 1”)etc.
			set consistency level 2;
			set consistency level 3;
			set consistency level 4;
		*/
        return hih.set("set consistency level " + setCMD);
    }

    public String DML(String sql) {
        //System.out.println(Thread.currentThread().getName() +":  "+ sql);
        return hih.executeUpdate(sql).trim();
    }

    public String START_TX() {
        //System.out.println(Thread.currentThread().getName() + ": start_tx");
        return hih.startTransaction();
    }

    //TRANSACTION CONTROL LANGUAGE: COMMIT, ROLLBACK;
    public String TCL(String tcl_cmd) {
        if (tcl_cmd.equalsIgnoreCase("commit")) {
            //System.out.println(Thread.currentThread().getName() + ": commit");
            return hih.commitTransaction();
        }
        else {
            //System.out.println(Thread.currentThread().getName() + ": rollback");
            return hih.rollbackTransaction();
        }
    }
}
