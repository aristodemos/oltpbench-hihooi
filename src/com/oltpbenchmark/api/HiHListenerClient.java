package com.oltpbenchmark.api;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

public class HiHListenerClient
{
    //############################################################################################################################
    //############################################################################################################################
    private int listener_port = -1;
    private String listener_ip = "";
    private SocketChannel socketChannel;
    private ByteBuffer buffer;

    private boolean isSessionConnected =  false;
    private String session_id = "";
    private List<String> SQL_CURSOR = new ArrayList<String>();
    private List<String> SQL_CURSOR_METADATA = new ArrayList<String>();
    private int SQL_ROWS=0;

    //private int BUFFER_SIZE=1024*1024;
    //private static int BUFFER_SIZE=4096*4096;
    private int BUFFER_SIZE=4*1024;
    private StringBuffer C_ROWS  = null;
    //############################################################################################################################
    //############################################################################################################################
    public int getCursorSize()
    {
        return SQL_CURSOR.size();
    }
    //############################################################################################################################
    //###########################################################################################################################
    public int getColumnCount()
    {
        return SQL_CURSOR_METADATA.size();
    }
    //----------------------------------------------------------------------------------------------------------------------------
    public HiHListenerClient() {}
    //############################################################################################################################
    //############################################################################################################################
    private String readB(ByteBuffer buffer)
    {
        StringBuffer str = new StringBuffer();
        buffer.flip();
        for (int j=0; j< buffer.limit(); j++)
        {
            byte b = buffer.get(j);
            str.append(new String(new byte[] {b}));
        }
        return str.toString();
    }
    //############################################################################################################################
    //############################################################################################################################
    public void closeChannel()
    {
        try
        {
            this.socketChannel.close();
        }
        catch (IOException e1)
        {
            e1.printStackTrace();
        }
    }
    //############################################################################################################################
    //############################################################################################################################
    public String connect(Properties p)
    {
        String rt_value = "";
        if (! isSessionConnected)
        {
            try
            {
                if (p.getProperty("server")!=null)
                {
                    this.listener_ip = p.getProperty("server");
                }


                if (p.getProperty("port")!=null)
                {
                    this.listener_port = Integer.parseInt(p.getProperty("port"));
                }

                // You need to manage users etc.

                this.socketChannel = SocketChannel.open();
                //this.buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
                this.buffer = ByteBuffer.allocate(BUFFER_SIZE);

                if (this.socketChannel.isOpen())
                {
                    this.socketChannel.configureBlocking(true);
                    this.socketChannel.setOption(StandardSocketOptions.SO_RCVBUF,BUFFER_SIZE);
                    this.socketChannel.setOption(StandardSocketOptions.SO_SNDBUF,BUFFER_SIZE);
                    this.socketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                    this.socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                    //this.socketChannel.setOption(StandardSocketOptions.SO_LINGER, 5);
                    this.socketChannel.connect(new InetSocketAddress(this.listener_ip,this.listener_port));

                    if (this.socketChannel.isConnected())
                    {

                        this.socketChannel.write(ByteBuffer.wrap("Connect".getBytes()));
                        while (socketChannel.read(this.buffer) != -1)
                        {

                            String serverMsg =readB(buffer);
                            //System.out.println("Server Message:"+serverMsg);
                            buffer.clear();
                            if (serverMsg.startsWith("CONNECTED"));
                            {
                                this.session_id=serverMsg.split("::")[1];
                                rt_value = "Session "+this.session_id+" is created.";
                                break;
                            }

                        }
                    }
                    else
                    {
                        rt_value ="Listener cannot accept the connection.";
                    }

                }
                else
                {
                    rt_value="Listener Socket is closed.";
                }
            }
            catch (Exception ex)
            {
                closeChannel();
                ex.printStackTrace();
            }
        }
        else
        {
            rt_value="Session is already connected.";
        }
        return rt_value;
    }
    //############################################################################################################################
    //############################################################################################################################
    public String disconnect()
    {
        String rt_value=  signal_Disconnect();
        try
        {
            if (this.socketChannel.isConnected())
            {
                closeChannel();
                rt_value = "Socket Channel Closed.";
            }
            else
            {
                rt_value ="Session is already disconnected.";
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return rt_value;
    }
    //############################################################################################################################
    //############################################################################################################################

    //############################################################################################################################
    //############################################################################################################################
    public String getColumn(int col_index)
    {
        return this.C_ROWS.toString().split("\\|")[col_index-1].trim();
        //return SQL_CURSOR.get(row_index).split("\\|")[col_index-1];
    }
    //############################################################################################################################
    //############################################################################################################################
    public String getColumn(String column_name)
    {
        int col_index = -1;
        for (int k=1; k<= getColumnCount(); k++)
        {
            if (getColumnName(k).trim().equalsIgnoreCase(column_name)){col_index=k; break;}
        }
        return this.C_ROWS.toString().split("\\|")[col_index-1].trim();
    }
    //############################################################################################################################
    //############################################################################################################################
    public String getColumnName(int index)
    {
        return SQL_CURSOR_METADATA.get(index-1).split("\\,")[0];
    }

    public String getColumnTypeName(int index)
    {
        return SQL_CURSOR_METADATA.get(index-1).split("\\,")[1];
    }

    public int getColumnDisplaySize(int index)
    {
        return Integer.parseInt(SQL_CURSOR_METADATA.get(index-1).split("\\,")[2]);
    }
    //############################################################################################################################
    //############################################################################################################################
    //############################################################################################################################
    //############################################################################################################################
    //############################################################################################################################
    //############################################################################################################################
    public void EXEC_QUERY(String sql)
    {
        cleanSQL();

        int error_code = 0;
        String error_msg = "";
        sql = sql.trim();
        int sqlLength = sql.length();
        sql = "EXEC_QUERY::"+sqlLength+"::"+sql;
        //System.out.println(Thread.currentThread().getName() + ": " + sql);
        try
        {
            if (this.socketChannel.isConnected())
            {

                this.socketChannel.write(ByteBuffer.wrap(sql.getBytes()));
                while (socketChannel.read(this.buffer) != -1)
                {
                    String serverMsg =readB(buffer);
                    try
                    {
                        error_code = Integer.parseInt(serverMsg.split("::")[0]);
                        error_msg = serverMsg.split("::")[1];

                        if (error_code==0)
                        {
                            //System.out.println(error_msg);

                        }
                        else if (error_code==1)
                        {
                            System.out.println(error_msg);
                        }
                        else
                        {
                            System.out.println("Unknown error exit code "+error_code);
                        }
                    }
                    catch(Exception e)
                    {
                        System.out.println(e.getMessage());
                    }

                    buffer.clear();
                    break;
                }
            }
            else
            {
                System.out.println("The connection cannot be established!");
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
            closeChannel();
        }
    }
    //############################################################################################################################
    //############################################################################################################################
    private byte printSQL()
    {
        byte hex_eot = 0;
        StringBuffer str = new StringBuffer();
        buffer.flip();
        for (int j=0; j< buffer.limit(); j++)
        {
            byte b = buffer.get(j);
            if (b==10)
            {
                C_ROWS = new StringBuffer();
                C_ROWS.append(str);
                str.delete(0,str.length());
            }
            else if (b==4)// EOT
            {
                hex_eot=4;
            }
            else
            {
                str.append(new String(new byte[] {b}));
            }
        }
        return hex_eot;
    }
    //############################################################################################################################
    //############################################################################################################################
    public boolean delivery()
    {
        boolean hasNext=true;
        byte b=0;
        try
        {
            String message = "DELIVERY";
            this.socketChannel.write(ByteBuffer.wrap(message.getBytes()));
            while (socketChannel.read(this.buffer) != -1)
            {
                b=printSQL();
                if (b==4)
                {
                    hasNext=false;
                }
                buffer.clear();
                break;
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
            closeChannel();
        }
        return hasNext;
    }
    //############################################################################################################################
    //############################################################################################################################
	/*
	private byte loadFromCursor()
	{
		byte hex_eot = 0;
		this.SQL_CURSOR.clear();

			StringBuffer str = new StringBuffer();
			buffer.flip();
			for (int j=0; j< buffer.limit(); j++)
			{
				byte b = buffer.get(j);
				if (b==10)
				{
				    if (str.toString().length() > 0)
				    {
				    	SQL_CURSOR.add(str.toString());
						str.delete(0,str.length());
				    }
				}
				else if (b==4)// EOT
				{
					hex_eot=4;
				}
				else
				{
					if (b==0)
					{
						str.append(new String("null"));
					}
					else
					{
						str.append(new String(new byte[] {b}));
					}
				}
			}
			return hex_eot;
	}
	*/
    //############################################################################################################################
    //############################################################################################################################
	/*
	public boolean deliveryForCursor()
	{
		boolean hasNext=true;
		byte b=0;
		try
		{
			String message = "DELIVERY";
			this.socketChannel.write(ByteBuffer.wrap(message.getBytes()));
			while (socketChannel.read(this.buffer) != -1)
			{
				b=loadFromCursor();
				if (b==4)
				{
					hasNext=false;
				}
				buffer.clear();
				break;
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			closeChannel();
		}
		return hasNext;
	}
	*/
    //############################################################################################################################
    //############################################################################################################################
    private void cleanSQL()
    {
        this.SQL_ROWS=0;
        this.SQL_CURSOR.clear();
        this.SQL_CURSOR_METADATA.clear();
    }
    //############################################################################################################################
    //############################################################################################################################
    public String updateExtractorTranset(String runtime_id,String extractor_host,String transet_id)
    {
        String rt_value="Transet update failed.";
        String msg = "TRANSETUPDATE::"+runtime_id+"::"+extractor_host+"::"+transet_id;
        try
        {
            if (this.socketChannel.isConnected())
            {
                this.socketChannel.write(ByteBuffer.wrap(msg.getBytes()));
                while (socketChannel.read(this.buffer) != -1)
                {
                    String serverMsg =readB(buffer);
                    //System.out.println("Server Message:"+serverMsg);
                    buffer.clear();
                    rt_value = serverMsg;
                    break;
                }
            }
            else
            {
                rt_value="The connection with Listener:"+this.listener_ip+" cannot be established!";
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return rt_value;
    }
    //############################################################################################################################
    //############################################################################################################################
    public String registerExtractor(String service_name, String extractor_host,int transet_id)
    {
        /// Returns : PostgreSQL:localhost:5432:ext01tpcc:postgres:postgres
        String rt_value="Registration Failed.";
        String msg = "REGISTEREXTRACTOR::"+service_name+"::"+extractor_host+"::"+transet_id;
        try
        {
            if (this.socketChannel.isConnected())
            {
                this.socketChannel.write(ByteBuffer.wrap(msg.getBytes()));
                while (socketChannel.read(this.buffer) != -1)
                {
                    String serverMsg =readB(buffer);
                    //System.out.println("Server Message:"+serverMsg);
                    buffer.clear();
                    rt_value = serverMsg;
                    break;
                }
            }
            else
            {
                rt_value="The connection with Listener:"+this.listener_ip+" cannot be established!";
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return rt_value;
    }

    //############################################################################################################################
    //############################################################################################################################
    public String getMemCachedProperties()
    {
        /// Returns : PostgreSQL:localhost:5432:ext01tpcc:postgres:postgres
        String rt_value="Get Mem Cached Properties Failed.";
        String msg = "GETMEMCACHEDPROP";
        try
        {
            if (this.socketChannel.isConnected())
            {
                this.socketChannel.write(ByteBuffer.wrap(msg.getBytes()));
                while (socketChannel.read(this.buffer) != -1)
                {
                    String serverMsg =readB(buffer);
                    //System.out.println("Server Message:"+serverMsg);
                    buffer.clear();
                    rt_value = serverMsg;
                    break;
                }
            }
            else
            {
                rt_value="The connection with Listener:"+this.listener_ip+" cannot be established!";
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return rt_value;
    }
    //############################################################################################################################
    //############################################################################################################################
    public void fetchRowCount()
    {
        String msg="FETCHROWCOUNT";

        try
        {
            if (this.socketChannel.isConnected())
            {
                this.socketChannel.write(ByteBuffer.wrap(msg.getBytes()));
                while (socketChannel.read(this.buffer) != -1)
                {
                    String serverMsg =readB(buffer);
                    buffer.clear();
                    System.out.println(serverMsg);
                    break;
                }
            }

        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

    }
    //############################################################################################################################
    //############################################################################################################################
    public String executeUpdate(String sql)
    {
        String rt_value="Execute Update Failed.";
        sql = sql.trim();
        sql = "WRITE::"+sql;
        try
        {
            if (this.socketChannel.isConnected())
            {
                this.socketChannel.write(ByteBuffer.wrap(sql.getBytes()));
                while (socketChannel.read(this.buffer) != -1)
                {
                    String serverMsg =readB(buffer);
                    //System.out.println("Server Message:"+serverMsg);
                    buffer.clear();
                    rt_value = serverMsg;
                    break;
                }
            }
            else
            {
                rt_value="The connection with Listener:"+this.listener_ip+" cannot be established!";
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return rt_value.trim();
    }
    //############################################################################################################################
    //############################################################################################################################
    public void getColumnMetadata()
    {
        SQL_CURSOR_METADATA.clear();
        String clientMsg = "COLMETADATA";
        try
        {
            this.socketChannel.write(ByteBuffer.wrap(clientMsg.getBytes()));
            while (socketChannel.read(this.buffer) != -1)
            {
                String serverMsg =readB(buffer);
                //System.out.println("FROM SERVER:["+serverMsg+"]");
                int columnCount=0;
                try
                {
                    columnCount = Integer.parseInt(serverMsg.split("::")[0]);
                }
                catch(Exception e)
                {
                    columnCount=0;
                }

                if (columnCount > 0)
                {
                    String col_metadata = serverMsg.split("::")[1];
                    for (int i=0; i<columnCount; i++)
                    {
                        String t = col_metadata.split("\\|")[i];
                        SQL_CURSOR_METADATA.add(t);
                    }
                }
                buffer.clear();
                break;
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
            //closeChannel();
        }
    }
    //############################################################################################################################
    //############################################################################################################################
    //############################################################################################################################
    //############################################################################################################################
    public int openCursor(String sql)
    {
        return 0;
    }
    ///##########################################################################################################################
    //############################################################################################################################
    //############################################################################################################################
    public String rollbackTransaction()
    {
        String rt_value="Execute Rollback Failed.";
        String tcl = "ROLLBACK";
        try
        {
            if (this.socketChannel.isConnected())
            {
                this.socketChannel.write(ByteBuffer.wrap(tcl.getBytes()));
                while (socketChannel.read(this.buffer) != -1)
                {
                    String serverMsg =readB(buffer);
                    //System.out.println("Server Message:"+serverMsg);
                    buffer.clear();
                    rt_value = serverMsg;
                    break;
                }
            }
            else
            {
                rt_value="The connection with Listener:"+this.listener_ip+" cannot be established!";
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return rt_value;
    }
    //############################################################################################################################
    //############################################################################################################################
    public String commitTransaction()
    {
        String rt_value="Execute Commit Failed.";
        String tcl = "COMMIT";
        try
        {
            if (this.socketChannel.isConnected())
            {
                this.socketChannel.write(ByteBuffer.wrap(tcl.getBytes()));
                while (socketChannel.read(this.buffer) != -1)
                {
                    String serverMsg =readB(buffer);
                    //System.out.println("Server Message:"+serverMsg);
                    buffer.clear();
                    rt_value = serverMsg;
                    break;
                }
            }
            else
            {
                rt_value="The connection with Listener:"+this.listener_ip+" cannot be established!";
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return rt_value;
    }
    //############################################################################################################################
    //############################################################################################################################
    public String startTransaction()
    {
        String rt_value="Transaction Failed to Start.";
        String tcl = "STARTTX";
        try
        {
            if (this.socketChannel.isConnected())
            {
                this.socketChannel.write(ByteBuffer.wrap(tcl.getBytes()));
                while (socketChannel.read(this.buffer) != -1)
                {
                    String serverMsg =readB(buffer);
                    //System.out.println("Server Message:"+serverMsg);
                    buffer.clear();
                    rt_value = serverMsg;
                    break;
                }
            }
            else
            {
                rt_value="The connection with Listener:"+this.listener_ip+" cannot be established!";
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return rt_value;
    }
    //############################################################################################################################
    //############################################################################################################################
    public String set(String input)
    {
        //System.out.println("Set Input:"+input);
        String rt_value="Set command failed.";
        String tcl = "SET::"+input;
        try
        {
            if (this.socketChannel.isConnected())
            {
                this.socketChannel.write(ByteBuffer.wrap(tcl.getBytes()));
                while (socketChannel.read(this.buffer) != -1)
                {
                    String serverMsg =readB(buffer);
                    //System.out.println("Server Message:"+serverMsg);
                    buffer.clear();
                    rt_value = serverMsg;
                    break;
                }
            }
            else
            {
                rt_value="The connection with Listener:"+this.listener_ip+" cannot be established!";
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return rt_value;
    }
    //############################################################################################################################
    //############################################################################################################################
    public List<Map<String, Object>> executeQuery(String sql)
    {
        return null;
    }
    //############################################################################################################################
    //############################################################################################################################
    public String fastload(Vector<String> SQLSTMT, Properties prop)
    {
        return "Obsolete Method.";
    }
    //############################################################################################################################
    //############################################################################################################################
    public String printConnectorsTranset()
    {
        String rt_value="Execute printConnectorsTranset Failed.";
        String tcl = "printConnectorsTranset";
        try
        {
            if (this.socketChannel.isConnected())
            {
                this.socketChannel.write(ByteBuffer.wrap(tcl.getBytes()));
                while (socketChannel.read(this.buffer) != -1)
                {
                    String serverMsg =readB(buffer);
                    System.out.println(serverMsg);
                    buffer.clear();
                    rt_value = serverMsg;
                    break;
                }
            }
            else
            {
                rt_value="The connection with Listener:"+this.listener_ip+" cannot be established!";
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return rt_value;
    }
    //############################################################################################################################
    //############################################################################################################################
    private String signal_Disconnect()
    {
        String rt_value="Disconnect Failed";
        String tcl = "Disconnect";
        try
        {
            if (this.socketChannel.isConnected())
            {
                this.socketChannel.write(ByteBuffer.wrap(tcl.getBytes()));
                while (socketChannel.read(this.buffer) != -1)
                {
                    String serverMsg =readB(buffer);
                    System.out.println(serverMsg);
                    buffer.clear();
                    rt_value = serverMsg;
                    break;
                }
            }
            else
            {
                rt_value="The connection with Listener:"+this.listener_ip+" cannot be established!";
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return rt_value;
    }
    //############################################################################################################################
    //############################################################################################################################
    private String signal_shutdown()
    {
        String rt_value="Shutdown Failed";
        String tcl = "shutdown-force";
        try
        {
            if (this.socketChannel.isConnected())
            {
                this.socketChannel.write(ByteBuffer.wrap(tcl.getBytes()));
                while (socketChannel.read(this.buffer) != -1)
                {
                    String serverMsg =readB(buffer);
                    System.out.println(serverMsg);
                    buffer.clear();
                    rt_value = serverMsg;
                    break;
                }
            }
            else
            {
                rt_value="The connection with Listener:"+this.listener_ip+" cannot be established!";
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return rt_value;
    }
    //############################################################################################################################
    //############################################################################################################################
    public String shutdown(Properties p)
    {
        System.out.print(signal_shutdown());
        disconnect();
        //connect(p);
        return "System Shutdown";
    }
    //############################################################################################################################
    //############################################################################################################################
    public static void main( String[] args )
    {
        Properties p = new Properties();
        p.setProperty("server", "localhost");
        p.setProperty("port","7788");
        HiHListenerClient hih = new HiHListenerClient();
        System.out.println(hih.connect(p));




        List<String> readStatements = new ArrayList<>();
        readStatements.add("select * from oorder limit 2");
        readStatements.add("select * from warehouse");
        readStatements.add("SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = 10 AND NO_W_ID = 2 ORDER BY NO_O_ID ASC LIMIT 1;");
        readStatements.add("SELECT D_NEXT_O_ID, D_TAX FROM DISTRICT WHERE D_W_ID = 1 AND D_ID = 1 ");
        readStatements.add("SELECT I_PRICE, I_NAME , I_DATA FROM ITEM  WHERE I_ID = 20;");
        readStatements.add("SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10 FROM STOCK WHERE S_I_ID = 20 AND S_W_ID = 1");

        long startTimeA = System.currentTimeMillis();
        //main loop:
        for (int i=0;i<readStatements.size();i++){
            hih.EXEC_QUERY(readStatements.get(i));
            hih.getColumnMetadata();
            while (hih.delivery()){
                for (int q=1; q<=hih.getColumnCount(); q++)
                {
                    System.out.print(hih.getColumnName(q)+" | "+hih.getColumn(q));
                }
                System.out.println();
            }
        }

        long endTimeA = System.currentTimeMillis();
        System.out.println("TEST 2 : " + (endTimeA-startTimeA) + "msec");
        //System.out.println(hih.shutdown(p));

        //System.out.println(hih.disconnect());
		/*
		hih.EXEC_QUERY("Select * from hih_users");
		//hih.EXEC_QUERY("Select * from customer");
		hih.getColumnMetadata();
		for (int q=1; q<=hih.getColumnCount(); q++)
		  {
		       System.out.print(hih.getColumnName(q)+" | ");
		  }
		  System.out.println();


		while(hih.delivery())
		{
			//System.out.print("*** "+hih.getColumn("employee_id"));
			//System.out.print(hih.getColumn("first_name"));
			//System.out.println();

			for (int k=1; k<= hih.getColumnCount(); k++)
            {
                System.out.print(hih.getColumn(k)+" | ");
            }

			System.out.println();

		}

		hih.fetchRowCount();
		*/


		/*
		hih.EXEC_QUERY("Select * from employees where employee_id <=10");
		hih.getColumnMetadata();
		while(hih.deliveryForCursor())
		{
			   for(int j=0; j<hih.getCursorSize(); j++)
		       {
		              for (int k=1; k<=hih.getColumnCount(); k++)
		              {
		                  System.out.print(hih.getColumn(j,k)+" | ");
		              }
		              System.out.println();
		       }
		}
		hih.fetchRowCount();
		*/
        //System.out.println(hih.registerExtractor("TESTSRV","hicl01",12345));
        //System.out.println(hih.getMemCachedProperties());

    }


}
