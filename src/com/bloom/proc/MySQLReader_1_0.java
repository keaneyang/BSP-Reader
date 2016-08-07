package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.event.Event;
import com.bloom.intf.SourceMetadataProvider;
import com.bloom.proc.events.WAEvent;
import com.bloom.recovery.MySQLSourcePosition;
import com.bloom.recovery.SourcePosition;
import com.bloom.runtime.compiler.TypeDefOrName;
import com.bloom.runtime.components.Flow;
import com.bloom.security.Password;
import com.bloom.source.cdc.common.TableMD;
import com.bloom.source.lib.prop.Property;
import com.bloom.source.mysql.MysqlProperty;
import com.bloom.uuid.UUID;
import java.net.URI;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.regex.PatternSyntaxException;
import org.apache.log4j.Logger;

@PropertyTemplate(name="MysqlReader", type=AdapterType.source, properties={@com.bloom.anno.PropertyTemplateProperty(name="ConnectionURL", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Username", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Password", type=Password.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Database", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Tables", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="ExcludedTables", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="FilterTransactionBoundaries", type=Boolean.class, required=false, defaultValue="true"), @com.bloom.anno.PropertyTemplateProperty(name="SendBeforeImage", type=Boolean.class, required=false, defaultValue="true")}, inputType=WAEvent.class)
public class MySQLReader_1_0
  extends SourceProcess
  implements SourceMetadataProvider
{
  private MysqlReplicationClient waClient;
  private Connection connection = null;
  private List<String> tables = null;
  private String tablesValue = null;
  private String excludedTablesValue = null;
  private String database = null;
  private Map<String, TypeDefOrName> tableMetadata;
  private HashMap<String, ArrayList<Integer>> tableKeyColumns = new HashMap();
  private boolean sendBeforeImage;
  private boolean filterTransactionBoundaries;
  private String username = null;
  private String password = null;
  private String hostname = null;
  private int portno;
  private int server_id_bits = 32;
  private String connectionURL;
  private String JDBCConnectionURL;
  private int lower_case_table_names;
  private static Logger logger = Logger.getLogger(MySQLReader_1_0.class);
  
  public MySQLReader_1_0() {}
  
  public MySQLReader_1_0(Connection conn)
  {
    this.connection = conn;
  }
  
  public void init(Map<String, Object> properties, Map<String, Object> properties2, UUID sourceUUID, String distributionID, SourcePosition restartPosition, boolean sendPositions, Flow flow)
    throws Exception
  {
    Map<String, Object> localProp = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    this.sendBeforeImage = true;
    
    localProp.putAll(properties);
    localProp.putAll(properties2);
    localProp.put(BaseReader.SOURCE_PROCESS, this);
    localProp.put(Property.SOURCE_UUID, sourceUUID);
    localProp.put("distributionId", distributionID);
    localProp.put("sendPosition", Boolean.valueOf(sendPositions));
    if (localProp.get("Password") != null)
    {
      String pp = ((Password)localProp.get("Password")).getPlain();
      
      localProp.put("Password", pp);
    }
    MysqlProperty prop = new MysqlProperty(localProp);
    if ((sendPositions) && (restartPosition != null))
    {
      String name = ((MySQLSourcePosition)restartPosition).getBinLogName();
      Long position = Long.valueOf(((MySQLSourcePosition)restartPosition).getRestartPosition());
      prop.setBinlogFileName(name);
      prop.setBinLogPosition(position);
      prop.setPreviousBinlogFileName(((MySQLSourcePosition)restartPosition).getPreviousBinLogName());
    }
    getAndCheckProperties(prop.getMap());
    try
    {
      openJDBCConnection();
      checkMySQLConfig();
      checkMySQLPrivileges();
      buildTableList();
      this.tableMetadata = TableMD.getTablesMetadataFromJDBCConnection(this.tables, this.connection, this.tableKeyColumns);
      if (this.connection != null) {
        try
        {
          this.connection.close();
        }
        catch (Exception e) {}
      }
      super.init(properties, properties2, sourceUUID, distributionID);
    }
    finally
    {
      if (this.connection != null) {
        try
        {
          this.connection.close();
        }
        catch (Exception e) {}
      }
    }
    String session = (String)localProp.get("SessionType");
    if ((session == null) || (session != "METADATA"))
    {
      this.waClient = new MysqlReplicationClient(prop, this.hostname, this.portno, this.username, this.password);
      this.waClient.setupTablenameHash(this.tables, this.tableKeyColumns);
      this.waClient.setSendBeforeImage(this.sendBeforeImage);
      this.waClient.setFilterTransactionBoundaries(this.filterTransactionBoundaries);
      this.waClient.setServer_ID_Bits(this.server_id_bits);
      this.waClient.init();
    }
  }
  
  private void getAndCheckProperties(Map<String, Object> properties)
    throws Exception
  {
    if ((!properties.containsKey("ConnectionURL")) || (properties.get("ConnectionURL") == null))
    {
      String errMsg = " ConnectionURL is missing";
      logger.error(errMsg);
      throw new Exception(errMsg);
    }
    this.connectionURL = ((String)properties.get("ConnectionURL"));
    if (this.connectionURL.trim().isEmpty())
    {
      String errMsg = " ConnectionURL is empty";
      logger.error(errMsg);
      throw new Exception(errMsg);
    }
    String[] parts = this.connectionURL.split(":", 3);
    if ((parts.length > 1) && (parts[0].toLowerCase().equals("mysql")))
    {
      URI uri = new URI(this.connectionURL);
      String scheme = uri.getScheme().toLowerCase();
      if ((scheme == null) || (!scheme.equals("mysql")))
      {
        String errMsg = " ConnectionURL is not a MySQL URL: " + this.connectionURL;
        logger.error(errMsg);
        throw new Exception(errMsg);
      }
      this.hostname = uri.getHost();
      if ((this.hostname == null) || (this.hostname.isEmpty()))
      {
        String errMsg = " Hostname in ConnectionURL is missing or malformed";
        logger.error(errMsg);
        throw new Exception(errMsg);
      }
      this.portno = uri.getPort();
      if (this.portno == -1) {
        this.portno = 3306;
      }
      String path = uri.getPath();
      if ((!path.isEmpty()) && (!path.equals("/"))) {
        this.database = path.substring(1);
      }
    }
    else
    {
      if (parts.length > 0)
      {
        this.hostname = parts[0].trim();
        if (this.hostname.isEmpty())
        {
          String errMsg = "The host name in ConnectionURL is empty: " + this.connectionURL;
          logger.error(errMsg);
          throw new Exception(errMsg);
        }
      }
      this.portno = 3306;
      if ((parts.length > 1) && 
        (!parts[1].trim().isEmpty())) {
        try
        {
          this.portno = Integer.parseInt(parts[1].trim());
        }
        catch (Exception NumberFormatException)
        {
          String errMsg = " Invalid port number: " + parts[1];
          logger.error(errMsg);
          throw new Exception(errMsg);
        }
      }
      if ((parts.length > 2) && 
        (!parts[2].trim().isEmpty())) {
        this.database = parts[2].trim();
      }
    }
    if ((properties.containsKey("Database")) && (properties.get("Database") != null))
    {
      this.database = ((String)properties.get("Database"));
      this.database = this.database.trim();
      if (this.database.isEmpty())
      {
        String errMsg = " Database property is empty";
        logger.error(errMsg);
        throw new Exception(errMsg);
      }
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Initialized MySQLReader with Database: " + this.database);
    }
    this.JDBCConnectionURL = ("jdbc:mysql://" + this.hostname);
    if (this.portno != -1) {
      this.JDBCConnectionURL = (this.JDBCConnectionURL + ":" + this.portno);
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Initialized MySQLReader with url: " + this.JDBCConnectionURL);
    }
    try
    {
      if ((properties.containsKey("Username")) && (properties.get("Username") != null) && (((String)properties.get("Username")).length() > 0))
      {
        this.username = ((String)properties.get("Username"));
      }
      else
      {
        logger.error("Username is not specified");
        throw new Exception("Username is not specified");
      }
    }
    catch (ClassCastException e)
    {
      logger.error("Invalid Username format.Value specified cannot be cast to java.lang.String");
      
      throw new Exception("Invalid Username format.Value specified cannot be cast to java.lang.String");
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Initialized MySQLReader with username: " + this.username);
    }
    try
    {
      if ((properties.containsKey("Password")) && (properties.get("Password") != null) && (((String)properties.get("Password")).length() > 0))
      {
        this.password = ((String)properties.get("Password"));
      }
      else
      {
        logger.error("Password is not specified");
        throw new Exception("Password is not specified");
      }
    }
    catch (ClassCastException e)
    {
      logger.error("Invalid Password format.Value specified cannot be cast to java.lang.String");
      
      throw new Exception("Invalid Password format.Value specified cannot be cast to java.lang.String");
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Initialized MySQLReader with password: " + this.password);
    }
    try
    {
      if ((properties.containsKey("Tables")) && (properties.get("Tables") != null) && (((String)properties.get("Tables")).length() > 0)) {
        this.tablesValue = ((String)properties.get("Tables"));
      } else if ((properties.containsKey("Table")) && (properties.get("Table") != null) && (((String)properties.get("Table")).length() > 0)) {
        this.tablesValue = ((String)properties.get("Table"));
      } else {
        throw new IllegalArgumentException("Expected required parameter 'Tables' not found");
      }
    }
    catch (ClassCastException e)
    {
      logger.error("Invalid Table format.Value specified cannot be cast to java.lang.String");
      
      throw new Exception("Invalid Table format.Value specified cannot be cast to java.lang.String");
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Initialized MySQLReader with Tables:" + this.tablesValue);
    }
    try
    {
      if ((properties.containsKey("ExcludedTables")) && (properties.get("ExcludedTables") != null) && (((String)properties.get("ExcludedTables")).length() > 0)) {
        this.excludedTablesValue = properties.get("ExcludedTables").toString();
      }
    }
    catch (Exception e)
    {
      String errMsg = "Invalid value in ExcludedTables property: " + e.getMessage();
      logger.error(errMsg);
      throw new IllegalArgumentException(errMsg);
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Initialized MySQLReader with ExcludedTables:" + this.excludedTablesValue);
    }
    try
    {
      if ((properties.containsKey("SendBeforeImage")) && (properties.get("SendBeforeImage") != null)) {
        this.sendBeforeImage = ((Boolean)properties.get("SendBeforeImage")).booleanValue();
      }
    }
    catch (Exception e)
    {
      String errMsg = "Problem getting value of SendBeforeImage";
      logger.error(errMsg);
      throw new IllegalArgumentException(errMsg);
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Initialized MySQLReader with SendBeforeImage = " + this.sendBeforeImage);
    }
    try
    {
      if ((properties.containsKey("FilterTransactionBoundaries")) && (properties.get("FilterTransactionBoundaries") != null)) {
        this.filterTransactionBoundaries = ((Boolean)properties.get("FilterTransactionBoundaries")).booleanValue();
      }
    }
    catch (Exception e)
    {
      String errMsg = "Problem getting value of FilterTransactionBoundaries";
      logger.error(errMsg);
      throw new IllegalArgumentException(errMsg);
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Initialized MySQLReader with FilterTransactionBoundaries = " + this.filterTransactionBoundaries);
    }
  }
  
  private synchronized void openJDBCConnection()
    throws Exception
  {
    if (logger.isDebugEnabled()) {
      logger.debug("Load the JDBC driver for MySQL");
    }
    try
    {
      Class.forName("com.mysql.jdbc.Driver");
    }
    catch (ClassNotFoundException e)
    {
      String errMsg = " MySQL JDBC driver class was not found: " + e.getMessage();
      logger.error(errMsg);
      throw e;
    }
    catch (Exception e)
    {
      String errMsg = " Problem loading JDBC driver: " + e.getMessage();
      logger.error(errMsg, e);
      throw e;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Retrieving from url '" + this.JDBCConnectionURL + "'");
    }
    try
    {
      this.connection = DriverManager.getConnection(this.JDBCConnectionURL, this.username, this.password);
    }
    catch (SQLException e)
    {
      String errorString = " MySQLReader failed to get JDBC connection to database with url : " + this.JDBCConnectionURL + " username : " + this.username + " \n ErrorCode : " + e.getErrorCode() + ";" + "SQLCode : " + e.getSQLState() + ";" + "SQL Message : " + e.getMessage();
      
      Exception exception = new Exception(errorString);
      
      logger.error(errorString, e);
      throw exception;
    }
    catch (Exception e)
    {
      String errorString = " MySQLReader failed to get JDBC connection to database with url : " + this.JDBCConnectionURL + " username : " + this.username + " \n Cause : " + e.getCause() + ";" + "Message : " + e.getMessage();
      
      Exception exception = new Exception(errorString);
      
      logger.error(errorString, e);
      throw exception;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Connected to Database with url:" + this.JDBCConnectionURL);
    }
  }
  
  public boolean mysqlVersionLess(String mysqlVersion, int[] givenVersion)
    throws Exception
  {
    int[] vnum = { 0, 0, 0 };
    String[] vers;
    try
    {
      vers = mysqlVersion.split("[.-]");
    }
    catch (PatternSyntaxException e)
    {
      String errmsg = "Internal error in MySQLReader: " + e;
      logger.error(errmsg, e);
      throw new Exception(errmsg);
    }
    try
    {
      if (vers.length > 0) {
        vnum[0] = Integer.parseInt(vers[0]);
      }
      if (vers.length > 1) {
        vnum[1] = Integer.parseInt(vers[1]);
      }
      if (vers.length > 2) {
        vnum[2] = Integer.parseInt(vers[2]);
      }
    }
    catch (NumberFormatException e)
    {
      String errmsg = "Unexpected version ID from MySQL: " + mysqlVersion;
      logger.error(errmsg);
      throw new Exception(errmsg);
    }
    for (int i = 0; i < 3; i++) {
      if (vnum[i] != givenVersion[i])
      {
        if (vnum[i] < givenVersion[i]) {
          return true;
        }
        return false;
      }
    }
    return false;
  }
  
  public void checkMySQLConfig()
    throws Exception
  {
    String msg = "";
    String msg2 = "";
    boolean log_bin_found = false;
    boolean server_id_found = false;
    boolean binlog_format_found = false;
    boolean binlog_row_image_found = false;
    Statement s = null;
    ResultSet rs = null;
    try
    {
      this.lower_case_table_names = -1;
      
      s = this.connection.createStatement();
      rs = s.executeQuery("show global variables where variable_name in ('server_id','log_bin','binlog_format','binlog_row_image','lower_case_table_names','server_id_bits','version')");
      while (rs.next())
      {
        String vname = rs.getString(1);
        String vvalue = rs.getString(2);
        switch (vname)
        {
        case "log_bin": 
          if (!vvalue.trim().equalsIgnoreCase("ON"))
          {
            msg = msg + "\nBinary logging is not enabled.";
            msg2 = msg2 + "\nAdd --log_bin to the mysqld command line or add log_bin to your my.cnf file.";
          }
          log_bin_found = true;
          break;
        case "server_id": 
          if (vvalue.trim().equals("0"))
          {
            msg = msg + "\nThe server ID must be specified.";
            msg2 = msg2 + "\nAdd --server-id=n where n is a positive number to the mysqld command line " + "or add server-id=n to your my.cnf file.";
          }
          server_id_found = true;
          break;
        case "binlog_format": 
          if (!vvalue.trim().equalsIgnoreCase("ROW"))
          {
            msg = msg + "\nRow logging must be specified.";
            msg2 = msg2 + "\nAdd --binlog-format=ROW to the mysqld command line " + "or add binlog-format=ROW to your my.cnf file.";
          }
          binlog_format_found = true;
          break;
        case "binlog_row_image": 
          if (!vvalue.trim().equalsIgnoreCase("FULL"))
          {
            msg = msg + "\nThe full row must be logged.";
            msg2 = msg2 + "\nAdd --binlog_row_image=FULL to the mysqld command line " + "or add binlog_row_image=FULL to your my.cnf file.";
          }
          binlog_row_image_found = true;
          break;
        case "lower_case_table_names": 
          switch (vvalue.trim())
          {
          case "0": 
            this.lower_case_table_names = 0;
            break;
          case "1": 
            this.lower_case_table_names = 1;
            break;
          case "2": 
            this.lower_case_table_names = 2;
            break;
          default: 
            this.lower_case_table_names = 0;
          }
          break;
        case "server_id_bits": 
          this.server_id_bits = 32;
          try
          {
            this.server_id_bits = Integer.parseInt(vvalue.trim());
          }
          catch (Exception e)
          {
            String warnMsg = "Trouble interpreting the value of server_id_bits: " + vvalue + "  " + e;
            
            logger.warn(warnMsg, e);
          }
        case "version": 
          if (mysqlVersionLess(vvalue, new int[] { 5, 6, 2 })) {
            binlog_row_image_found = true;
          }
          break;
        default: 
          logger.warn("Unexpected name in SHOW VARIABLES result: " + vname);
        }
      }
      if (!log_bin_found) {
        msg = msg + "\nDid not get a value for log_bin";
      }
      if (!server_id_found) {
        msg = msg + "\nDid not get a value for server_id";
      }
      if (!binlog_format_found) {
        msg = msg + "\nDid not get a value for binlog_format";
      }
      if (!binlog_row_image_found) {
        msg = msg + "\nDid not get a value for binlog_row_image";
      }
      if (rs != null) {
        try
        {
          rs.close();
        }
        catch (Exception e) {}
      }
      if (s != null) {
        try
        {
          s.close();
        }
        catch (Exception e) {}
      }
      String errmsg;
      if (msg.length() <= 1) {
        return;
      }
    }
    catch (Exception e)
    {
      errmsg = "Exception while checking MySQL configuration settings: " + e;
      logger.error(errmsg, e);
      throw new Exception(errmsg);
    }
    finally
    {
      if (rs != null) {
        try
        {
          rs.close();
        }
        catch (Exception e) {}
      }
      if (s != null) {
        try
        {
          s.close();
        }
        catch (Exception e) {}
      }
    }
    String errmsg = "Problem with the configuration of MySQL: " + msg + msg2;
    logger.error(errmsg);
    throw new Exception(errmsg);
  }
  
  private String addQuotes(String val)
    throws Exception
  {
    String res = null;
    if (val.indexOf("'") == -1) {
      res = "'" + val + "'";
    }
    if (val.indexOf("\"") == -1) {
      res = "\"" + val + "\"";
    }
    if (val.indexOf("`") == -1) {
      res = "`" + val + "`";
    }
    if (res == null)
    {
      String errmsg = "In MySQLReader, part of the userid could not be quoted: " + val;
      logger.error(errmsg);
      throw new Exception(errmsg);
    }
    return res;
  }
  
  public void checkMySQLPrivileges()
    throws Exception
  {
    boolean priv_select = false;
    boolean priv_repl_slave = false;
    boolean priv_repl_client = false;
    Statement s = null;
    ResultSet rs = null;
    String current_user;
    try
    {
      s = this.connection.createStatement();
      rs = s.executeQuery("select current_user()");
      if (!rs.first())
      {
        String errmsg = "No result returned for current_user()";
        logger.error(errmsg);
        throw new Exception(errmsg);
      }
      current_user = rs.getString(1);
      String[] parts;
      try
      {
        parts = current_user.split("@");
      }
      catch (PatternSyntaxException e)
      {
        String errmsg = "MySQLReader internal error- " + e;
        logger.error(errmsg, e);
        throw new Exception(errmsg);
      }
      if (parts.length != 2)
      {
        String errmsg = "The MySQL user name is not as expected: " + current_user;
        logger.error(errmsg);
        throw new Exception(errmsg);
      }
      String safe_current_user = addQuotes(parts[0]) + "@" + addQuotes(parts[1]);
      
      rs = s.executeQuery("show grants for " + safe_current_user);
      while (rs.next())
      {
        String grantsLine = rs.getString(1);
        int i = grantsLine.indexOf(" ON ");
        if (i > -1)
        {
          String[] privs = grantsLine.substring(6, i).split(",");
          for (String priv : privs)
          {
            priv = priv.trim();
            if (priv.equalsIgnoreCase("all privileges"))
            {
              priv_select = true;
              priv_repl_slave = true;
              priv_repl_client = true;
            }
            if (priv.equalsIgnoreCase("select")) {
              priv_select = true;
            }
            if (priv.equalsIgnoreCase("replication slave")) {
              priv_repl_slave = true;
            }
            if (priv.equalsIgnoreCase("replication client")) {
              priv_repl_client = true;
            }
          }
        }
      }
      if (rs != null) {
        try
        {
          rs.close();
        }
        catch (Exception e) {}
      }
      if (s != null) {
        try
        {
          s.close();
        }
        catch (Exception e) {}
      }
      String errmsg;
      if (!priv_select) {
        break label528;
      }
    }
    catch (Exception e)
    {
      errmsg = "Exception while determining the user's privileges in MySQL" + e;
      logger.error(errmsg, e);
      throw new Exception(errmsg);
    }
    finally
    {
      if (rs != null) {
        try
        {
          rs.close();
        }
        catch (Exception e) {}
      }
      if (s != null) {
        try
        {
          s.close();
        }
        catch (Exception e) {}
      }
    }
    if ((!priv_repl_slave) || (!priv_repl_client))
    {
      label528:
      String errmsg = "User " + current_user + " does not have the MySQL privileges needed to run MySQLReader.  " + "To correct this, run the following command in MySQL: grant ";
      if (!priv_select) {
        errmsg = errmsg + "select, ";
      }
      if (!priv_repl_slave) {
        errmsg = errmsg + "replication slave, ";
      }
      if (!priv_repl_client) {
        errmsg = errmsg + "replication client, ";
      }
      errmsg = errmsg.substring(0, errmsg.length() - 2);
      errmsg = errmsg + " on *.* to " + current_user + ";";
      if (!priv_select) {
        errmsg = errmsg + "\n For the select privilege, you can grant it only on the tables you want to " + "collect change data from, rather than on *.*.";
      }
      logger.error(errmsg);
      throw new Exception(errmsg);
    }
  }
  
  private synchronized void buildTableList()
    throws Exception
  {
    DatabaseMetaData md = null;
    HashMap excludedTablenameHash = new HashMap();
    ResultSet excludedTableResultSet = null;
    ResultSet tableResultSet = null;
    try
    {
      String[] excludedArray = new String[0];
      if (this.excludedTablesValue != null) {
        excludedArray = this.excludedTablesValue.split(";");
      }
      for (String excld : excludedArray)
      {
        String excldName = excld.trim();
        String catalog = this.database;
        String schema = null;
        String excluded = null;
        if (excldName.contains("."))
        {
          StringTokenizer tokenizer = new StringTokenizer(excldName, ".");
          if (tokenizer.countTokens() != 2) {
            throw new IllegalArgumentException("Illegal argument found in ExcludedTables property. Expected argument should contain at most 2 dot separated strings. Found '" + excld + "'");
          }
          catalog = tokenizer.nextToken();
          excluded = tokenizer.nextToken();
        }
        else
        {
          if (this.database == null) {
            throw new IllegalArgumentException("Illegal argument found in ExcludedTables property. Unqualified table name given, but no Database property was included: '" + excld + "'");
          }
          excluded = excldName;
        }
        if (logger.isDebugEnabled()) {
          logger.debug("Trying to fetch excluded table metadata for catalog '" + catalog + "' and schema '" + schema + "' and table pattern '" + excluded + "'");
        }
        if (md == null) {
          md = this.connection.getMetaData();
        }
        excludedTableResultSet = md.getTables(catalog, schema, excluded, new String[] { "TABLE" });
        while (excludedTableResultSet.next())
        {
          String p1 = excludedTableResultSet.getString(1);
          String p3 = excludedTableResultSet.getString(3);
          String fullName = p1 + "." + p3;
          excludedTablenameHash.put(fullName, " ");
        }
      }
      if (excludedTableResultSet != null) {
        try
        {
          excludedTableResultSet.close();
        }
        catch (Exception e) {}
      }
      String errorString;
      this.tables = new ArrayList();
    }
    catch (Exception e)
    {
      errorString = " Failure in fetching excluded tables metadata from Database \n Cause : " + e.getCause() + ";" + "Message : " + e.getMessage();
      
      logger.error(errorString, e);
      throw new Exception(errorString);
    }
    finally
    {
      if (excludedTableResultSet != null) {
        try
        {
          excludedTableResultSet.close();
        }
        catch (Exception e) {}
      }
    }
    try
    {
      String[] tablesArray = this.tablesValue.split(";");
      for (String tbl : tablesArray)
      {
        String tableName = tbl.trim();
        String catalog = this.database;
        String schema = null;
        String table = null;
        if (tableName.contains("."))
        {
          StringTokenizer tokenizer = new StringTokenizer(tableName, ".");
          if (tokenizer.countTokens() != 2) {
            throw new IllegalArgumentException("Illegal argument found in TABLES property. Expected argument should contain at most 2 dot separated strings. Found '" + tbl + "'");
          }
          catalog = tokenizer.nextToken();
          table = tokenizer.nextToken();
        }
        else
        {
          if (this.database == null) {
            throw new IllegalArgumentException("Illegal argument found in TABLES property. Unqualified table name given, but no Database property was included: '" + tbl + "'");
          }
          table = tableName;
        }
        if (logger.isDebugEnabled()) {
          logger.debug("Trying to fetch table metadata for catalog '" + catalog + "' and schema '" + schema + "' and table pattern '" + table + "'");
        }
        boolean aTableMatched = false;
        if (md == null) {
          md = this.connection.getMetaData();
        }
        tableResultSet = md.getTables(catalog, schema, table, new String[] { "TABLE", "VIEW", "ALIAS", "SYNONYM" });
        while (tableResultSet.next())
        {
          aTableMatched = true;
          String p1 = tableResultSet.getString(1);
          String p3 = tableResultSet.getString(3);
          String fullName = p1 + "." + p3;
          
          boolean isExcluded = excludedTablenameHash.get(fullName) != null;
          if (!isExcluded)
          {
            this.tables.add(fullName);
            if (logger.isInfoEnabled()) {
              logger.info("Adding table " + fullName + " to the list of tables to be queried");
            }
          }
          else if (logger.isInfoEnabled())
          {
            logger.info("Table " + fullName + " being excluded as it is in ExcludedTables");
          }
        }
        if (!aTableMatched) {
          logger.warn("MySQLReader: No tables matched " + tableName);
        }
      }
      if (tableResultSet != null) {
        try
        {
          tableResultSet.close();
        }
        catch (Exception e) {}
      }
      String errorString;
      Exception exception;
      if (this.tables.size() != 0) {
        return;
      }
    }
    catch (Exception e)
    {
      errorString = " Failure in fetching tables metadata from Database \n Cause : " + e.getCause() + ";" + "Message : " + e.getMessage();
      
      exception = new Exception(errorString);
      
      logger.error(errorString, e);
      throw exception;
    }
    finally
    {
      if (tableResultSet != null) {
        try
        {
          tableResultSet.close();
        }
        catch (Exception e) {}
      }
    }
    String errmsg = "MySQLReader: Found no tables to take change data from";
    logger.error(errmsg);
    throw new Exception(errmsg);
  }
  
  public void receiveImpl(int channel, Event event)
    throws Exception
  {
    try
    {
      Thread.sleep(500L);
    }
    catch (InterruptedException ie) {}
  }
  
  public void close()
    throws Exception
  {
    super.close();
    if (this.waClient != null)
    {
      this.waClient.close();
      this.waClient = null;
    }
  }
  
  public String getMetadataKey()
  {
    return "TableName";
  }
  
  public Map<String, TypeDefOrName> getMetadata()
  {
    return this.tableMetadata;
  }
  
  public static void main(String[] args)
    throws Exception
  {
    MySQLReader_1_0 mysql = new MySQLReader_1_0();
    Map<String, Object> map = new HashMap();
    map.put("ConnectionURL", "mysql://localhost:3306");
    
    map.put("Tables", "test.table1");
    map.put("UserName", "root");
    map.put("Password", "root");
    
    mysql.init(map, map, null, null, null, false, null);
  }
}

