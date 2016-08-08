package com.bloom.proc;

import com.bloom.proc.events.WAEvent;
import com.bloom.runtime.compiler.TypeDefOrName;
import com.bloom.security.Password;
import com.bloom.source.cdc.common.TableMD;
import com.bloom.uuid.UUID;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;

public class DatabaseReaderNew
  implements DatabaseReader
{
  private Connection connection = null;
  private PreparedStatement statement = null;
  private ResultSet results = null;
  private List<String> tables = null;
  private List<String> excludedTables = new ArrayList();
  private int currentTableIndex = 0;
  private String currentTableName = null;
  private int colCount = 0;
  private String tablesValue = null;
  private int fetchSize = 100;
  private String url = null;
  private static Logger logger = Logger.getLogger(DatabaseReaderNew.class);
  
  public synchronized void initDR(Map<String, Object> properties)
    throws Exception
  {
    String username = null;
    String password = null;
    try
    {
      if ((properties.containsKey("ConnectionURL")) && (properties.get("ConnectionURL") != null) && (((String)properties.get("ConnectionURL")).length() > 0))
      {
        this.url = ((String)properties.get("ConnectionURL"));
      }
      else
      {
        logger.error("ConnectionURL is not specified");
        throw new Exception("ConnectionURL is not specified");
      }
    }
    catch (ClassCastException e)
    {
      logger.error("Invalid ConnectionURL format.Value specified cannot be cast to java.lang.String");
      
      throw new Exception("Invalid ConnectionURL format.Value specified cannot be cast to java.lang.String");
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Initialized DatabaseReaderNew with url: " + this.url);
    }
    try
    {
      if ((properties.containsKey("Username")) && (properties.get("Username") != null) && (((String)properties.get("Username")).length() > 0))
      {
        username = (String)properties.get("Username");
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
      logger.trace("Initialized DatabaseReaderNew with username: " + username);
    }
    try
    {
      if ((properties.containsKey("Password")) && (properties.get("Password") != null) && (((Password)properties.get("Password")).getPlain().length() > 0))
      {
        password = ((Password)properties.get("Password")).getPlain();
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
      logger.trace("Initialized DatabaseReaderNew with password: " + password);
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
      logger.trace("Initialized DatabaseReaderNew with tableList:" + this.tablesValue);
    }
    try
    {
      if ((properties.containsKey("ExcludedTables")) && (properties.get("ExcludedTables") != null) && (((String)properties.get("ExcludedTables")).length() > 0))
      {
        String excludedTableStr = properties.get("ExcludedTables").toString();
        
        String[] tabs = excludedTableStr.split(";");
        for (String tab : tabs) {
          this.excludedTables.add(tab.substring(tab.lastIndexOf('.') + 1));
        }
      }
    }
    catch (Exception e)
    {
      String errMsg = "Invalid value in excludedTables property: " + e.getMessage();
      logger.error(errMsg);
      throw new IllegalArgumentException(errMsg);
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Initialized DatabaseReaderNew with excludedTables:" + Arrays.toString(this.excludedTables.toArray()));
    }
    try
    {
      int fetchS = 100;
      if (properties.containsKey("FetchSize"))
      {
        if (properties.get("FetchSize") == null)
        {
          Exception exception = new Exception("FetchSize is specified Null");
          logger.error(exception.getMessage());
          throw exception;
        }
        Object val = properties.get("FetchSize");
        if ((val instanceof Number)) {
          fetchS = ((Number)val).intValue();
        } else if ((val instanceof String)) {
          fetchS = Integer.parseInt((String)val);
        }
        if ((fetchS < 0) || (fetchS > 1000000000))
        {
          Exception exception = new Exception("FetchSize specified is out of Range");
          logger.error(exception.getMessage());
          throw exception;
        }
      }
      else
      {
        fetchS = 100;
      }
      this.fetchSize = fetchS;
    }
    catch (ClassCastException e)
    {
      Exception exception = new Exception("Invalid FetchSize format. Value specified cannot be cast to java.lang.Integer");
      
      logger.error(exception.getMessage());
      throw exception;
    }
    catch (NumberFormatException e)
    {
      Exception exception = new Exception("Invalid FetchSize format. Value specified cannot be cast to java.lang.Integer");
      
      logger.error(exception.getMessage());
      throw exception;
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Initialized DatabaseReaderNew with fetchSize : " + this.fetchSize);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Retrieving from url '" + this.url + "'");
    }
    try
    {
      this.connection = DriverManager.getConnection(this.url, username, password);
    }
    catch (SQLException e)
    {
      String errorString = " Failure in connecting to Database with url : " + this.url + " username : " + username + " \n ErrorCode : " + e.getErrorCode() + ";" + "SQLCode : " + e.getSQLState() + ";" + "SQL Message : " + e.getMessage();
      
      Exception exception = new Exception(errorString);
      
      logger.error(errorString);
      throw exception;
    }
    catch (Exception e)
    {
      String errorString = " Failure in connecting to Database with url : " + this.url + " username : " + username + " \n Cause : " + e.getCause() + ";" + "Message : " + e.getMessage();
      
      Exception exception = new Exception(errorString);
      
      logger.error(errorString);
      throw exception;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Connected to Database with url:" + this.url);
    }
    initializeTableList();
    if (!properties.containsKey("SessionType")) {
      initializeNextResultSet();
    }
  }
  
  private synchronized void initializeTableList()
    throws Exception
  {
    this.tables = new ArrayList();
    try
    {
      String[] tablesArray = this.tablesValue.split(";");
      for (String tbl : tablesArray)
      {
        String tableName = tbl.trim();
        String catalog = null;
        String schema = null;
        String table = null;
        if (tableName.contains("."))
        {
          StringTokenizer tokenizer = new StringTokenizer(tableName, ".");
          if (tokenizer.countTokens() > 3) {
            throw new IllegalArgumentException("Illegal argument in TABLES property found. Expected argument should contain at most 3 dot seperated string. Found '" + tableName + "'");
          }
          if (tokenizer.countTokens() == 3) {
            catalog = tokenizer.nextToken();
          }
          schema = tokenizer.nextToken();
          table = tokenizer.nextToken();
        }
        else
        {
          table = tableName;
        }
        if (logger.isDebugEnabled()) {
          logger.debug("Trying to fetch table metadata for catalog '" + catalog + "' and schema '" + schema + "' and table pattern '" + table + "'");
        }
        DatabaseMetaData md = this.connection.getMetaData();
        ResultSet tableResultSet = md.getTables(catalog, schema, table, new String[] { "TABLE", "VIEW", "ALIAS", "SYNONYM" });
        while (tableResultSet.next())
        {
          String p1 = tableResultSet.getString(1);
          String p2 = tableResultSet.getString(2);
          String p3 = tableResultSet.getString(3);
          
          boolean isExcluded = this.excludedTables.contains(p3);
          if (!isExcluded)
          {
            StringBuilder tableFQN = new StringBuilder();
            if ((p1 != null) && (!p1.trim().isEmpty())) {
              tableFQN.append(p1 + ".");
            }
            if ((p2 != null) && (!p2.trim().isEmpty())) {
              tableFQN.append(p2 + ".");
            }
            tableFQN.append(p3);
            this.tables.add(tableFQN.toString());
            if (logger.isInfoEnabled()) {
              logger.info("Adding table " + tableFQN.toString() + " to the list of tables to be queried");
            }
          }
          else if (logger.isInfoEnabled())
          {
            logger.info("Table " + p3 + " being excluded as it is in explicit ExcludedTables");
          }
        }
      }
    }
    catch (Exception e)
    {
      String errorString = " Failure in fetching tables metadata from Database \n Cause : " + e.getCause() + ";" + "Message : " + e.getMessage();
      
      Exception exception = new Exception(errorString);
      
      logger.error(errorString);
      throw exception;
    }
  }
  
  private synchronized void initializeNextResultSet()
    throws SQLException, NoSuchElementException
  {
    try
    {
      if (this.results != null)
      {
        this.results.close();
        this.results = null;
        if (this.statement != null)
        {
          this.statement.close();
          this.statement = null;
        }
      }
      if (this.currentTableIndex >= this.tables.size()) {
        throw new NoSuchElementException("No more tables to be read");
      }
      this.currentTableName = ((String)this.tables.get(this.currentTableIndex++));
      if (logger.isDebugEnabled()) {
        logger.debug("Going to read from Table " + this.currentTableName);
      }
      String query = "Select * from " + this.currentTableName;
      this.statement = this.connection.prepareStatement(query);
      
      this.statement.setFetchSize(this.fetchSize);
      if (logger.isDebugEnabled()) {
        logger.debug("Fetchsize" + this.fetchSize + "Executing query:" + query);
      }
      this.results = this.statement.executeQuery();
      if (logger.isDebugEnabled()) {
        logger.debug("Query executed sucessfully");
      }
      ResultSetMetaData metadata = this.results.getMetaData();
      this.colCount = metadata.getColumnCount();
    }
    catch (IndexOutOfBoundsException ioe)
    {
      throw new NoSuchElementException("No more tables to read from the tableList");
    }
  }
  
  public WAEvent nextEvent(UUID sourceUUID)
    throws NoSuchElementException, SQLException
  {
    if ((this.results != null) && (this.results.next()))
    {
      if (logger.isTraceEnabled()) {
        logger.trace("Preparing event from an open Resultset");
      }
      return prepareEventFromResultSet(this.results, sourceUUID);
    }
    initializeNextResultSet();
    if ((this.results != null) && (this.results.next())) {
      return prepareEventFromResultSet(this.results, sourceUUID);
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Returning null event");
    }
    return null;
  }
  
  private WAEvent prepareEventFromResultSet(ResultSet rs, UUID sourceUUID)
    throws SQLException
  {
    WAEvent out = new WAEvent(this.colCount, sourceUUID);
    out.metadata = new HashMap();
    out.metadata.put("TableName", this.currentTableName);
    out.metadata.put("OperationName", "SELECT");
    out.metadata.put("ColumnCount", Integer.valueOf(this.colCount));
    
    out.data = new Object[this.colCount];
    ResultSetMetaData rsmd = this.results.getMetaData();
    for (int i = 0; i < this.colCount; i++)
    {
      int colIndex = i + 1;
      int colType = rsmd.getColumnType(colIndex);
      Object columnValue = null;
      switch (colType)
      {
      case -1: 
      case 1: 
      case 12: 
        columnValue = this.results.getString(colIndex);
        break;
      case -7: 
        columnValue = Boolean.valueOf(this.results.getBoolean(colIndex));
        break;
      case -6: 
        columnValue = Short.valueOf(this.results.getShort(colIndex));
        break;
      case 5: 
        columnValue = Short.valueOf(this.results.getShort(colIndex));
        break;
      case 4: 
        columnValue = Integer.valueOf(this.results.getInt(colIndex));
        break;
      case -5: 
        columnValue = Long.valueOf(this.results.getLong(colIndex));
        break;
      case 7: 
        columnValue = Float.valueOf(this.results.getFloat(colIndex));
        break;
      case 6: 
      case 8: 
        columnValue = Double.valueOf(this.results.getDouble(colIndex));
        break;
      case 2: 
      case 3: 
        columnValue = this.results.getString(colIndex);
        break;
      case 91: 
        Date date = this.results.getDate(colIndex);
        if (date != null) {
          columnValue = LocalDate.fromDateFields(date);
        }
        break;
      case -102: 
      case -101: 
      case 93: 
        Timestamp timestamp = this.results.getTimestamp(colIndex);
        columnValue = new DateTime(timestamp);
        break;
      default: 
        columnValue = this.results.getString(colIndex);
      }
      if (this.results.wasNull()) {
        columnValue = null;
      }
      out.setData(i, columnValue);
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Returning event " + out.toString());
    }
    return out;
  }
  
  public synchronized void close()
    throws SQLException
  {
    if (this.results == null) {
      logger.info("ResultSet is null");
    } else {
      this.results.close();
    }
    if (this.statement == null) {
      logger.info("PreparedStatement is null, ");
    } else {
      this.statement.close();
    }
    if (this.connection == null)
    {
      logger.info("Connection is null");
    }
    else
    {
      this.connection.close();
      if (logger.isDebugEnabled()) {
        logger.debug("Closed database connection sucessfully");
      }
    }
  }
  
  public String getMetadataKey()
  {
    return "TableName";
  }
  
  public Map<String, TypeDefOrName> getMetadata()
    throws Exception
  {
    HashMap<String, ArrayList<Integer>> tableKeyColumns = new HashMap();
    return TableMD.getTablesMetadataFromJDBCConnection(this.tables, this.connection, tableKeyColumns);
  }
  
  public String getCurrentTableName()
  {
    return this.currentTableName.replace('.', '_');
  }
}

