package com.bloom.proc;

import com.bloom.proc.events.WAEvent;
import com.bloom.runtime.compiler.AST;
import com.bloom.runtime.compiler.TypeDefOrName;
import com.bloom.runtime.compiler.TypeField;
import com.bloom.runtime.compiler.TypeName;
import com.bloom.security.Password;
import com.bloom.source.cdc.common.TableMD;
import com.bloom.source.lib.constant.CDCConstant;
import com.bloom.source.lib.meta.DatabaseColumn;
import com.bloom.source.lib.type.columntype;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;

public class DatabaseReaderOld
  implements DatabaseReader
{
  private static Logger logger = Logger.getLogger(DatabaseReaderOld.class);
  private Connection connection = null;
  private PreparedStatement statement = null;
  private ResultSet results = null;
  private int colCount = 0;
  private String query = null;
  private String typeName = "QUERY";
  private String url = null;
  private UUID typeUUID = null;
  
  public synchronized void initDR(Map<String, Object> properties)
    throws Exception
  {
    String username = null;
    String password = null;
    int fetchSize = 0;
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
      logger.trace("Initialized DatabaseReaderOld with url: " + this.url);
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
      logger.trace("Initialized DatabaseReaderOld with username: " + username);
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
      logger.trace("Initialized DatabaseReaderOld with password: " + password);
    }
    try
    {
      if ((properties.containsKey("Query")) && (properties.get("Query") != null) && (((String)properties.get("Query")).length() > 0)) {
        this.query = ((String)properties.get("Query"));
      } else {
        throw new IllegalArgumentException("Expected required parameter 'query' not found");
      }
    }
    catch (ClassCastException e)
    {
      logger.error("Invalid Query format.Value specified cannot be cast to java.lang.String");
      
      throw new Exception("Invalid Query format.Value specified cannot be cast to java.lang.String");
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Initialized DatabaseReaderOld with query: " + this.query);
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
      fetchSize = fetchS;
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
      logger.trace("Initialized DatabaseReaderOld with fetchsize: " + fetchSize);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Retrieving from url '" + this.url + "'");
    }
    if ((this.query == null) || (this.query.length() == 0))
    {
      logger.error("Missing 'Query' property values");
      Exception exception = new Exception("Missing 'Query' property values");
      
      logger.error(exception.getMessage());
      throw exception;
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
    this.statement = this.connection.prepareStatement(this.query);
    
    this.statement.setFetchSize(fetchSize);
    if (logger.isDebugEnabled()) {
      logger.debug("Fetchsize" + fetchSize + "Executing query:" + this.query);
    }
    this.results = this.statement.executeQuery();
    if (logger.isDebugEnabled()) {
      logger.debug("Query executed sucessfully");
    }
    ResultSetMetaData metadata = this.results.getMetaData();
    this.colCount = metadata.getColumnCount();
  }
  
  public WAEvent nextEvent(UUID sourceUUID)
    throws NoSuchElementException, SQLException
  {
    if (this.results.next())
    {
      WAEvent out = new WAEvent(this.colCount, sourceUUID);
      out.metadata = new HashMap();
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
      return out;
    }
    throw new NoSuchElementException("No more elements in the resultset");
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
  
  public Map<String, TypeDefOrName> getMetadata()
    throws Exception
  {
    Map<String, TypeDefOrName> tableMetadata = new HashMap();
    DatabaseColumn column = null;
    try
    {
      DatabaseMetaData dmd = this.connection.getMetaData();
      column = DatabaseColumn.initializeDataBaseColumnFromProductName(dmd.getDatabaseProductName());
    }
    catch (Exception e1)
    {
      logger.error("Unable to identify the DatabaseColumn class for this database.");
    }
    try
    {
      ResultSetMetaData metadata = this.results.getMetaData();
      int colCount = metadata.getColumnCount();
      List<TypeField> columnList = new ArrayList(colCount);
      List<String> columnNameList = new ArrayList(colCount);
      for (int i = 1; i <= colCount; i++)
      {
        String columnName = metadata.getColumnName(i);
        if ((columnName == null) || (columnName.isEmpty()))
        {
          columnName = "Column_" + i;
          logger.warn("Column name with index " + i + " is empty. Corresponding field name for this column index is " + columnName);
        }
        else
        {
          columnNameList.add(columnName);
        }
      }
      columnNameList = TableMD.updateDuplicateColumns(columnNameList);
      for (int i = 1; i <= colCount; i++)
      {
        TypeName tn = null;
        if (column != null)
        {
          column.setInternalColumnType(metadata.getColumnTypeName(i));
          tn = AST.CreateType(CDCConstant.getCorrespondingClassForCDCType(column.getInternalColumnType().getType()), 0);
        }
        columnList.add(AST.TypeField((String)columnNameList.get(i - 1), tn != null ? tn : AST.CreateType("string", 0), false));
      }
      TypeDefOrName tableDef = new TypeDefOrName(this.typeName, columnList);
      tableMetadata.put(this.typeName, tableDef);
    }
    catch (SQLException e)
    {
      logger.error("Failed to create Striim type. " + e);
      throw e;
    }
    return tableMetadata;
  }
  
  public String getMetadataKey()
  {
    return "TableName";
  }
  
  public String getCurrentTableName()
  {
    return this.typeName;
  }
}
