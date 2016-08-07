package com.bloom.source.mysql;

import com.bloom.Wizard.ICDCWizard;
import com.bloom.Wizard.ICDCWizard.DBTYPE;
import com.bloom.Wizard.ValidationResult;
import com.bloom.proc.MySQLReader_1_0;
import com.bloom.source.WizardCommons.WizCommons;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

public class MySQLWizard
  implements ICDCWizard
{
  private static Logger logger = Logger.getLogger(MySQLWizard.class);
  String jdbcString = "jdbc:mysql://";
  
  public String validateConnection(String userName, String password, String URL)
  {
    Connection dbConnection = null;
    logger.debug("Validating Connection...");
    ValidationResult resultObject;
    try
    {
      Class.forName("com.mysql.jdbc.Driver");
      dbConnection = getConnection(userName, password, URL);
      resultObject = new ValidationResult(true, "Connection Successful!");
    }
    catch (Exception e)
    {
      resultObject = new ValidationResult(false, "Error while making connection: " + e.getMessage());
    }
    String jsonResult = resultObject.toJson();
    
    closeConnection(dbConnection);
    return jsonResult;
  }
  
  public String checkPrivileges(String userName, String password, String URL)
  {
    Connection dbConnection = null;
    ValidationResult resultObject;
    try
    {
      dbConnection = getConnection(userName, password, URL);
      MySQLReader_1_0 mysqlreader = new MySQLReader_1_0(dbConnection);
      mysqlreader.checkMySQLPrivileges();
      resultObject = new ValidationResult(true, "Current User has all privileges!");
    }
    catch (Exception e)
    {
      resultObject = new ValidationResult(false, "Error while checking privileges: " + e.getMessage());
    }
    closeConnection(dbConnection);
    return resultObject.toJson();
  }
  
  public String checkVersion(String userName, String password, String URL)
  {
    Connection dbConnection = null;
    
    int[] versionToCheckAgainst = { 5, 5, 0 };
    
    logger.debug("Checking Version...");
    ValidationResult resultObject;
    try
    {
      dbConnection = getConnection(userName, password, URL);
      
      DatabaseMetaData meta = dbConnection.getMetaData();
      
      String mySQLVersion = meta.getDatabaseProductVersion();
      
      String[] versionSplit = mySQLVersion.split("-");
      String onlyVersion = versionSplit[0];
      
      MySQLReader_1_0 mysql = new MySQLReader_1_0(dbConnection);
      ValidationResult resultObject;
      if (!mysql.mysqlVersionLess(onlyVersion, versionToCheckAgainst)) {
        resultObject = new ValidationResult(true, "MySQL Version is compatible");
      } else {
        resultObject = new ValidationResult(false, "MySQL version " + onlyVersion + " not supported. MySQL version 5.5.0 or higher supported");
      }
      mysql.close();
    }
    catch (Exception e)
    {
      resultObject = new ValidationResult(false, "Error while checking version: " + e.getMessage());
    }
    String jsonResult = resultObject.toJson();
    closeConnection(dbConnection);
    return jsonResult;
  }
  
  public String getTables(String userName, String password, String URL, String fullTableName)
  {
    Connection dbConnection = null;
    ValidationResult resultObject;
    try
    {
      String parsedTableNames = parseTableNames(fullTableName);
      dbConnection = getConnection(userName, password, URL);
      
      doesDBExsit(dbConnection, parsedTableNames);
      WizCommons newWiz = new WizCommons(dbConnection);
      String allTables = newWiz.getTables(parsedTableNames, ICDCWizard.DBTYPE.MYSQL.name());
      resultObject = new ValidationResult(true, allTables);
    }
    catch (Exception e)
    {
      resultObject = new ValidationResult(false, "Error getting tables: " + e.getMessage());
    }
    closeConnection(dbConnection);
    return resultObject.toJson();
  }
  
  public void doesDBExsit(Connection dbConnection, String tableName)
    throws Exception
  {
    String[] dbAndTable = tableName.split("\\.");
    String dbName = "";
    try
    {
      PreparedStatement ps = dbConnection.prepareStatement("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '" + dbAndTable[0] + "'");
      ResultSet rs = ps.executeQuery();
      if (rs.next()) {
        dbName = rs.getString(1);
      }
    }
    catch (Exception e)
    {
      throw new Exception("ERROR while checking if DB exists: " + e.getMessage());
    }
    if (dbName.equalsIgnoreCase(dbAndTable[0])) {
      logger.info("MYSQL DB " + dbName + " exists");
    } else {
      throw new Exception("The database " + dbAndTable[0] + " does not exist.");
    }
  }
  
  public String getTableColumns(String userName, String password, String URL, String allTables)
    throws Exception
  {
    Connection dbConnection = null;
    
    String[] seperateDB = URL.split(";");
    URL = seperateDB[0];
    ValidationResult resultObject;
    try
    {
      dbConnection = getConnection(userName, password, URL);
      Map tablesAndCol = getMySQLcolumns(allTables, dbConnection);
      JSONObject tableMap = new JSONObject(tablesAndCol);
      String Stringmap = tableMap.toString();
      resultObject = new ValidationResult(true, Stringmap);
    }
    catch (Exception e)
    {
      resultObject = new ValidationResult(false, "Failure fetching column metadata: " + e.getMessage());
    }
    closeConnection(dbConnection);
    return resultObject.toJson();
  }
  
  public String checkCDCConfigurations(String userName, String password, String URL)
  {
    Connection dbConnection = null;
    ValidationResult resultObject = null;
    try
    {
      dbConnection = getConnection(userName, password, URL);
      MySQLReader_1_0 mySQLReader_1_0 = new MySQLReader_1_0(dbConnection);
      mySQLReader_1_0.checkMySQLConfig();
      if (logger.isInfoEnabled()) {
        logger.info("mysql config checking is done.");
      }
      resultObject = new ValidationResult(true, "MySQL CDC config check done.");
    }
    catch (Exception ex)
    {
      resultObject = new ValidationResult(false, "Exception while checking MySQL CDC configuration: " + ex.getMessage());
    }
    finally
    {
      closeConnection(dbConnection);
    }
    return resultObject.toJson();
  }
  
  public Map getMySQLcolumns(String allTables, Connection dbConnection)
    throws Exception
  {
    JSONArray newJArray = new JSONArray(allTables);
    LinkedHashMap tablesAndCol = new LinkedHashMap();
    String catalog = null;
    String schema = null;
    String table = null;
    String columnName = "";
    if (newJArray != null)
    {
      int arrLen = newJArray.length();
      for (int i = 0; i < arrLen; i++)
      {
        LinkedHashMap colAndType = new LinkedHashMap();
        String tn = newJArray.getString(i);
        StringTokenizer tokenizer = new StringTokenizer(tn, ".");
        if (tokenizer.countTokens() > 3) {
          throw new IllegalArgumentException("Illegal argument in TABLES property found. Expected argument should contain at most 3 dot separated string. Found '" + tn + "'");
        }
        if (tokenizer.countTokens() == 3)
        {
          catalog = tokenizer.nextToken();
          schema = tokenizer.nextToken();
        }
        if (tokenizer.countTokens() == 2)
        {
          catalog = tokenizer.nextToken();
          schema = catalog;
        }
        doesDBExsit(dbConnection, tn);
        table = tokenizer.nextToken();
        try
        {
          DatabaseMetaData md = dbConnection.getMetaData();
          ResultSet colResultSet = md.getColumns(catalog, schema, table, null);
          while (colResultSet.next())
          {
            columnName = colResultSet.getString("COLUMN_NAME");
            String dataType = colResultSet.getString("TYPE_NAME");
            colAndType.put(columnName, dataType);
          }
        }
        catch (SQLException e)
        {
          throw new SQLException("Could not retrieve column names for mySQL tables", e);
        }
        tablesAndCol.put(tn, colAndType);
      }
    }
    return tablesAndCol;
  }
  
  private String parseTableNames(String fullTableName)
    throws Exception
  {
    String catalog = null;
    String schema = null;
    String table = null;
    StringBuilder tablesList = new StringBuilder();
    try
    {
      String[] tablesArray = fullTableName.split(";");
      for (String tbl : tablesArray)
      {
        String tableName = tbl.trim();
        if (tableName.contains("."))
        {
          StringTokenizer tokenizer = new StringTokenizer(tableName, ".");
          if (tokenizer.countTokens() > 3) {
            throw new IllegalArgumentException("Illegal argument in TABLES property found. Expected argument should contain at most 3 dot separated string. Found '" + tableName + "'");
          }
          if (tokenizer.countTokens() == 3)
          {
            catalog = tokenizer.nextToken();
            schema = tokenizer.nextToken();
          }
          if (tokenizer.countTokens() == 2)
          {
            catalog = tokenizer.nextToken();
            schema = catalog;
          }
          table = tokenizer.nextToken();
          
          String fullTB = catalog + "." + schema + "." + table + ";";
          tablesList.append(fullTB);
        }
        else
        {
          throw new IllegalArgumentException("Please Specify a Database!");
        }
      }
    }
    catch (Exception e)
    {
      String errorString = " Failure in parsing tables \n Cause : " + e.getCause() + ";" + "Message : " + e.getMessage();
      
      throw new Exception(errorString);
    }
    return tablesList.toString();
  }
  
  private Connection getConnection(String userName, String password, String URL)
    throws SQLException
  {
    Connection dbConnection = null;
    URL = this.jdbcString + URL;
    if ((dbConnection == null) || (dbConnection.isClosed())) {
      dbConnection = DriverManager.getConnection(URL, userName, password);
    }
    return dbConnection;
  }
  
  private boolean closeConnection(Connection dbConnection)
  {
    boolean result = true;
    if (dbConnection != null) {
      try
      {
        if (!dbConnection.isClosed()) {
          dbConnection.close();
        }
      }
      catch (Exception ex)
      {
        logger.debug("Error while closing db connection. ", ex);
        result = false;
      }
    }
    return result;
  }
  
  public static void main(String[] args)
    throws Exception
  {
    String uname = "ameya";
    String pass = "pass";
    String URL = "localhost:3306";
    String tables = "dkr.%";
    
    MySQLWizard mysql = new MySQLWizard();
    System.out.println("Connection Status: " + mysql.validateConnection(uname, pass, URL));
    String allTables = mysql.getTables(uname, pass, URL, tables);
    System.out.println("All Tables: " + allTables);
    
    JSONObject tablesResponseAsJSON = new JSONObject(allTables);
    String tablesArray = tablesResponseAsJSON.getString("message");
    
    System.out.println("ALL COLUMNS:: " + mysql.getTableColumns(uname, pass, URL, tablesArray));
    System.out.println("Version Check: " + mysql.checkVersion(uname, pass, URL));
    System.out.println("Privilege Check: " + mysql.checkPrivileges(uname, pass, URL));
    System.out.println("Check CDC Configurations: " + mysql.checkCDCConfigurations(uname, pass, URL));
  }
}
