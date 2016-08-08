package com.bloom.proc;

import com.bloom.common.exc.AdapterException;
import com.bloom.source.lib.constant.Constant.fieldType;
import com.bloom.source.lib.meta.Column;
import com.bloom.source.lib.meta.JSONParser;
import com.bloom.source.lib.reader.Reader;
import com.bloom.source.lib.rs.MetaData;
import java.io.IOException;
import java.util.Hashtable;
import org.apache.log4j.Logger;

public class BinaryResultSetMetaData
  extends MetaData
{
  private String nextFileName = null;
  private String prevFileName = null;
  BinaryProperty prop;
  BinaryFileResultSet resultSet;
  Hashtable<Integer, Column> columnMetaData;
  static Logger logger = Logger.getLogger(BinaryResultSetMetaData.class);
  int colCount = 0;
  Column[] columns = null;
  JSONParser jsonParser = null;
  int endian = 0;
  boolean nullTerminatedString = false;
  
  public BinaryResultSetMetaData(BinaryFileResultSet waResultSet, BinaryProperty Binaryprop)
    throws AdapterException, IOException, InterruptedException
  {
    this.prop = Binaryprop;
    this.resultSet = waResultSet;
    
    this.endian = this.prop.endian;
    
    this.nullTerminatedString = this.prop.nullterminatedstring;
    
    String metaFileName = Binaryprop.meta_datafile;
    
    this.jsonParser = new JSONParser(metaFileName);
    
    int columnCount = this.jsonParser.getFieldCount();
    
    this.columnMetaData = new Hashtable(columnCount);
    
    this.columns = new Column[columnCount];
    setColumnCount(columnCount);
    setColumns();
  }
  
  public void setColumns()
  {
    for (int i = 0; i < this.colCount; i++) {
      this.columns[i] = this.jsonParser.fillColumnMetaData(this.columnMetaData, this.endian, this.nullTerminatedString, i);
    }
  }
  
  public Column[] getColumns()
  {
    return this.columns;
  }
  
  public void setColumnCount(int colCount)
  {
    this.colCount = colCount;
  }
  
  public int getColumnCount()
  {
    return this.colCount;
  }
  
  public String getDirectoryName()
  {
    return this.prop.directory;
  }
  
  public String getCurrentFileName()
  {
    if (this.resultSet.getCurrentFile() != null) {
      return this.resultSet.reader().name();
    }
    return null;
  }
  
  public String getNextFileName()
  {
    return this.nextFileName;
  }
  
  public String getPrevFileName()
  {
    return this.prevFileName;
  }
  
  public String getColumnName(int index)
  {
    if (!this.columnMetaData.isEmpty()) {
      return ((Column)this.columnMetaData.get(Integer.valueOf(index))).getName();
    }
    return null;
  }
  
  public Constant.fieldType getColumnType(int index)
  {
    return this.columns[index].getType();
  }
  
  public void close()
    throws IOException, AdapterException
  {
    this.resultSet.close();
  }
  
  public Column getColumn(int index)
  {
    return (Column)this.columnMetaData.get(Integer.valueOf(index));
  }
}
