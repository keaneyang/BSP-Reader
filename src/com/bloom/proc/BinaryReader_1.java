package com.bloom.proc;

import com.bloom.common.constants.Constant.recordstatus;
import com.bloom.common.exc.AdapterException;
import com.bloom.source.binary.BinaryFileResultSet;
import com.bloom.source.binary.BinaryProperty;
import com.bloom.source.binary.BinaryResultSetMetaData;
import com.bloom.source.lib.constant.Constant.fieldType;
import com.bloom.source.lib.reader.Reader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

public class BinaryReader_1
{
  static Reader binaryReader;
  
  public static void main(String[] args)
    throws AdapterException, IOException, InterruptedException
  {
    Map<String, Object> mp = new HashMap();
    
    mp.put("directory", "/Users/mahadevan/binary");
    mp.put("metadata", "/Users/mahadevan/binary/metadata.json");
    mp.put("wildcard", "test.bin");
    mp.put("positionByEOF", Boolean.valueOf(false));
    mp.put("nullterminatedstring", Boolean.valueOf(true));
    
    BinaryProperty prop = new BinaryProperty(mp);
    
    binaryReader = Reader.FileReader(prop);
    
    BinaryFileResultSet resultSet = new BinaryFileResultSet(binaryReader, prop);
    
    BinaryResultSetMetaData resultSetMetaData = (BinaryResultSetMetaData)resultSet.getMetaData();
    while (resultSet.next() != Constant.recordstatus.INVALID_RECORD) {
      for (int i = 0; i < resultSet.getColumnCount(); i++)
      {
        if (resultSetMetaData.getColumnType(i) == Constant.fieldType.BYTE) {
          System.out.println(((Byte)resultSet.getColumnValue(i)).byteValue());
        }
        if (resultSetMetaData.getColumnType(i) == Constant.fieldType.INTEGER) {
          System.out.println(((Integer)resultSet.getColumnValue(i)).intValue());
        }
        if (resultSetMetaData.getColumnType(i) == Constant.fieldType.DOUBLE) {
          System.out.println(((Double)resultSet.getColumnValue(i)).doubleValue());
        }
        if (resultSetMetaData.getColumnType(i) == Constant.fieldType.LONG) {
          System.out.println(((Long)resultSet.getColumnValue(i)).longValue());
        }
        if (resultSetMetaData.getColumnType(i) == Constant.fieldType.FLOAT) {
          System.out.println(((Float)resultSet.getColumnValue(i)).floatValue());
        }
        if (resultSetMetaData.getColumnType(i) == Constant.fieldType.SHORT) {
          System.out.println(((Short)resultSet.getColumnValue(i)).shortValue());
        }
        if (resultSetMetaData.getColumnType(i) == Constant.fieldType.STRING) {
          System.out.println((String)resultSet.getColumnValue(i));
        }
      }
    }
  }
}
