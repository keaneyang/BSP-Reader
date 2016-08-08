package com.bloom.proc;


import com.bloom.common.constants.Constant.recordstatus;
import com.bloom.common.exc.AdapterException;
import com.bloom.source.binary.BinaryFileResultSet;
import com.bloom.source.binary.BinaryProperty;
import com.bloom.source.binary.BinaryResultSetMetaData;
import com.bloom.source.lib.constant.Constant.fieldType;
import com.bloom.source.lib.reader.Reader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

public class BinaryReader
{
  public BinaryFileResultSet resultSet;
  static Logger logger = Logger.getLogger(BinaryReader.class);
  
  public static void main(String[] args)
    throws IOException, InterruptedException, AdapterException
  {
    DataOutputStream os = null;
    
    String outputDir = null;
    String outputFileName = null;
    String currFileName = null;
    int breakpt = 0;
    
    int recordCount = 0;
    Map<String, Object> mp = new HashMap();
    mp.put("directory", "/Users/mahadevan/binary");
    mp.put("metadata", "/Users/mahadevan/binary/metadata.json");
    mp.put("outDir", "/Users/mahadevan/Desktop");
    mp.put("wildcard", "test.bin");
    mp.put("blockSize", Integer.valueOf(64));
    mp.put("eof", Integer.valueOf(1000));
    mp.put("positionByEOF", Boolean.valueOf(false));
    
    mp.put("endian", "true");
    mp.put("encoding", Integer.valueOf(1));
    
    BinaryProperty binaryprop = new BinaryProperty(mp);
    long stime = System.currentTimeMillis();
    
    Reader fileReader = Reader.FileReader(binaryprop);
    BinaryFileResultSet resultSet = new BinaryFileResultSet(fileReader, binaryprop);
    BinaryResultSetMetaData resultSetMetaData = (BinaryResultSetMetaData)resultSet.getMetaData();
    try
    {
      for (;;)
      {
        if ((resultSet.next() == Constant.recordstatus.VALID_RECORD) || 
        
          (resultSet.getColumnCount() > 0))
        {
          for (int i = 0; i < resultSetMetaData.getColumnCount(); i++)
          {
            if (resultSetMetaData.getColumnType(i) == Constant.fieldType.BYTE) {
              System.out.println(((Byte)resultSet.getColumnValue(i)).byteValue());
            }
            if (resultSetMetaData.getColumnType(i) == Constant.fieldType.DOUBLE) {
              System.out.println(((Double)resultSet.getColumnValue(i)).doubleValue());
            }
            if (resultSetMetaData.getColumnType(i) == Constant.fieldType.LONG) {
              System.out.println(((Double)resultSet.getColumnValue(i)).doubleValue());
            }
            if (resultSetMetaData.getColumnType(i) == Constant.fieldType.INTEGER)
            {
              int value = ((Integer)resultSet.getColumnValue(i)).intValue();
              System.out.println(((Integer)resultSet.getColumnValue(i)).intValue());
            }
            if (resultSetMetaData.getColumnType(i) == Constant.fieldType.SHORT) {
              System.out.println(((Short)resultSet.getColumnValue(i)).shortValue());
            }
            if (resultSetMetaData.getColumnType(i) == Constant.fieldType.STRING)
            {
              String str = (String)resultSet.getColumnValue(i);
              System.out.println(str);
            }
          }
          recordCount++;
        }
      }
    }
    catch (IOException e)
    {
      e.printStackTrace();
      
      os.close();
      long etime = System.currentTimeMillis();
      long diff = etime - stime;
      System.out.print("Total time to process " + recordCount + " record is : " + diff + " milli seconds,\n");
      resultSetMetaData.close();
    }
  }
}

