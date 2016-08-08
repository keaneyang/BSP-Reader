package com.bloom.proc;

import com.bloom.common.constants.Constant.recordstatus;
import com.bloom.common.exc.AdapterException;
import com.bloom.recovery.CheckpointDetail;
import com.bloom.source.csv.CSVProperty;
import com.bloom.source.csv.CSVResultSet;
import com.bloom.source.lib.reader.Reader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

public class CSVReader
{
  public CSVResultSet resultSet;
  static Logger logger = Logger.getLogger(CSVReader.class);
  
  public static void main(String[] args)
    throws IOException, InterruptedException
  {
    BufferedWriter bw = null;
    CSVResultSet resultSet = null;
    String outputDir = null;
    String outputFileName = null;
    String currFileName = null;
    Map<String, Object> mp = new HashMap();
    mp.put("directory", "/Users/arul/Downloads");
    
    mp.put("eof", Integer.valueOf(1000));
    mp.put("positionByEOF", Boolean.valueOf(false));
    mp.put("header", Boolean.valueOf(false));
    mp.put("rowdelimiter", "\n");
    mp.put("columndelimiter", ",");
    
    mp.put("wildcard", "t");
    
    mp.put("readerType", "FileReader");
    
    mp.put("charset", "UTF-8");
    
    mp.put("quoteset", "\"");
    
    CSVProperty csvprop = new CSVProperty(mp);
    long stime = System.currentTimeMillis();
    int recordCount = 0;
    try
    {
      outputDir = csvprop.outdirectory;
      Reader reader = Reader.createInstance(csvprop);
      resultSet = new CSVResultSet(reader, csvprop);
      outputFileName = outputDir + "/" + currFileName;
      
      BufferedWriter b = new BufferedWriter(new FileWriter(new File("/tmp/out.csv")));
      long starttime = System.currentTimeMillis();
      for (;;)
      {
        Constant.recordstatus status = resultSet.next();
        if (status == Constant.recordstatus.ERROR_RECORD)
        {
          Thread.sleep(100L);
        }
        else if ((status == Constant.recordstatus.NO_RECORD) || (status == Constant.recordstatus.INVALID_RECORD) || (status == Constant.recordstatus.END_OF_DATASOURCE))
        {
          Thread.sleep(100L);
        }
        else
        {
          recordCount++;
          CheckpointDetail recordPos = resultSet.getCheckpointDetail();
          if (recordCount % 1000000 == 0)
          {
            long endtime = System.currentTimeMillis();
            System.out.println("Diff : [" + (endtime - starttime) + "]");
          }
          System.out.println("Event Time : [" + resultSet.eventTime() + "]");
          String msg = "";
          for (int idx = 0; idx < resultSet.getColumnCount(); idx++)
          {
            System.out.println(" Col " + idx + ": [" + resultSet.getColumnValue(idx) + "]");
            if (!msg.isEmpty()) {
              msg = msg + "," + resultSet.getColumnValue(idx);
            } else {
              msg = resultSet.getColumnValue(idx);
            }
          }
        }
      }
    }
    catch (AdapterException soExp)
    {
      soExp.printStackTrace();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
  }
}

