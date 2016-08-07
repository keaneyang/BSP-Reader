package com.bloom.proc;

import com.bloom.common.constants.Constant.recordstatus;
import com.bloom.common.exc.AdapterException;
import com.bloom.source.lib.meta.Column;
import com.bloom.source.lib.meta.ShortColumn;
import com.bloom.source.lib.meta.StringColumn;
import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.reader.Reader;
import com.bloom.source.snmp.SNMPResultSet;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class SNMPReader
{
  private static Logger logger = Logger.getLogger(SNMPReader.class);
  
  public static void main(String[] args)
  {
    Map<String, Object> mp = new HashMap();
    mp.put("blocksize", Integer.valueOf(10));
    mp.put("ipaddress", "0.0.0.0");
    mp.put("portno", Integer.valueOf(15021));
    mp.put("alias", "src/main/resources/snmpalias.txt");
    try
    {
      Reader udpReader = Reader.UDPThreadedReader(new Property(mp));
      SNMPResultSet resultSet = new SNMPResultSet(udpReader, new Property(mp));
      resultSet.init();
      for (;;)
      {
        Constant.recordstatus rs = resultSet.next();
        if (rs == Constant.recordstatus.ERROR_RECORD)
        {
          System.out.println("Exception received.");
          break;
        }
        if (rs == Constant.recordstatus.NO_RECORD)
        {
          Thread.sleep(100L);
        }
        else
        {
          Map<String, Object> mappedEvent = resultSet.getResultAsMap();
          HashMap<String, Object> meta = resultSet.getMetaAsMap();
          for (Map.Entry<String, Object> entry : meta.entrySet()) {
            logger.debug("Key : [" + (String)entry.getKey() + "] Value :[" + entry.getValue().toString() + "]");
          }
          for (Map.Entry<String, Object> entry : mappedEvent.entrySet()) {
            logger.debug("Key : [" + (String)entry.getKey() + "] Value :[" + entry.getValue().toString() + "]");
          }
        }
      }
    }
    catch (AdapterException|InterruptedException|IOException e)
    {
      e.printStackTrace();
    }
  }
  
  private static Object extractDateTime(byte[] bytes)
  {
    Column shortColumn = new ShortColumn();
    short year = ((Short)shortColumn.getValue(bytes, 0, 2)).shortValue();
    Column stringColumn = new StringColumn();
    String st = (String)stringColumn.getValue(bytes, 8, 1) + bytes[9];
    int hoursOffset = Integer.parseInt(st);
    return new DateTime(year, bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], DateTimeZone.forOffsetHoursMinutes(hoursOffset, bytes[10]));
  }
}
