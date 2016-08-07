package com.bloom.source.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.XidEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import com.bloom.proc.events.WAEvent;
import com.bloom.uuid.UUID;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Time;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

public class ParseMySQLEvent
{
  private WAEvent wEvent;
  private int server_id_bits;
  private int server_id_mask;
  private String artificialTransactionID;
  private BinaryLogClient binlogClient;
  private static final Logger logger = Logger.getLogger(ParseMySQLEvent.class);
  private HashMap<Long, TableMapEventData> tableMap;
  
  public ParseMySQLEvent()
  {
    this.tableMap = new HashMap();
  }
  
  public void setServer_ID_Bits(int server_id_bits)
  {
    this.server_id_bits = server_id_bits;
    this.server_id_mask = -1;
    if (server_id_bits == 31) {
      this.server_id_mask = Integer.MAX_VALUE;
    }
    if (server_id_bits < 31) {
      this.server_id_mask = ((1 << server_id_bits) - 1);
    }
  }
  
  public void setBinaryLogClient(BinaryLogClient binlogClient)
  {
    this.binlogClient = binlogClient;
  }
  
  private void makeWAEvent(Event event, int numColumns, int numBeforeColumns, UUID uuid, boolean sendBeforeImage, int seqno)
  {
    EventHeaderV4 header = (EventHeaderV4)event.getHeader();
    
    this.wEvent = new WAEvent(numColumns, null);
    this.wEvent.data = new Object[numColumns];
    this.wEvent.metadata = new HashMap();
    if (numBeforeColumns > 0) {
      this.wEvent.before = new Object[numBeforeColumns];
    }
    this.wEvent.metadata.put("TimeStamp", Long.valueOf(header.getTimestamp()));
    this.wEvent.setID(uuid);
  }
  
  public WAEvent parseMySQLEvent(Event event, UUID uuid, boolean sendBeforeImage, int seqno, ArrayList<Integer> keyCols)
  {
    EventType type = event.getHeader().getEventType();
    this.wEvent = null;
    int columnCount;
    switch (type)
    {
    case QUERY: 
      QueryEventData data = (QueryEventData)event.getData();
      String sql = data.getSql();
      if (sql.toUpperCase().contains("BEGIN"))
      {
        makeWAEvent(event, 2, 0, uuid, sendBeforeImage, seqno);
        this.wEvent.metadata.put("OperationName", "BEGIN");
        this.wEvent.setData(0, data.getDatabase());
        this.wEvent.setData(1, sql);
        this.artificialTransactionID = makeArtificialTransactionID(event);
        this.wEvent.metadata.put("TxnID", this.artificialTransactionID);
      }
      else if ((sql.contains("create")) || (sql.contains("CREATE")))
      {
        makeWAEvent(event, 2, 0, uuid, sendBeforeImage, seqno);
        this.wEvent.metadata.put("OperationName", "DDL CREATE");
        this.wEvent.setData(0, data.getDatabase());
        this.wEvent.setData(1, sql);
      }
      else if (sql.toUpperCase().contains("DROP"))
      {
        makeWAEvent(event, 2, 0, uuid, sendBeforeImage, seqno);
        this.wEvent.metadata.put("OperationName", "DDL DROP");
        this.wEvent.setData(0, data.getDatabase());
        this.wEvent.setData(1, sql);
      }
      break;
    case WRITE_ROWS: 
    case EXT_WRITE_ROWS: 
      WriteRowsEventData data = (WriteRowsEventData)event.getData();
      columnCount = getColumnCountFromTableMap(event);
      makeWAEvent(event, columnCount, 0, uuid, sendBeforeImage, seqno);
      TableMapEventData tableMapData = (TableMapEventData)this.tableMap.get(Long.valueOf(data.getTableId()));
      this.wEvent.metadata.put("TableName", createFullTablename(tableMapData));
      this.wEvent.metadata.put("OperationName", "INSERT");
      this.wEvent.metadata.put("TxnID", this.artificialTransactionID);
      Serializable[] columns = (Serializable[])data.getRows().get(seqno);
      byte[] includedColumnTypes = new byte[columns.length];
      if (tableMapData.getColumnTypes().length != columns.length) {
        getIncludedColumnTypes(tableMapData.getColumnTypes(), includedColumnTypes, data.getIncludedColumns());
      } else {
        includedColumnTypes = tableMapData.getColumnTypes();
      }
      for (int i = 0; i < columns.length; i++)
      {
        int typeCode = includedColumnTypes[i] & 0xFF;
        ColumnType columnType = ColumnType.byCode(typeCode);
        try
        {
          if (logger.isDebugEnabled()) {
            logger.debug("Parse insert: i,typeCode,columnType,column[i]= " + i + " " + typeCode + " " + columnType + " " + columns[i]);
          }
          this.wEvent.setData(i, getColumnData(columnType, columns[i]));
          if (logger.isDebugEnabled()) {
            logger.debug("wEvent.data[i]= " + this.wEvent.data[i]);
          }
        }
        catch (Exception e)
        {
          this.wEvent.data[i] = null;
          logger.error("Unable to parse data[" + i + "]." + e, e);
        }
      }
      break;
    case UPDATE_ROWS: 
    case EXT_UPDATE_ROWS: 
      UpdateRowsEventData data = (UpdateRowsEventData)event.getData();
      columnCount = getColumnCountFromTableMap(event);
      int beforeColumnCount;
      int beforeColumnCount;
      if (sendBeforeImage) {
        beforeColumnCount = columnCount;
      } else {
        beforeColumnCount = 0;
      }
      makeWAEvent(event, columnCount, beforeColumnCount, uuid, sendBeforeImage, seqno);
      TableMapEventData tableMapData = (TableMapEventData)this.tableMap.get(Long.valueOf(data.getTableId()));
      this.wEvent.metadata.put("OperationName", "UPDATE");
      this.wEvent.metadata.put("TableName", createFullTablename(tableMapData));
      this.wEvent.metadata.put("TxnID", this.artificialTransactionID);
      
      Serializable[] columnsBefore = (Serializable[])((Map.Entry)data.getRows().get(seqno)).getKey();
      Serializable[] columnsAfter = (Serializable[])((Map.Entry)data.getRows().get(seqno)).getValue();
      byte[] includedColumnTypesBefore = new byte[columnsBefore.length];
      byte[] includedColumnTypesAfter = new byte[columnsAfter.length];
      if (tableMapData.getColumnTypes().length != columnsBefore.length) {
        getIncludedColumnTypes(tableMapData.getColumnTypes(), includedColumnTypesBefore, data.getIncludedColumnsBeforeUpdate());
      } else {
        includedColumnTypesBefore = tableMapData.getColumnTypes();
      }
      if (tableMapData.getColumnTypes().length != columnsAfter.length) {
        getIncludedColumnTypes(tableMapData.getColumnTypes(), includedColumnTypesAfter, data.getIncludedColumns());
      } else {
        includedColumnTypesAfter = tableMapData.getColumnTypes();
      }
      int maxAfterBeforeLength = columnsAfter.length > columnsBefore.length ? columnsAfter.length : columnsBefore.length;
      boolean keyChanged = false;
      int keyColsIndex = 0;
      int maxKeyColsIndex = keyCols.size();
      for (int i = 0; i < maxAfterBeforeLength; i++)
      {
        int typeCode = i < columnsAfter.length ? includedColumnTypesAfter[i] & 0xFF : includedColumnTypesBefore[i] & 0xFF;
        ColumnType columnType = ColumnType.byCode(typeCode);
        Object valAfter = null;
        Object valBefore = null;
        try
        {
          if (logger.isDebugEnabled()) {
            logger.debug("Parse update: i,typeCode,columnType,keyColsIndex= " + i + " " + typeCode + " " + columnType + " " + keyColsIndex);
          }
          if (i < columnsAfter.length)
          {
            valAfter = getColumnData(columnType, columnsAfter[i]);
            this.wEvent.setData(i, valAfter);
            if (logger.isDebugEnabled()) {
              logger.debug("wEvent.data[i]= " + this.wEvent.data[i]);
            }
          }
          if (i < columnsBefore.length)
          {
            valBefore = getColumnData(columnType, columnsBefore[i]);
            if (sendBeforeImage)
            {
              this.wEvent.setBefore(i, valBefore);
              if (logger.isDebugEnabled()) {
                logger.debug("wEvent.before[i]= " + this.wEvent.before[i]);
              }
            }
          }
          if ((!keyChanged) && (keyColsIndex < maxKeyColsIndex) && (((Integer)keyCols.get(keyColsIndex)).intValue() == i))
          {
            if ((valAfter != null) && (valBefore != null))
            {
              if (!valAfter.equals(valBefore)) {
                keyChanged = true;
              }
            }
            else if ((valAfter != null) || (valBefore != null)) {
              keyChanged = true;
            }
            keyColsIndex++;
          }
        }
        catch (Exception e)
        {
          this.wEvent.data[i] = null;
          logger.error("Unable to parse data[" + i + "]." + e, e);
        }
      }
      this.wEvent.metadata.put("PK_UPDATE", Boolean.toString(keyChanged));
      
      break;
    case DELETE_ROWS: 
    case EXT_DELETE_ROWS: 
      DeleteRowsEventData data = (DeleteRowsEventData)event.getData();
      columnCount = getColumnCountFromTableMap(event);
      makeWAEvent(event, columnCount, 0, uuid, sendBeforeImage, seqno);
      TableMapEventData tableMapData = (TableMapEventData)this.tableMap.get(Long.valueOf(data.getTableId()));
      this.wEvent.metadata.put("OperationName", "DELETE");
      this.wEvent.metadata.put("TableName", createFullTablename(tableMapData));
      this.wEvent.metadata.put("TxnID", this.artificialTransactionID);
      
      Serializable[] columns = (Serializable[])data.getRows().get(seqno);
      byte[] includedColumnTypes = new byte[columns.length];
      if (tableMapData.getColumnTypes().length != columns.length) {
        getIncludedColumnTypes(tableMapData.getColumnTypes(), includedColumnTypes, data.getIncludedColumns());
      } else {
        includedColumnTypes = tableMapData.getColumnTypes();
      }
      for (int i = 0; i < columns.length; i++)
      {
        int typeCode = includedColumnTypes[i] & 0xFF;
        ColumnType columnType = ColumnType.byCode(typeCode);
        try
        {
          if (logger.isDebugEnabled()) {
            logger.debug("Parse delete: i,typeCode,columnType,column[i]= " + i + " " + typeCode + " " + columnType + " " + columns[i]);
          }
          this.wEvent.setData(i, getColumnData(columnType, columns[i]));
          if (logger.isDebugEnabled()) {
            logger.debug("wEvent.data[i]= " + this.wEvent.data[i]);
          }
        }
        catch (Exception e)
        {
          this.wEvent.data[i] = null;
          logger.error("Unable to parse data[" + i + "]." + e, e);
        }
      }
      break;
    case XID: 
      XidEventData data = (XidEventData)event.getData();
      seqno = 0;
      makeWAEvent(event, 1, 0, uuid, sendBeforeImage, seqno);
      this.wEvent.metadata.put("OperationName", "COMMIT");
      this.wEvent.metadata.put("TxnID", this.artificialTransactionID);
      try
      {
        this.wEvent.setData(0, Long.valueOf(data.getXid()));
      }
      catch (Exception e)
      {
        this.wEvent.data[0] = null;
        logger.error("Unable to parse " + data.getXid() + "." + e, e);
      }
      break;
    case STOP: 
      makeWAEvent(event, 1, 0, uuid, sendBeforeImage, seqno);
      this.wEvent.metadata.put("OperationName", "STOP");
      break;
    default: 
      this.wEvent = null;
    }
    return this.wEvent;
  }
  
  String makeArtificialTransactionID(Event event)
  {
    EventHeaderV4 header = (EventHeaderV4)event.getHeader();
    
    String binlogname = this.binlogClient.getBinlogFilename();
    int i = binlogname.lastIndexOf(".");
    String binlogSuffix = binlogname.substring(i + 1);
    
    String artificialTransactionID = String.valueOf(header.getServerId() & this.server_id_mask).concat(":");
    artificialTransactionID = artificialTransactionID.concat(binlogSuffix).concat(":");
    artificialTransactionID = artificialTransactionID.concat(String.valueOf(header.getPosition())).concat(":");
    artificialTransactionID = artificialTransactionID.concat(String.valueOf(header.getTimestamp()));
    return artificialTransactionID;
  }
  
  private void getIncludedColumnTypes(byte[] types, byte[] includedColumnTypes, BitSet includedColumn)
  {
    int skipColumns = 0;
    for (int i = 0; i < types.length; i++) {
      if (!includedColumn.get(i)) {
        skipColumns++;
      } else {
        includedColumnTypes[(i - skipColumns)] = types[i];
      }
    }
  }
  
  private String createFullTablename(TableMapEventData tm)
  {
    return tm.getDatabase() + "." + tm.getTable();
  }
  
  public void updateTableMap(Event event)
  {
    TableMapEventData data = (TableMapEventData)event.getData();
    Long tableId = Long.valueOf(data.getTableId());
    this.tableMap.put(tableId, data);
    if (logger.isDebugEnabled()) {
      logger.debug("updateTableMap: " + tableId + " " + data);
    }
  }
  
  public String getTablenameFromRowChangeEvent(Event event)
  {
    long tableId = getTableIdFromRowChangeEvent(event);
    
    TableMapEventData tm = (TableMapEventData)this.tableMap.get(Long.valueOf(tableId));
    if (tm == null) {
      return null;
    }
    return createFullTablename(tm);
  }
  
  private int getColumnCountFromTableMap(Event event)
  {
    long tableId = getTableIdFromRowChangeEvent(event);
    TableMapEventData tm = (TableMapEventData)this.tableMap.get(Long.valueOf(tableId));
    return tm.getColumnTypes().length;
  }
  
  private long getTableIdFromRowChangeEvent(Event event)
  {
    EventType type = event.getHeader().getEventType();
    long tableId;
    switch (type)
    {
    case WRITE_ROWS: 
    case EXT_WRITE_ROWS: 
      WriteRowsEventData data = (WriteRowsEventData)event.getData();
      tableId = data.getTableId();
      break;
    case UPDATE_ROWS: 
    case EXT_UPDATE_ROWS: 
      UpdateRowsEventData data = (UpdateRowsEventData)event.getData();
      tableId = data.getTableId();
      break;
    case DELETE_ROWS: 
    case EXT_DELETE_ROWS: 
      DeleteRowsEventData data = (DeleteRowsEventData)event.getData();
      tableId = data.getTableId();
      break;
    default: 
      tableId = 0L;
    }
    return tableId;
  }
  
  public int getRowCount(Event event)
  {
    EventType type = event.getHeader().getEventType();
    int rowCount;
    switch (type)
    {
    case WRITE_ROWS: 
    case EXT_WRITE_ROWS: 
      WriteRowsEventData data = (WriteRowsEventData)event.getData();
      rowCount = data.getRows().size();
      break;
    case UPDATE_ROWS: 
    case EXT_UPDATE_ROWS: 
      UpdateRowsEventData data = (UpdateRowsEventData)event.getData();
      rowCount = data.getRows().size();
      break;
    case DELETE_ROWS: 
    case EXT_DELETE_ROWS: 
      DeleteRowsEventData data = (DeleteRowsEventData)event.getData();
      rowCount = data.getRows().size();
      break;
    default: 
      rowCount = 0;
    }
    return rowCount;
  }
  
  private Object getColumnData(ColumnType columnType, Serializable columns)
  {
    if (columns == null) {
      return columns;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Class type name for column: " + columns.getClass().getName());
    }
    switch (columnType)
    {
    case BIT: 
    case INT24: 
    case LONG: 
    case LONGLONG: 
    case FLOAT: 
    case DOUBLE: 
    case STRING: 
    case VARCHAR: 
    case VAR_STRING: 
      return columns;
    case TINY: 
      return Byte.valueOf((byte)((Integer)columns).intValue());
    case SHORT: 
      return Short.valueOf((short)((Integer)columns).intValue());
    case NEWDECIMAL: 
      return ((BigDecimal)columns).toPlainString();
    case DATE: 
      LocalDate date = new LocalDate((java.sql.Date)columns);
      return date;
    case TIME: 
    case TIME_V2: 
      LocalTime time = new LocalTime((Time)columns);
      return time;
    case TIMESTAMP: 
    case TIMESTAMP_V2: 
      DateTime timestamp = new DateTime((java.util.Date)columns);
      return timestamp;
    case DATETIME: 
    case DATETIME_V2: 
      DateTime timestamp = new DateTime((java.util.Date)columns);
      return timestamp;
    case YEAR: 
      return Integer.valueOf(columns.toString());
    case BLOB: 
      return new String((byte[])columns);
    case ENUM: 
      return (String)columns;
    case SET: 
      return (String)columns;
    case GEOMETRY: 
      return null;
    }
    logger.warn("Unsupported type " + columnType + " java type - " + columns.getClass().getName());
    return null;
  }
}
