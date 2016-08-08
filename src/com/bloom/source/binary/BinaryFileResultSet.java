package com.bloom.source.binary;

import com.bloom.common.constants.Constant.recordstatus;
import com.bloom.common.exc.AdapterException;
import com.bloom.common.exc.RecordException;
import com.bloom.common.exc.RecordException.Type;
import com.bloom.source.lib.constant.Constant.fieldType;
import com.bloom.source.lib.meta.Column;
import com.bloom.source.lib.meta.StringColumnNullTerminated;
import com.bloom.source.lib.reader.Reader;
import com.bloom.source.lib.rs.BinaryResultSet;
import com.bloom.source.lib.rs.MetaData;
import com.bloom.source.smlite.SMEvent;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.log4j.Logger;

public class BinaryFileResultSet
  extends BinaryResultSet
{
  Logger logger = Logger.getLogger(BinaryFileResultSet.class);
  BinaryResultSetMetaData resultSetMetadata = null;
  Reader reader = null;
  int rowEndOffset = 0;
  int firstColumnEndPos = 0;
  public long rowStartOffset = 0L;
  Constant.recordstatus rstatus;
  public Column[] columns = null;
  int stringColumnLength = 0;
  private Object[] data;
  private byte[] dataArray;
  private boolean partialRecordExists = false;
  private int lastProcessedColIndex = 0;
  private int dataArrayOffset = 0;
  
  public BinaryFileResultSet(Reader reader, BinaryProperty BinaryProp)
    throws AdapterException, IOException, InterruptedException
  {
    super(reader, BinaryProp);
    this.reader = reader;
    Init();
    this.resultSetMetadata = new BinaryResultSetMetaData(this, BinaryProp);
    this.columns = this.resultSetMetadata.getColumns();
    this.stringColumnLength = BinaryProp.stringlengthcolumnsize;
    
    this.dataArray = new byte[reader().blockSize()];
  }
  
  public MetaData getMetaData()
  {
    return this.resultSetMetadata;
  }
  
  public Constant.recordstatus hasNext()
    throws RuntimeException
  {
    int index = 0;
    if (this.partialRecordExists)
    {
      index = this.lastProcessedColIndex;
    }
    else
    {
      index = 0;
      this.data = null;
      this.data = new Object[this.resultSetMetadata.getColumnCount()];
    }
    for (; index < this.resultSetMetadata.getColumnCount(); index++)
    {
      if (this.columns[index].getType() == Constant.fieldType.STRING)
      {
        int len = 0;
        if ((this.columns[index] instanceof StringColumnNullTerminated))
        {
          int bytes = readData(this.dataArray, this.dataArrayOffset, 1);
          if (bytes == 0)
          {
            this.dataArrayOffset = bytes;
            return partialRecord(index);
          }
          while (this.dataArray[this.dataArrayOffset] != 0)
          {
            this.dataArrayOffset += 1;
            if (readData(this.dataArray, this.dataArrayOffset, 1) < 1) {
              return partialRecord(index);
            }
          }
          len = this.dataArrayOffset;
          this.dataArrayOffset = 0;
          this.columns[index].setSize(len);
          this.data[index] = this.columns[index].getValue(this.dataArray, 0, len);
        }
        else
        {
          if (this.columns[index].getSize() <= 0)
          {
            int bytes = readData(this.dataArray, this.dataArrayOffset, this.stringColumnLength - this.dataArrayOffset);
            if (bytes == this.stringColumnLength - this.dataArrayOffset)
            {
              ByteBuffer buffer = ByteBuffer.allocate(this.stringColumnLength);
              buffer.put(this.dataArray, 0, this.stringColumnLength);
              buffer.flip();
              len = this.columns[index].getLengthOfString(buffer, this.stringColumnLength);
              this.columns[index].setSize(len);
              this.dataArrayOffset = 0;
            }
            else
            {
              this.dataArrayOffset += bytes;
              return partialRecord(index);
            }
          }
          int bytes = readData(this.dataArray, this.dataArrayOffset, this.columns[index].getSize() - this.dataArrayOffset);
          if (bytes == this.columns[index].getSize() - this.dataArrayOffset)
          {
            this.dataArrayOffset = 0;
            this.data[index] = this.columns[index].getValue(this.dataArray, 0, this.columns[index].getSize());
          }
          else
          {
            this.dataArrayOffset += bytes;
            return partialRecord(index);
          }
        }
        this.columns[index].setSize(0);
      }
      else
      {
        int bytes = readData(this.dataArray, this.dataArrayOffset, this.columns[index].getSize() - this.dataArrayOffset);
        if (bytes < this.columns[index].getSize() - this.dataArrayOffset)
        {
          this.dataArrayOffset += bytes;
          return partialRecord(index);
        }
        this.dataArrayOffset = 0;
        this.data[index] = this.columns[index].getValue(this.dataArray, 0, this.columns[index].getSize());
      }
      this.partialRecordExists = false;
      this.lastProcessedColIndex = index;
      this.dataArrayOffset = 0;
    }
    return Constant.recordstatus.VALID_RECORD;
  }
  
  public Constant.recordstatus partialRecord(int index)
  {
    this.partialRecordExists = true;
    this.lastProcessedColIndex = index;
    return Constant.recordstatus.NO_RECORD;
  }
  
  public int readData(byte[] buf, int offset, int length)
    throws RuntimeException
  {
    int readBytes = 0;
    readBytes = this.reader.read(buf, offset, length);
    if (readBytes < 0) {
      throw new RuntimeException("Lost the connection from data source.", new RecordException(RecordException.Type.END_OF_DATASOURCE));
    }
    return readBytes;
  }
  
  public void close()
    throws AdapterException
  {
    try
    {
      this.isClosed = true;
      super.close();
      if (this.reader != null) {
        this.reader.close();
      }
      this.columns = null;this.data = null;this.dataArray = null;
    }
    catch (IOException exp)
    {
      throw new AdapterException("Problem closing the reader " + this.reader.name(), exp);
    }
  }
  
  public boolean isClosed()
  {
    return this.isClosed;
  }
  
  public Constant.recordstatus getRecordStatus()
  {
    return this.rstatus;
  }
  
  public long getRowStartOffset()
  {
    return this.rowStartOffset;
  }
  
  public Object[] getColumnValue()
  {
    if (this.data == null) {
      return null;
    }
    return this.data;
  }
  
  public boolean onEvent(SMEvent eventData)
  {
    return false;
  }
}
