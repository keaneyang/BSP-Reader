package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.common.constants.Constant.recordstatus;
import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import com.bloom.common.exc.RecordException;
import com.bloom.common.exc.RecordException.Type;
import com.bloom.event.Event;
import com.bloom.proc.events.WAEvent;
import com.bloom.recovery.CheckpointDetail;
import com.bloom.source.lib.intf.BaseParser;
import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.reader.Reader;
import com.bloom.source.snmp.SNMPResultSet;
import com.bloom.uuid.UUID;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

@PropertyTemplate(name="SNMPParser", type=AdapterType.parser, properties={@com.bloom.anno.PropertyTemplateProperty(name="alias", type=String.class, required=false, defaultValue="")}, inputType=WAEvent.class)
public class SNMPParser_1_0
  extends BaseParser
{
  protected SNMPResultSet resultSet;
  protected Property property;
  protected CheckpointDetail checkpoint;
  protected UUID srcUUID;
  protected WAEvent event;
  
  public SNMPParser_1_0(Map<String, Object> prop, UUID sourceUUID)
    throws AdapterException
  {
    super(prop, sourceUUID);
    this.srcUUID = sourceUUID;
    this.property = new Property(prop);
  }
  
  public Iterator<Event> parse(Reader in)
    throws Exception
  {
    Reader reader = null;
    if ((in instanceof Reader)) {
      reader = in;
    } else {
      throw new AdapterException("Reader instance is expected but other InputStream instance is passed");
    }
    try
    {
      this.resultSet = new SNMPResultSet(reader, this.property);
      this.resultSet.init();
    }
    catch (IOException|InterruptedException e)
    {
      throw new AdapterException(Error.GENERIC_EXCEPTION, e);
    }
    return this;
  }
  
  public boolean hasNext()
  {
    try
    {
      Constant.recordstatus status = this.resultSet.next();
      if (status == Constant.recordstatus.VALID_RECORD)
      {
        this.checkpoint = this.resultSet.getCheckpointDetail();
        this.event = new WAEvent(1, this.srcUUID);
        this.event.data = new Object[1];
        this.event.data[0] = this.resultSet.getResultAsMap();
        this.event.metadata = this.resultSet.getMetaAsMap();
        return true;
      }
      if ((status == Constant.recordstatus.NO_RECORD) || (status == Constant.recordstatus.INVALID_RECORD)) {
        return false;
      }
      if (status == Constant.recordstatus.ERROR_RECORD) {
        throw new RuntimeException(new RecordException(RecordException.Type.ERROR));
      }
      throw new RuntimeException("Unhandled RecordStatus.");
    }
    catch (IOException|InterruptedException e)
    {
      throw new RuntimeException(e);
    }
  }
  
  public Event next()
  {
    return this.event;
  }
  
  public void close()
    throws AdapterException
  {
    if (this.resultSet != null) {
      this.resultSet.close();
    }
  }
  
  public void remove() {}
}
