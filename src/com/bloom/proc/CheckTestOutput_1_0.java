package com.bloom.proc;


import com.fasterxml.jackson.databind.JsonNode;
import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.event.Event;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.WAEvent;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@PropertyTemplate(name="CheckTestOutput", type=AdapterType.internal, properties={@com.bloom.anno.PropertyTemplateProperty(name="data", type=String.class, required=true, defaultValue="")}, outputType=Event.class)
public class CheckTestOutput_1_0
  extends BaseProcess
{
  private Iterator<List<JsonNode>> inputIter;
  
  public void init(Map<String, Object> properties)
    throws Exception
  {
    super.init(properties);
    String data = (String)properties.get("data");
    BatchTextParser.Format format = BatchTextParser.resolveFormat(data);
    List<List<JsonNode>> inputEvents = BatchTextParser.parse(data, format);
    this.inputIter = inputEvents.iterator();
  }
  
  public void close()
    throws Exception
  {
    super.close();
  }
  
  public void receiveImpl(int channel, Event event)
    throws Exception
  {
    throw new UnsupportedOperationException();
  }
  
  private List<JsonNode> convertBatchToJson(IBatch batch)
  {
    List<JsonNode> ret = new ArrayList();
    IBatch<WAEvent> waEventIBatch = batch;
    for (WAEvent e : waEventIBatch)
    {
      JsonNode n = BatchTextParser.eventToJSON((Event)e.data);
      ret.add(n);
    }
    return ret;
  }
  
  private void error(String messaage)
  {
    System.out.println(messaage);
  }
}
