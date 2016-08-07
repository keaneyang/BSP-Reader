package com.bloom.proc;


import com.fasterxml.jackson.databind.JsonNode;
import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.event.Event;
import com.bloom.proc.events.JsonNodeEvent;
import com.bloom.runtime.containers.TaskEvent;
import com.bloom.runtime.containers.WAEvent;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@PropertyTemplate(name="TestInput", type=AdapterType.internal, properties={@com.bloom.anno.PropertyTemplateProperty(name="data", type=String.class, required=true, defaultValue="")}, inputType=JsonNodeEvent.class)
public class TestInput_1_0
  extends SourceProcess
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
  
  public void receiveImpl(int channel, Event event)
    throws Exception
  {
    if (this.inputIter.hasNext())
    {
      List<JsonNode> l = (List)this.inputIter.next();
      super.send(makeTaskEvent(l));
    }
  }
  
  public void close()
    throws Exception
  {
    super.close();
  }
  
  private TaskEvent makeTaskEvent(List<JsonNode> batch)
  {
    long timestamp = System.currentTimeMillis();
    List<WAEvent> added = new ArrayList();
    for (JsonNode o : batch)
    {
      JsonNodeEvent event = new JsonNodeEvent(timestamp);
      event.setData(o);
      added.add(new WAEvent(event));
    }
    return TaskEvent.createStreamEvent(added);
  }
}

