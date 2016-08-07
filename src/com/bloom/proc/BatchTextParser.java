package com.bloom.proc;


import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.bloom.event.Event;
import com.bloom.event.SimpleEvent;
import com.bloom.proc.events.JsonNodeEvent;
import com.bloom.proc.events.StringArrayEvent;
import com.bloom.proc.events.WAEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class BatchTextParser
{
  public static enum Format
  {
    CSV,  JSON;
    
    private Format() {}
  }
  
  public static Format resolveFormat(String data)
  {
    Pattern p = Pattern.compile("^[\\s-]*\\{");
    if (p.matcher(data).find()) {
      return Format.JSON;
    }
    return Format.CSV;
  }
  
  public static List<List<JsonNode>> parse(String text, Format format)
    throws Exception
  {
    List<String> batches = parseBatches(text);
    Parser p;
    switch (format)
    {
    case CSV: 
      p = new CSVBatchParser(null); break;
    case JSON: 
      p = new JSONBatchParser(null); break;
    default: 
      if (!$assertionsDisabled) {
        throw new AssertionError();
      }
      p = null;
    }
    List<List<JsonNode>> ret = new ArrayList();
    for (String batchText : batches)
    {
      List<JsonNode> l = p.parse(batchText);
      ret.add(l);
    }
    return ret;
  }
  
  private static List<String> parseBatches(String data)
  {
    List<String> ret = new ArrayList();
    StringBuilder b = new StringBuilder();
    for (String line : data.split("\\r?\\n")) {
      if (line.startsWith("---"))
      {
        ret.add(b.toString());
        b = new StringBuilder();
      }
      else
      {
        b.append(line).append('\n');
      }
    }
    if (b.length() > 0) {
      ret.add(b.toString());
    }
    return ret;
  }
  
  private static abstract interface Parser
  {
    public abstract List<JsonNode> parse(String paramString);
  }
  
  private static class CSVBatchParser
    implements BatchTextParser.Parser
  {
    public List<JsonNode> parse(String batchText)
    {
      try
      {
        CSVParser p = CSVParser.parse(batchText, CSVFormat.DEFAULT);
        List<JsonNode> ret = new ArrayList();
        for (CSVRecord rec : p.getRecords())
        {
          ArrayNode array = JsonNodeFactory.instance.arrayNode();
          int size = rec.size();
          for (int i = 0; i < size; i++) {
            array.add(rec.get(i));
          }
          ret.add(array);
        }
        return ret;
      }
      catch (IOException e)
      {
        throw new RuntimeException(e);
      }
    }
  }
  
  private static class JSONBatchParser
    implements BatchTextParser.Parser
  {
    public List<JsonNode> parse(String batchText)
    {
      try
      {
        JsonNode batch = new ObjectMapper().readTree("[" + batchText + "]");
        if (!batch.isArray()) {
          throw new RuntimeException("invalid json batch\n" + batchText);
        }
        List<JsonNode> ret = new ArrayList();
        for (JsonNode data : batch) {
          ret.add(data);
        }
        return ret;
      }
      catch (IOException e)
      {
        throw new RuntimeException(e);
      }
    }
  }
  
  private static ArrayNode arrayToJSON(Object[] array)
  {
    ArrayNode ret = JsonNodeFactory.instance.arrayNode();
    for (Object o : array) {
      ret.add(o.toString());
    }
    return ret;
  }
  
  public static JsonNode eventToJSON(Event event)
  {
    if ((event instanceof JsonNodeEvent)) {
      return ((JsonNodeEvent)event).getData();
    }
    if ((event instanceof StringArrayEvent)) {
      return arrayToJSON(((StringArrayEvent)event).getData());
    }
    if ((event instanceof WAEvent)) {
      return arrayToJSON(((WAEvent)event).data);
    }
    if (event.getClass() == SimpleEvent.class)
    {
      SimpleEvent e = (SimpleEvent)event;
      if (e.payload != null) {
        return arrayToJSON(e.payload);
      }
    }
    ObjectMapper m = new ObjectMapper();
    m.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
    m.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.PUBLIC_ONLY);
    return m.valueToTree(event);
  }
}
