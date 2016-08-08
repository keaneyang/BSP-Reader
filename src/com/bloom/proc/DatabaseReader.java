package com.bloom.proc;

import com.bloom.intf.SourceMetadataProvider;
import com.bloom.proc.events.WAEvent;
import com.bloom.uuid.UUID;
import java.sql.SQLException;
import java.util.Map;
import java.util.NoSuchElementException;

public abstract interface DatabaseReader
  extends SourceMetadataProvider
{
  public abstract void initDR(Map<String, Object> paramMap)
    throws Exception;
  
  public abstract WAEvent nextEvent(UUID paramUUID)
    throws NoSuchElementException, SQLException;
  
  public abstract String getCurrentTableName();
  
  public abstract void close()
    throws SQLException;
}
