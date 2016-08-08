package com.bloom.source.binary;

import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import com.bloom.source.lib.prop.Property;
import java.io.File;
import java.util.Map;

public class BinaryProperty
  extends Property
{
  public String header;
  public String meta_datafile;
  public int endian;
  public boolean encoding;
  public boolean nullterminatedstring;
  public int stringlengthcolumnsize = 0;
  
  public BinaryProperty(Map<String, Object> mp)
    throws AdapterException
  {
    super(mp);
    if (mp.get("wildcard") == null) {
      this.wildcard = "*.bin";
    } else {
      this.wildcard = ((String)mp.get("wildcard"));
    }
    if (mp.get("metadata") == null) {
      throw new AdapterException(Error.INVALID_DIRECTORY, "Please set a valid path");
    }
    this.meta_datafile = ((String)mp.get("metadata"));
    File metaDataDirectory = new File(this.meta_datafile);
    if (!metaDataDirectory.exists()) {
      throw new AdapterException(Error.INVALID_DIRECTORY, "Please set a valid path");
    }
    if (mp.get("endian") != null)
    {
      if (getBoolean(mp, "endian") == true) {
        this.endian = 1;
      } else {
        this.endian = 0;
      }
    }
    else {
      this.endian = 1;
    }
    if (mp.get("encoding") == null) {
      this.encoding = false;
    } else {
      this.encoding = true;
    }
    this.nullterminatedstring = ((Boolean)mp.get("StringTerminatedByNull")).booleanValue();
    if (!this.nullterminatedstring) {
      this.stringlengthcolumnsize = 2;
    }
  }
}
