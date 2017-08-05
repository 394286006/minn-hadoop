package p.minn.fs;

import java.io.InputStream;

import org.apache.hadoop.io.IOUtils;

/**
 * 
 * @author minn
 * @QQ:3942986006
 *
 */
public abstract class FSFileOperation {

  private String output = "output";

  private String input = "input";

  private String prefix = "";

  public String readFileContent(String fileName) throws Exception {
    byte[] data = readFileData(fileName);
    return new String(data);
  }

  public byte[] readFileData(String fileName) throws Exception {
    InputStream in = readDataInputStream(fileName);
    byte[] data = new byte[in.available()];
    IOUtils.readFully(in, data, 0, in.available());
    return data;
  }

  public abstract InputStream readDataInputStream(String fileName) throws Exception;

  public String getOutput() {
    return output;
  }

  public void setOutput(String output) {
    this.output = output;
  }

  public String getInput() {
    return input;
  }

  public void setInput(String input) {
    this.input = input;
  }

  public String getPrefix() {
    return prefix;
  }

  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }


}
