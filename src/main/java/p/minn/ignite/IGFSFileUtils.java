package p.minn.ignite;

import java.io.IOException;
import java.io.InputStream;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.IgfsPath;

import p.minn.fs.FSFileOperation;


/**
 * 
 * @author minn
 * @QQ:3942986006
 *
 */
public class IGFSFileUtils extends FSFileOperation {

  private IgniteFileSystem ifs;

  private Ignite ignite;

  public IGFSFileUtils(IgniteConfiguration igniteConfiguration, String name) throws IOException {
    this.ignite = Ignition.getOrStart(igniteConfiguration);
    this.ifs = ignite.fileSystem(name);
  }

  public InputStream readDataInputStream(String fileName) throws Exception {
    IgfsPath path = new IgfsPath(this.getPrefix() + this.getInput() + fileName);
    IgfsInputStream in = ifs.open(path);
    return in;
  }

}
