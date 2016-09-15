package bumjoon;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.regex.Pattern;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import org.apache.log4j.Logger;

public class SimpleScheme implements Scheme {

	private static final long serialVersionUID = -2635855902686304014L;
	private static final Logger LOG = Logger.getLogger(SimpleScheme.class);

	private static Pattern PIPE_PATTERN = Pattern.compile("\\|");
	
	private static int count = 0;
	
	public List<Object> deserialize(byte[] bytes) {
		LOG.error("bkimtest : deserialize() is invoked. count=" + ++count);

		String line = null;
		try {
			line = new String(bytes, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			LOG.error(e.getLocalizedMessage(), e);
			throw new RuntimeException(e);
		}
		
		String[] pieces = PIPE_PATTERN.split(line, 8);
		if (pieces.length >= 4) {
			return new Values(pieces[0], pieces[1], pieces[2], pieces[3]);
		}
		return new Values("", "", "", "");
	}

	public Fields getOutputFields() {
		return new Fields("A", "B", "C", "D");
	}
}
