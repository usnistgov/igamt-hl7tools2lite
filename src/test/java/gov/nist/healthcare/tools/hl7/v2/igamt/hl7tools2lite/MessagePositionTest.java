package gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import com.mongodb.MongoClient;

import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Field;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.IGDocument;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.IGDocumentScope;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Message;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Messages;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Segment;

public class MessagePositionTest {

	Logger log = LoggerFactory.getLogger(MessagePositionTest.class);
	static MongoClient mongo;
	static MongoOperations mongoOps = null;
	String hl7Version = "2.7.1";

	@BeforeClass
	public static void init() {
		mongo = new MongoClient("localhost", 27017);
		mongoOps = new MongoTemplate(new SimpleMongoDbFactory(mongo, "igamt1"));

	}
	
	@Test
	public void testMessagePosition() {
	    Criteria where =
	            Criteria.where("scope").is(IGDocumentScope.HL7STANDARD)
	                .andOperator(Criteria.where("profile.metaData.hl7Version").is(hl7Version));
	    Query query = Query.query(where);
	    List<IGDocument> igds = mongoOps.find(query, IGDocument.class);
	    IGDocument igd = igds.get(0);
	    Messages msgs  = igd.getProfile().getMessages();
	    List<Integer> poss = new ArrayList<Integer>();
	    for (Message msg : msgs.getChildren()) {
	    	poss.add(msg.getPosition());
	    }
	    assertFalse(poss.contains(0));
	    Collections.sort(poss);
    	log.info("poss=" + poss);
	}
	
	@Test
	public void testConfLength() {
	    Criteria where =
	            Criteria.where("scope").is(IGDocumentScope.HL7STANDARD)
	                .andOperator(Criteria.where("hl7Version").is(hl7Version));
	    Query query = Query.query(where);
	    List<Segment> segs = mongoOps.find(query, Segment.class);
	    for (Segment seg : segs) {
	    	for (Field fld : seg.getFields()) {
	    		Integer confLength = new Integer(fld.getConfLength());
	    		assertTrue(confLength > 0);
	    	}
	    }
	}
	
//	@Test
	public void testExpMessagePosition() {
		final String fileName = "eximfiles/v2.8.2/message-expimp.json";
		String msgs = null;
		try {
			msgs = readFileFromClasspath(fileName);
			msgs.replaceAll("\\R", ",");
		} catch (IOException | URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		StringBuilder json = new StringBuilder();
		json.append("[");
		json.append(msgs);
		int pos = json.lastIndexOf(",");
		json.replace(pos, pos + 1, "");
		json.append("]");
		ReadContext ctx = JsonPath.parse(json.toString());
		List<String> poss = ctx.read("$@position == 0");
		log.info("poss=" + poss);
	}
	
	public String readFileFromClasspath(final String fileName) throws IOException, URISyntaxException {
	    return new String(Files.readAllBytes(
	                Paths.get(getClass().getClassLoader()
	                        .getResource(fileName)
	                        .toURI())));
	}
}
