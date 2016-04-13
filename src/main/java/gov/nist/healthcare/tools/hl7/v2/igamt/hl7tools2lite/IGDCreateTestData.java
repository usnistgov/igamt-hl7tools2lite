package gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.IGDocument;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.messageevents.MessageEvents;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.service.IGDocumentCreationService;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.service.IGDocumentException;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.service.test.integration.PersistenceContext;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {PersistenceContext.class})
public class IGDCreateTestData implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(IGDCreateTestData.class);

	public final static File OUTPUT_DIR = new File(System.getenv("IGAMT") + "/igDocuments");

	@Autowired
	IGDocumentCreationService create;

	@Override
	public void run() {
		if (!OUTPUT_DIR.exists()) {
			OUTPUT_DIR.mkdir();
		}
		MongoOperations mongoOps;
		try {
//			mongoOps = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "igl"));
//
//			IGDocumentReadConverter conv = new IGDocumentReadConverter();
//			ReferentialIntegrityTest ref = new ReferentialIntegrityTest();
//			
//			DBCollection coll = mongoOps.getCollection("igdocument");
//			BasicDBObject qry = new BasicDBObject();
//			List<BasicDBObject> where = new ArrayList<BasicDBObject>();
//			where.add(new BasicDBObject("scope", "HL7STANDARD"));
//			where.add(new BasicDBObject("profile.metaData.hl7Version", "2.7"));
//			qry.put("$and", where);			
//			DBCursor cur = coll.find(qry);
//
//			IGDocument igDocumentSource = null;
			IGDocument igDocumentTarget = null;
//			while (cur.hasNext()) {
//				DBObject obj = cur.next();
//				igDocumentSource = conv.convert(obj);
//			}
//			
//			Set<Message> msgs = igDocumentSource.getProfile().getMessages().getChildren();
//			int limit = msgs.size();
//			Message[] msgsArr = msgs.toArray(new Message[limit]);
			List<MessageEvents> msgEvts = new ArrayList<MessageEvents>();
//			for (int i = 0; i < 5; i++) {
//				msgIds.add(msgsArr[randInt(0, limit)].getId());
//			} d D d
			igDocumentTarget = create.createIntegratedIGDocument(msgEvts, "2.7", 45L);
//			ref.account4Segments(igDocumentTarget.getProfile());
//			ref.account4DataTypes(igDocumentTarget.getProfile());
//			ref.account4ValueSets(igDocumentTarget.getProfile());
			File outfile = new File(OUTPUT_DIR, "igdocument-" + "2.7.5" + "-" + igDocumentTarget.getScope().name() + "-" + igDocumentTarget.getMetaData().getVersion() + ".json"); 
			Writer igdocumentJson = new FileWriter(outfile);
			ObjectMapper mapper = new ObjectMapper();
			mapper.writerWithDefaultPrettyPrinter().writeValue(igdocumentJson, igDocumentTarget);
		} catch (JsonGenerationException e) {
			log.error("" , e);
		} catch (JsonMappingException e) {
			log.error("" , e);
		} catch (IOException e) {
			log.error("" , e);
		} catch (IGDocumentException e) {
			log.error("" , e);
		}
	}
	
	public static int randInt(int min, int max) {

	    // Usually this can be a field rather than a method variable
	    Random rand = new Random();

	    // nextInt is normally exclusive of the top value,
	    // so add 1 to make it inclusive
	    int randomNum = rand.nextInt((max - min)) + min;

	    return randomNum;
	}
	
	public static void main(String[] args) {
		IGDCreateTestData app = new IGDCreateTestData();
		app.run();
	}

}
