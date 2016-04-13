package gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Constant;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatype;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatypes;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.IGDocument;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.service.converters.IGDocumentReadConverter;

public class DtsCreateTestData implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(DtsCreateTestData.class);

	public final static File OUTPUT_DIR = new File(System.getenv("IGAMT") + "/datatypes");

	@Override
	public void run() {
		if (!OUTPUT_DIR.exists()) {
			OUTPUT_DIR.mkdir();
		}
		MongoOperations mongoOps;
		try {
			mongoOps = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "igl"));
			ObjectMapper mapper = new ObjectMapper();

			IGDocumentReadConverter conv = new IGDocumentReadConverter();
			
			DBCollection coll = mongoOps.getCollection("igdocument");
			BasicDBObject qry = new BasicDBObject();
			List<BasicDBObject> where = new ArrayList<BasicDBObject>();
			where.add(new BasicDBObject("scope", "HL7STANDARD"));
			where.add(new BasicDBObject("profile.metaData.hl7Version", "2.5.1"));
			qry.put("$and", where);			
			DBCursor curr = coll.find(qry);

			IGDocument igdocument = null;
			while (curr.hasNext()) {
				DBObject obj = curr.next();
				igdocument = conv.convert(obj);
			}
			
			DatatypeLibrary source = igdocument.getProfile().getDatatypeLibrary();
			DatatypeLibrary dtLib = new DatatypeLibrary();
			dtLib.setScope(Constant.SCOPE.HL7STANDARD);
			
			for (Datatype dt : source.getChildren()) {
				dtLib.addDatatype(dt);
				mongoOps.insert(dt);
			}
			mongoOps.insert(dtLib);

			File outfile = new File(OUTPUT_DIR, "datatypes-library.json"); 
			Writer targetJson = new FileWriter(outfile);
			mapper.writerWithDefaultPrettyPrinter().writeValue(targetJson, dtLib);
		} catch (JsonGenerationException e) {
			log.error("" , e);
		} catch (JsonMappingException e) {
			log.error("" , e);
		} catch (IOException e) {
			log.error("" , e);
		}
	}
	
	public static void main(String[] args) {
		DtsCreateTestData app = new DtsCreateTestData();
		app.run();
	}
}
