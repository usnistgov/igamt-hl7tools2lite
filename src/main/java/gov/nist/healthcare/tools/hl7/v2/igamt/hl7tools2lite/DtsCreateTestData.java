package gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLink;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.service.converters.DatatypeLibraryReadConverter;

public class DtsCreateTestData implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(DtsCreateTestData.class);

	public final static File OUTPUT_DIR_LOCAL = new File(System.getenv("LOCAL_OUT") + "/datatypeLibraries");
	public final static File OUTPUT_DIR_IGAMT = new File(System.getenv("IGAMT_OUT") + "/datatypeLibraries");

	@Override
	public void run() {

		if (!OUTPUT_DIR_LOCAL .exists()) {
			OUTPUT_DIR_LOCAL .mkdir();
		}

		if (!OUTPUT_DIR_IGAMT .exists()) {
			OUTPUT_DIR_IGAMT .mkdir();
		}
		
		MongoOperations mongoOps;
		try {
			mongoOps = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "igl"));
			ObjectMapper mapper = new ObjectMapper();

			DatatypeLibraryReadConverter conv = new DatatypeLibraryReadConverter();

			DBCollection coll = mongoOps.getCollection("datatype-library");
			BasicDBObject qry = new BasicDBObject();
			List<BasicDBObject> where = new ArrayList<BasicDBObject>();
			where.add(new BasicDBObject("scope", "HL7STANDARD"));
			where.add(new BasicDBObject("metaData.hl7Version", "2.5.1"));
			qry.put("$and", where);
			DBCursor curr = coll.find(qry);

			DatatypeLibrary lib = null;
			while (curr.hasNext()) {
				DBObject obj = curr.next();
				lib = conv.convert(obj);
			}

			lib.setId(null);
			lib.setScope(Constant.SCOPE.MASTER);

			String hl7Version = lib.getMetaData().getHl7Version();
			int i = 0;
			Set<DatatypeLink> temp = new HashSet<DatatypeLink>();
			for (DatatypeLink dt : lib.getChildren()) {
				if (i < 5) {
					temp.add(dt);
				}
				i++;
			}
			lib.setChildren(temp);
			mongoOps.insert(lib);

			File outfileLocal = new File(OUTPUT_DIR_LOCAL,
					"dtLib-" + lib.getScope().name() + "-" + hl7Version + ".json");
			File outfileIGAMT = new File(OUTPUT_DIR_IGAMT,
					"dtLib-" + lib.getScope().name() + "-" + hl7Version + ".json");
			Writer jsonLocal = new FileWriter(outfileLocal);
			mapper.writerWithDefaultPrettyPrinter().writeValue(jsonLocal, lib);
			Writer jsonIGAMT = new FileWriter(outfileIGAMT);
			mapper.writerWithDefaultPrettyPrinter().writeValue(jsonIGAMT, lib);
		} catch (JsonGenerationException e) {
			log.error("", e);
		} catch (JsonMappingException e) {
			log.error("", e);
		} catch (IOException e) {
			log.error("", e);
		}
	}

	public static void main(String[] args) {
		DtsCreateTestData app = new DtsCreateTestData();
		app.run();
	}
}
