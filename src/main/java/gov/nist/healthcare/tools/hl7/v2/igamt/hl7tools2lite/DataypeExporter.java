/**
 * This software was developed at the National Institute of Standards and Technology by employees
 * of the Federal Government in the course of their official duties. Pursuant to title 17 Section 105 of the
 * United States Code this software is not subject to copyright protection and is in the public domain.
 * This is an experimental system. NIST assumes no responsibility whatsoever for its use by other parties,
 * and makes no guarantees, expressed or implied, about its quality, reliability, or any other characteristic.
 * We would appreciate acknowledgement if the software is used. This software can be redistributed and/or
 * modified freely provided that any derivative works bear some notice that they are derived from it, and any
 * modified versions bear some notice that they have been modified.
 */
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatype;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.service.converters.DatatypeReadConverter;

public class DataypeExporter implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(DataypeExporter.class);

	public final static File OUTPUT_DIR_LOCAL = new File(System.getenv("LOCAL_OUT") + "/datatypes");
	public final static File OUTPUT_DIR_IGAMT = new File(System.getenv("IGAMT_OUT") + "/datatypes");

	public void run() {

		if (!OUTPUT_DIR_LOCAL .exists()) {
			OUTPUT_DIR_LOCAL .mkdir();
		}

		if (!OUTPUT_DIR_IGAMT .exists()) {
			OUTPUT_DIR_IGAMT .mkdir();
		}
		
		MongoOperations mongoOps;
		mongoOps = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "igl"));
		ObjectMapper mapper = new ObjectMapper();

		Datatype dt;

		DatatypeReadConverter conv = new DatatypeReadConverter();

		DBCollection coll = mongoOps.getCollection("datatype");
		BasicDBObject qry = new BasicDBObject();
		List<BasicDBObject> where = new ArrayList<BasicDBObject>();
		where.add(new BasicDBObject("scope", "HL7STANDARD"));
		qry.put("$and", where);
		DBCursor cur = coll.find(qry);

		while (cur.hasNext()) {
			DBObject obj = cur.next();
			dt = conv.convert(obj);
			String hl7Version = dt.getHl7Version();
			String fName = "datatype-" + dt.getName() + "-" + dt.getScope().name() + "-" + hl7Version + ".json"; 
			File outfileLocal = new File(OUTPUT_DIR_LOCAL , fName);
			File outfileIGAMT = new File(OUTPUT_DIR_IGAMT , fName);
			try {
				Writer jsonLocal = new FileWriter(outfileLocal);
				mapper.writerWithDefaultPrettyPrinter().writeValue(jsonLocal, dt);
				Writer jsonIGAMT = new FileWriter(outfileIGAMT);
				mapper.writerWithDefaultPrettyPrinter().writeValue(jsonIGAMT, dt);
			} catch (IOException e) {
				log.error("", e);
			}
		}
	}

	public static void main(String[] args) throws Exception {

		DataypeExporter app = new DataypeExporter();
		app.run();
	}
}