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
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite.converter.IGDocumentPreLib;
import gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite.converter.IGDocumentReadConverterPreLib;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.IGDocument;

public class IGDocumentExporter implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(IGDocumentExporter.class);

	public final static File OUTPUT_DIR = new File(System.getenv("LOCAL_OUT") + "/igDocuments");
	public final static File OUTPUT_DIR_IGAMT = new File(System.getenv("IGAMT_OUT") + "/igDocuments");

	public void run() {
		if (OUTPUT_DIR.exists()) {
			OUTPUT_DIR.delete();
		}
		OUTPUT_DIR.mkdir();

		if (OUTPUT_DIR_IGAMT.exists()) {
			OUTPUT_DIR_IGAMT.delete();
		}
		OUTPUT_DIR_IGAMT.mkdir();
		
		MongoTemplate mongoOps;
		try {
			mongoOps = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "igl"));
			List<IGDocument> igds = mongoOps.findAll(IGDocument.class);
			ObjectMapper mapper = new ObjectMapper();
			for (IGDocument igdocument : igds) {
				String hl7Version = igdocument.getProfile().getMetaData().getHl7Version();
				log.info("hl7Version=" + hl7Version);
				File outfile = new File(OUTPUT_DIR,
						"igdocument-" + hl7Version + "-" + igdocument.getScope().name() + ".json");
				File outfileIGAMT = new File(OUTPUT_DIR_IGAMT,
						"igd-" + igdocument.getScope().name() + "-" + hl7Version + ".json");
				Writer jsonLocal = new FileWriter(outfile);
				mapper.writerWithDefaultPrettyPrinter().writeValue(jsonLocal, igdocument);
				Writer jsonIGAMT = new FileWriter(outfileIGAMT);
				mapper.writerWithDefaultPrettyPrinter().writeValue(jsonIGAMT, igdocument);
			}
		} catch (IOException e) {
			log.error("", e);
		}
	}

	public static void main(String[] args) throws Exception {

		IGDocumentExporter app = new IGDocumentExporter();
		app.run();
	}
}