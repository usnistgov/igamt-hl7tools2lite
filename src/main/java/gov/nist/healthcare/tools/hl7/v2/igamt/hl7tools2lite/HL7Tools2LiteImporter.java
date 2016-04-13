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
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.IGDocument;

public class HL7Tools2LiteImporter implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(HL7Tools2LiteImporter.class);

	public final static File INPUT_DIR = new File(System.getenv("IGAMT") + "/igDocuments/hl7");

	public void run() {
		if (!INPUT_DIR.exists()) {
			INPUT_DIR.mkdir();
		}
		MongoOperations mongoOps;
		try {
			mongoOps = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "igl"));
			ObjectMapper mapper = new ObjectMapper();

			IGDocument igdocument = null;

			File[] files = INPUT_DIR.listFiles();
			for (File infile : files) {
				Reader igdocumentJson = new FileReader(infile);
				igdocument = mapper.readValue(igdocumentJson, IGDocument.class);
				mongoOps.insert(igdocument);
			}
		} catch (IOException e) {
			log.error("", e);
		}
	}

	public static void main(String[] args) throws Exception {

		HL7Tools2LiteImporter app = new HL7Tools2LiteImporter();
		app.run();
	}
}