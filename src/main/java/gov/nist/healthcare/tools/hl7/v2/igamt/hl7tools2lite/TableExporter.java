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
import java.util.Date;
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

import gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite.converter.IGDocumentPreLib;
import gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite.converter.IGDocumentReadConverterPreLib;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Constant;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Table;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.TableLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.TableLibraryMetaData;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Tables;

public class TableExporter implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(TableExporter.class);

	public final static File OUTPUT_DIR = new File(System.getenv("LOCAL_OUT") + "/tableLibraries");
	public final static File OUTPUT_DIR_TAB = new File(OUTPUT_DIR, "tables");

	public void run() {
		if (!OUTPUT_DIR.exists()) {
			OUTPUT_DIR.mkdir();
		}

		if (!OUTPUT_DIR_TAB.exists()) {
			OUTPUT_DIR_TAB.mkdir();
		}

		MongoOperations mongoOps;
		mongoOps = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "igl"));
		ObjectMapper mapper = new ObjectMapper();

		IGDocumentPreLib igd;

		IGDocumentReadConverterPreLib convIGD = new IGDocumentReadConverterPreLib();

		DBCollection coll = mongoOps.getCollection("igdocumentPreLib");
		BasicDBObject qry = new BasicDBObject();
		List<BasicDBObject> where = new ArrayList<BasicDBObject>();
		where.add(new BasicDBObject("scope", "HL7STANDARD"));
		qry.put("$and", where);
		DBCursor cur = coll.find(qry);

		while (cur.hasNext()) {
			DBObject obj = cur.next();
			igd = convIGD.convert(obj);
			String hl7Version = igd.getProfile().getMetaData().getHl7Version();
			log.info("hl7Version=" + hl7Version);
			Tables tabs = igd.getProfile().getTables();
			TableLibrary tabLib = new TableLibrary();
			tabLib.setScope(Constant.SCOPE.HL7STANDARD);
			tabLib.setMetaData(createMetaData(tabLib.getId(), hl7Version));

			for (Table tab : tabs.getChildren()) {
				tab.setHl7Version(igd.getProfile().getMetaData().getHl7Version());
				tab.setScope(Constant.SCOPE.HL7STANDARD);
				tabLib.addTable(tab);
				File outfile = new File(OUTPUT_DIR_TAB,
						tab.getBindingIdentifier() + "-" + hl7Version + "-" + tabLib.getScope().name() + ".json");
				try {
					Writer tabJson = new FileWriter(outfile);
					mapper.writerWithDefaultPrettyPrinter().writeValue(tabJson, tab);
				} catch (IOException e) {
					log.error("", e);
				}
			}
			File outfileLib = new File(OUTPUT_DIR, "tabLib-" + hl7Version + "-" + tabLib.getScope().name() + ".json");
			try {
				Writer tabLibJson = new FileWriter(outfileLib);
				mapper.writerWithDefaultPrettyPrinter().writeValue(tabLibJson, tabLib);
			} catch (IOException e) {
				log.error("", e);
			}
		}
	}

	TableLibraryMetaData createMetaData(String libId, String hl7version) {
		TableLibraryMetaData meta = new TableLibraryMetaData();
		meta.setTableLibId(libId);
		meta.setDate(Constant.mdy.format(new Date()));
		meta.setName(null);
		meta.setOrgName("NIST");
		meta.setVersion(null);
		meta.setHl7Version(hl7version);
		return meta;
	}

	public static void main(String[] args) throws Exception {

		TableExporter app = new TableExporter();
		app.run();
	}
}