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
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Component;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Constant;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatype;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLibraryMetaData;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatypes;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Table;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Tables;

public class DataypeExporter implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(DataypeExporter.class);

	public final static File OUTPUT_DIR = new File(System.getenv("LOCAL_OUT") + "/datatypeLibraries");
	public final static File OUTPUT_DIR_DTS = new File(OUTPUT_DIR, "datatypes");

	public void run() {

		if (!OUTPUT_DIR.exists()) {
			OUTPUT_DIR.mkdir();
		}

		if (!OUTPUT_DIR_DTS.exists()) {
			OUTPUT_DIR_DTS.mkdir();
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
			Datatypes dts = igd.getProfile().getDatatypes();
			Tables tabs = igd.getProfile().getTables();
			DatatypeLibrary dtLib = new DatatypeLibrary();
			dtLib.setScope(Constant.SCOPE.HL7STANDARD);
			dtLib.setMetaData(createMetaData(dtLib.getId(), hl7Version));

			for (Datatype dt : dts.getChildren()) {
				dt.setHl7Version(igd.getProfile().getMetaData().getHl7Version());
				dt.setScope(Constant.SCOPE.HL7STANDARD);
				dt.setStatus(Datatype.STATUS.UNPUBLISHED);
				for (Component cpt : dt.getComponents()) {
					String tabId = cpt.getTable();
					if (tabId != null && !"".equals(tabId)) {
						Table tab = tabs.findOneTableById(tabId);
						if (tab != null) {
							dtLib.addTable(tab);
						} else {
							log.error("Table not found version.dt.cpt=" + hl7Version + "." + dt.getName() + "." + cpt.getName());
						}
					}
				}
				dtLib.addDatatype(dt);
				File outfile = new File(OUTPUT_DIR_DTS, dt.getName() + "-" + hl7Version + "-" + dtLib.getScope().name() + ".json");
				try {
					Writer dtJson = new FileWriter(outfile);
					mapper.writerWithDefaultPrettyPrinter().writeValue(dtJson, dt);
				} catch (IOException e) {
					log.error("", e);
				}
			}
			File outfileLib = new File(OUTPUT_DIR, "dtLib-" + hl7Version + "-" + dtLib.getScope().name() + ".json");
			try {
				Writer dtLibJson = new FileWriter(outfileLib);
				mapper.writerWithDefaultPrettyPrinter().writeValue(dtLibJson, dtLib);
			} catch (IOException e) {
				log.error("", e);
			}
		}
	}

	DatatypeLibraryMetaData createMetaData(String libId, String hl7version) {
		DatatypeLibraryMetaData meta = new DatatypeLibraryMetaData();
		meta.setDatatypLibId(libId);
		meta.setDate(Constant.mdy.format(new Date()));
		meta.setName(null);
		meta.setOrgName("NIST");
		meta.setVersion(null);
		meta.setHl7Version(hl7version);
		return meta;
	}

	public static void main(String[] args) throws Exception {

		DataypeExporter app = new DataypeExporter();
		app.run();
	}
}