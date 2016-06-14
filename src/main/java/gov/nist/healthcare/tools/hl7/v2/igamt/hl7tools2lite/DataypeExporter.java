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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;

import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatype;

public class DataypeExporter implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(DataypeExporter.class);

	public final static File OUTPUT_DIR_LOCAL = new File(System.getenv("LOCAL_OUT") + "/datatypes");
	public final static File OUTPUT_DIR_IGAMT = new File(System.getenv("IGAMT_OUT") + "/datatypes");

	public void run() {

		if (!OUTPUT_DIR_LOCAL.exists()) {
			OUTPUT_DIR_LOCAL.mkdir();
		}

		if (!OUTPUT_DIR_IGAMT.exists()) {
			OUTPUT_DIR_IGAMT.mkdir();
		}

		MongoTemplate mongoOps;
		mongoOps = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "igl"));
		Criteria where = Criteria.where("scope").is("HL7STANDARD");
		Query qry = Query.query(where);
		qry.with(new Sort(Sort.Direction.ASC, "hl7Version"));

		ObjectMapper mapper = new ObjectMapper();

		List<Datatype> dts = mongoOps.find(qry, Datatype.class);
		String hl7VersionOld = null;
		String hl7Version = null;
		StringBuilder bld = new StringBuilder();
		bld.append("[");

		for (Datatype dt : dts) {
			hl7Version = dt.getHl7Version();
			hl7VersionOld = hl7VersionOld == null ? hl7Version : hl7VersionOld;
			if (hl7Version.equals(hl7VersionOld)) {
				String fName = "datatype-" + dt.getName() + "-" + dt.getScope().name() + "-" + hl7Version + ".json";
				File outfileLocal = new File(OUTPUT_DIR_LOCAL, fName);
				File outfileIGAMT = new File(OUTPUT_DIR_IGAMT, fName);
				try {
					Writer jsonLocal = new FileWriter(outfileLocal);
					mapper.writerWithDefaultPrettyPrinter().writeValue(jsonLocal, dt);
					Writer jsonIGAMT = new FileWriter(outfileIGAMT);
					mapper.writerWithDefaultPrettyPrinter().writeValue(jsonIGAMT, dt);
					Writer jsonCollection = new StringWriter();
					mapper.writerWithDefaultPrettyPrinter().writeValue(jsonCollection, dt);
					bld.append(jsonCollection.toString());
					bld.append(",");

				} catch (IOException e) {
					log.error("", e);
				}
			} else {
				writeCollection(hl7VersionOld, bld);
				hl7VersionOld = hl7Version;
				bld = new StringBuilder();
				bld.append("[");
			}
		}
		writeCollection(hl7Version, bld);
	}

	void writeCollection(String hl7Version, StringBuilder bld) {
		int pos = bld.lastIndexOf(",");
		bld.delete(pos, pos + 1);
		bld.append("]");
		String fNameColl = "datatypes-" + "HL7STANDARD-" + hl7Version + ".json";
		File outfileLocal = new File(OUTPUT_DIR_LOCAL, fNameColl);
		File outfileIGAMT = new File(OUTPUT_DIR_IGAMT, fNameColl);
		BufferedWriter writeCollLocal = null;
		BufferedWriter writeCollIGAMT = null;
		try {
			writeCollLocal = new BufferedWriter(new FileWriter(outfileLocal));
			writeCollIGAMT = new BufferedWriter(new FileWriter(outfileIGAMT));
			writeCollLocal.write(bld.toString());
			writeCollIGAMT.write(bld.toString());
			writeCollLocal.close();
			writeCollIGAMT.close();
		} catch (IOException e) {
			log.error("", e);
		}
	}

	public static void main(String[] args) throws Exception {

		DataypeExporter app = new DataypeExporter();
		app.run();
	}
}