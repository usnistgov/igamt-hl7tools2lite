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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite.converter.IGDocumentReadConverterPreLib;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Component;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatype;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLink;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Field;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.IGDocument;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Message;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Messages;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Profile;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Segment;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentLink;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentRef;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Table;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.TableLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.TableLink;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.service.converters.IGDocumentReadConverter;

/**
 * @author gcr1
 *
 */
public class ReferentialIntegrityTest {

	private static final Logger log = LoggerFactory.getLogger(ReferentialIntegrityTest.class);
	MongoOperations mongoOps;

	public boolean account4Messages(Profile profile) {
		TableLibrary tables = profile.getTableLibrary();
		Messages messages = profile.getMessages();
		int empty = 0;
		int found = 0;
		List<Table> notFound = new ArrayList<Table>();
		for (Message msg : messages.getChildren()) {
			String s = msg.getStructID();
			if (s != null) {
				Criteria where = Criteria.where("id").is(s);
				Query qry = new Query(where);
				Table tab = mongoOps.findOne(qry, Table.class);
				if (tab != null) {
					found++;
				} else {
					notFound.add(tab);
				}
			} else {
				empty++;
			}
		}
		System.out.println("Messages2Tables");
		System.out.println("empty structIds=" + empty);
		System.out.println("Found table in message=" + found);
		System.out.println("Not Found table in message=" + notFound.size());
		return true;
	}

	public boolean xCheckLib(SegmentLibrary lib, MongoOperations mongoOps) {
		boolean b = true;
		for (SegmentLink ref : lib.getChildren()) {
			b = b && (mongoOps.findById(ref.getId(), Segment.class) != null);
		}
		return b;
	}

	public boolean xCheckLib(DatatypeLibrary lib, MongoOperations mongoOps) {
		boolean b = true;
		for (DatatypeLink ref : lib.getChildren()) {
			b = b && (mongoOps.findById(ref.getId(), Datatype.class) != null);
		}
		return b;
	}

	public boolean xCheckLib(TableLibrary lib, MongoOperations mongoOps) {
		boolean b = true;
		for (TableLink ref : lib.getChildren()) {
			b = b && (mongoOps.findById(ref.getId(), Table.class) != null);
		}
		return b;
	}

	public boolean account4ValueSets(Profile profile, MongoOperations mongoOps) {

		DatatypeLibrary dtLib = profile.getDatatypeLibrary();
		Criteria where = Criteria.where("libIds").in(dtLib.getId());
		Query qry = new Query(where);
		List<Datatype> dts = mongoOps.find(qry, Datatype.class);
		Set<String> dtTabIds = new HashSet<String>();
		int empty = 0;
		for (Datatype dt : dts) {
			for (Component cpt : UtilHL7Tools2Lite.getComponents(dt)) {
				String s = cpt.getTable();
				if (s != null) {
					dtTabIds.add(s);
				} else {
					empty++;
				}
			}
		}
		Criteria where1 = Criteria.where("id").in(dtTabIds);
		Query qry1 = new Query(where1);
		List<Table> tabs = mongoOps.find(qry1, Table.class);

		System.out.println("Datatypes2Tables");
		System.out.println("empty Component.getTable()=" + empty);
		System.out.println("Datatypes.tables=" + dtTabIds.size());
		System.out.println("Tables=" + tabs.size());
		return dtTabIds.size() == tabs.size();
	}

	boolean account4DataTypes(Profile profile, MongoOperations mongoOps) {
		SegmentLibrary segLib = profile.getSegmentLibrary();
		Criteria where = Criteria.where("libIds").in(segLib.getId());
		Query qry = new Query(where);
		List<Segment> segs = mongoOps.find(qry, Segment.class);
		Set<String> segDtIds = new HashSet<String>();
		int empty = 0;
		for (Segment seg : segs) {
			for (Field fld : UtilHL7Tools2Lite.getFields(seg)) {
				String s = fld.getDatatype();
				if (s != null) {
					segDtIds.add(s);
				} else {
					empty++;
				}
			}
		}
		Criteria where1 = Criteria.where("id").in(segDtIds);
		Query qry1 = new Query(where1);
		List<Datatype> dts = mongoOps.find(qry1, Datatype.class);

		System.out.println("Segments2Datatypes");
		System.out.println("empty Field.getDatatype()=" + empty);
		System.out.println("Segments.datatypes=" + segDtIds.size());
		System.out.println("Datatypes=" + dts.size());
		return segDtIds.size() == dts.size();
	}

	public boolean account4Segments(Profile profile, MongoOperations mongoOps) {
		Messages msgs = profile.getMessages();
		Set<String> msgSegIds = new HashSet<String>();
		int empty = 0;
		for (Message msg : msgs.getChildren()) {
			for (SegmentRef ref : UtilHL7Tools2Lite.getMessageRefs(msg)) {
				if (ref != null) {
					msgSegIds.add(ref.getRef());
				} else {
					empty++;
				}
			}
		}
		Criteria where1 = Criteria.where("id").in(msgSegIds);
		Query qry1 = new Query(where1);
		List<Segment> segs = mongoOps.find(qry1, Segment.class);

		System.out.println("Messages2Segments");
		System.out.println("empty SegmentRef=" + empty);
		System.out.println("Messages.segmentRefs=" + msgSegIds.size());
		System.out.println("Datatypes=" + segs.size());
		return msgSegIds.size() == segs.size();
	}

	public void run() {

		try {
			mongoOps = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "igl"));
			ObjectMapper mapper = new ObjectMapper();

			IGDocumentReadConverter conv = new IGDocumentReadConverter();
			IGDocumentReadConverterPreLib convPreLib = new IGDocumentReadConverterPreLib();
			IGDocument igdocument = null;

			DBCollection coll = mongoOps.getCollection("igdocument");
			BasicDBObject qry = new BasicDBObject();
			List<BasicDBObject> where = new ArrayList<BasicDBObject>();
			where.add(new BasicDBObject("scope", "HL7STANDARD"));
			qry.put("$and", where);
			DBCursor cur = coll.find(qry);

			while (cur.hasNext()) {
				DBObject obj = cur.next();
				igdocument = conv.convert(obj);
				System.out.println("scope=" + igdocument.getScope());
				System.out.println("version=" + igdocument.getProfile().getMetaData().getHl7Version());
				System.out.println("Messages=" + account4Messages(igdocument.getProfile()));
				System.out.println("Segments=" + account4Segments(igdocument.getProfile(), mongoOps));
				System.out.println("Datatypes=" + account4DataTypes(igdocument.getProfile(), mongoOps));
				System.out.println("Value Sets=" + account4ValueSets(igdocument.getProfile(), mongoOps));
				System.out
						.println("SegmentLibrary=" + xCheckLib(igdocument.getProfile().getSegmentLibrary(), mongoOps));
				System.out.println(
						"DatatypeLibrary=" + xCheckLib(igdocument.getProfile().getDatatypeLibrary(), mongoOps));
				System.out.println("TableLibrary=" + xCheckLib(igdocument.getProfile().getTableLibrary(), mongoOps));
			}
		} catch (Exception e) {
			log.error("", e);
		}

	}

	public static void main(String[] args) {
		ReferentialIntegrityTest app = new ReferentialIntegrityTest();
		app.run();
	}
}
