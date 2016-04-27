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
import java.util.Arrays;
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
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.IGDocument;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Message;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Messages;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Profile;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Segment;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentLink;
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

	public boolean account4Messages(Profile profile) {
		TableLibrary tables = profile.getTableLibrary();
		Messages messages = profile.getMessages();

		// First we find all valueSet ids from the Messages.
		Set<String> msgStructIds = new HashSet<String>();
		for (Message msg : messages.getChildren()) {
			msgStructIds.add(msg.getStructID());
		}

		// Second we get all valueSet ids from the Tables.
		Set<TableLink> vsStructIds = tables.getChildren();

		// Third we ensure each message can be found in the valueSets.
		int hasmsgStructIds = 0;
		int hasNotmsgStructIds = 0;
		List<String> listNotmsgStructIds = new ArrayList<String>();
		int allmsgStructIds = msgStructIds.size();
		for (TableLink s : vsStructIds) {
			if (msgStructIds.contains(s.getId())) {
				hasmsgStructIds++;
			} else {
				hasNotmsgStructIds++;
				listNotmsgStructIds.add(s.getId());
			}
		}

		// Fourth we ensure each valueSet can be found in the Messages.
		int hasvsStructIds = 0;
		int hasNotvsStructIds = 0;
		List<String> listNotvsStructIds = new ArrayList<String>();
		int allvsStructIds = vsStructIds.size();
		for (String s : msgStructIds) {
			if (vsStructIds.contains(s)) {
				hasvsStructIds++;
			} else {
				hasNotvsStructIds++;
				listNotvsStructIds.add(s);
			}
		}
		System.out.println("Events==>");
		System.out.println("vs found in msg=" + hasmsgStructIds);
		System.out.println("vs NOT found in msg=" + hasNotmsgStructIds + " " + Arrays.toString(listNotmsgStructIds.toArray()));
		System.out.println("all msg=" + allmsgStructIds);
		System.out.println("msg found in vs=" + hasvsStructIds);
		System.out.println("msg NOT found in vs=" + hasNotvsStructIds + " " + Arrays.toString(listNotvsStructIds.toArray()));
		System.out.println("all vs=" + allvsStructIds);
		return (hasNotvsStructIds + hasNotmsgStructIds) == 0;
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

		TableLibrary tables = profile.getTableLibrary();
		DatatypeLibrary datatypes = profile.getDatatypeLibrary();

		// First we find all valueSet ids from the datatypes.
		Set<String> dtVsIds = new HashSet<String>();

		// to do this we take the datatype.ids...
		Set<String> dtIds = new HashSet<String>();
		for (DatatypeLink dtl : datatypes.getChildren()) {
			dtIds.add(dtl.getId());
		}

		// ...and use them to fetch the datatypes.
		// From there, we collect the valueSet ids from the components.
		Criteria where = Criteria.where("id").in(dtIds);
		Query qry = new Query(where);
		int empty = 0;
		List<Datatype> dts = mongoOps.find(qry, Datatype.class);
		for (Datatype dt : dts) {
			for (Component cmt : dt.getComponents()) {
				String tabId = cmt.getTable();
				if (tabId == null) {
					empty++;
				} else {
					dtVsIds.add(cmt.getTable());
				}
			}
		}

		// Second we get all valueSet ids from the Tables.
		Set<String> vsIds = new HashSet<String>();
		for (TableLink tbl : tables.getChildren()) {
			vsIds.add(tbl.getId());
		}

		// Third we ensure each datatype can be found in the valueSets.
		int hasdtVsIds = 0;
		int hasNotdtVsIds = 0;
		int alldtVsIds = dtVsIds.size();
		for (String s : dtVsIds) {
			if (vsIds.contains(s)) {
				hasdtVsIds++;
			} else {
				hasNotdtVsIds++;
			}
		}

		// Fourth we ensure each valueSet can be found in the dataypes.
		int hasVsIds = 0;
		int hasNotVsIds = 0;
		int allVsIds = vsIds.size();
		for (String s : vsIds) {
			if (dtVsIds.contains(s)) {
				hasVsIds++;
			} else {
				hasNotVsIds++;
			}
		}
		System.out.println("VSS==>");
		System.out.println("empty tables=" + empty);
		System.out.println("datatype.table found in table.ids=" + hasdtVsIds);
		System.out.println("datatype.table NOT found in table.ids=" + hasNotdtVsIds);
		System.out.println("all table.ids=" + allVsIds);
		System.out.println("tables.table.id found in datatype.tables=" + hasVsIds);
		System.out.println("tables.table.id NOT found in datatype.tables=" + hasNotVsIds);
		System.out.println("all datatype.tables=" + alldtVsIds);
		return (hasNotdtVsIds + hasNotVsIds) == 0;
	}

	boolean account4DataTypes(Profile profile, MongoOperations mongoOps) {
		// SegmentLibrary segs = profile.getSegmentLibrary();
		// DatatypeLibrary datatypes = profile.getDatatypeLibrary();
		//
		// // First we find all datatype Ids from the Segments.
		// Set<String> segDtIds = new HashSet<String>();
		// for (Segment seg : segs.getChildren()) {
		// for (Field fld : seg.getFields()) {
		// segDtIds.add(fld.getDatatype());
		// }
		// }
		//
		// // Second we get all datatype ids from the Datatypes.
		// Set<String> dtIds = new HashSet<String>();
		// for (Datatype dt : datatypes.getChildren()) {
		// dtIds.add(dt.getId());
		// }
		//
		// // Third we ensure each datatype can be found in the segments.
		// int hassegDtIds = 0;
		// int hasNotsegDtIds = 0;
		// int allsegDtIds = segDtIds.size();
		// for (String s : dtIds) {
		// if (segDtIds.contains(s)) {
		// hassegDtIds++;
		// } else {
		// hasNotsegDtIds++;
		// }
		// }
		//
		// // Fourth we ensure each segment can be found in the datatypes.
		// int hasdtIds = 0;
		// int hasNotdtIds = 0;
		// int alldtIds = dtIds.size();
		// for (String s : segDtIds) {
		// if (dtIds.contains(s)) {
		// hasdtIds++;
		// } else {
		// hasNotdtIds++;
		// }
		// }
		// System.out.println("DTS==>");
		// System.out.println("datatype.DtId found in segment.datatypes=" + hassegDtIds);
		// System.out.println("datatype.DtId NOT found in segment.datatypes=" +
		// hasNotsegDtIds);
		// System.out.println("all segment.datatypes=" + alldtIds);
		// System.out.println("segment.datatype found in datatype.DtIds=" + hasdtIds);
		// System.out.println("segment.datatype NOT found in datatype.DtIds=" +
		// hasNotdtIds);
		// System.out.println("all datatype.DtIds=" + allsegDtIds);
		return true; // (hasNotsegDtIds + hasNotdtIds) == 0;
	}

	public boolean account4Segments(Profile profile, MongoOperations mongoOps) {
		// SegmentLibrary segs = profile.getSegmentLibrary();
		//
		// // First we list all the Segment ids.
		// Map<String, Segment> segIds = new HashMap<String, Segment>();
		// for (Segment seg : segs.getChildren()) {
		// segIds.put(seg.getId(), seg);
		// }
		//
		// // Second we list all the SegmentRefs from the Messages.
		// Map<String, Message> segRefs = new HashMap<String, Message>();
		// Messages msgs = profile.getMessages();
		// Iterator<Message> itr = msgs.getChildren().iterator();
		//
		// Message msg = null;
		// while (itr.hasNext()) {
		// msg = itr.next();
		// Set<String> segRefs1 = UtilHL7Tools2Lite.doGroup(msg.getChildren());
		// for (String key : segRefs1) {
		// segRefs.put(key ,msg);
		// }
		// }
		//
		// // Third we check each segmentRef to be sure it has a corresponding
		// // Segment.id.
		// List<Segment> hassegIds = new ArrayList<Segment>();
		// Map<String, Message> hasNotsegIds = new HashMap<String, Message>();
		// int allsegIds = segIds.size();
		// for (Map.Entry<String, Message> entry : segRefs.entrySet()) {
		// Segment seg = segIds.get(entry.getKey());
		// if (seg != null) {
		// hassegIds.add(seg);
		// } else {
		// hasNotsegIds.put(entry.getKey(), entry.getValue());
		// }
		// }
		//
		// // Fourth we check each Segment.id to be sure it has a corresponding
		// // segmentRef.
		// Map<String, Message> hassegRefs = new HashMap<String, Message>();
		// Map<String, Segment> hasNotsegRefs = new HashMap<String, Segment>();
		// int allsegRefs = segRefs.size();
		// for (Map.Entry<String, Segment> entry : segIds.entrySet()) {
		// Message msg1 = segRefs.get(entry.getKey());
		// if (msg1 != null) {
		// hassegRefs.put(entry.getKey(), msg1);
		// } else {
		// hasNotsegRefs.put(entry.getKey(), entry.getValue());
		// }
		// }
		//
		// // System.out.println("segRefs=" + segRefs.size());
		// // System.out.println("segRefCnt=" + segRefCnt);
		// // System.out.println("segIds=" + segIds.size());
		// System.out.println("SEGS==>");
		// System.out.println("segment.SegId found in message.segRefs=" +
		// hassegIds.size());
		// System.out.println("segment.SegId NOT found in message.segRefs=" +
		// hasNotsegIds.size());
		// for (Map.Entry<String, Message> entry : hasNotsegIds.entrySet()) {
		// System.out.println(" segId=" + entry.getKey() + " msgName=" +
		// entry.getValue().getName());
		// }
		// System.out.println("all message.segRefs=" + allsegRefs);
		// System.out.println("message.segRef found in segment.SegIds=" +
		// hassegRefs.size());
		// System.out.println("message.segRef NOT found in segment.SegIds=" +
		// hasNotsegRefs.size());
		// for (Map.Entry<String, Segment> entry : hasNotsegRefs.entrySet()) {
		// System.out.println(" segRef=" + entry.getKey() + " segLabel=" +
		// entry.getValue().getName());
		// }
		// System.out.println("all segment.SegIds=" + allsegIds);
		return true; // (hasNotsegIds.size() + hasNotsegRefs.size()) == 0;
	}

	public void run() {

		MongoOperations mongoOps;
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
						// b = account4Segments(igdocument.getProfile(),
						// mongoOps);
						// b = account4DataTypes(igdocument.getProfile(),
						// mongoOps);
				System.out.println("Value Sets=" + account4ValueSets(igdocument.getProfile(), mongoOps));
				System.out.println("SegmentLibrary=" + xCheckLib(igdocument.getProfile().getSegmentLibrary(), mongoOps));
				System.out.println("DatatypeLibrary=" + xCheckLib(igdocument.getProfile().getDatatypeLibrary(), mongoOps));
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
