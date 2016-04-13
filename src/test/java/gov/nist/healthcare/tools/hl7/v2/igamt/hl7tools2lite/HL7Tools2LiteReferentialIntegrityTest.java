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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Component;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Constant;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatype;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatypes;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Field;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Group;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Message;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Messages;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Profile;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Segment;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentRef;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentRefOrGroup;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Segments;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Table;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Tables;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.service.converters.ProfileReadConverter;

/**
 * @author gcr1
 *
 */
public class HL7Tools2LiteReferentialIntegrityTest {

	private static final Logger log = LoggerFactory.getLogger(HL7Tools2LiteReferentialIntegrityTest.class);

	static ProfileReadConverter conv;
	static DBCollection coll;
	int segRefCnt = 0;

	@BeforeClass
	public static void beforeClass() {
		MongoClient mongoClient = new MongoClient();
		DB db = mongoClient.getDB("igl");

		coll = db.getCollection("profile");

		conv = new ProfileReadConverter();
	}

	@Test
	public void testMessagesVsSegments() {

		log.info("Running testMessagesVsSegments...");

		DBCursor cur = coll.find();

		while (cur.hasNext()) {
			DBObject obj = cur.next();
			Profile profile = conv.convert(obj);
			String version = profile.getMetaData().getHl7Version();
			log.info("version=" + version + " scope=" + profile.getScope().name());
			account4ValueSets(profile);
			account4DataTypes(profile);
			account4Segments(profile);
		}
	}
	
	public void account4ValueSets(Profile profile) {
		Tables tables = profile.getTables();
		Datatypes datatypes = profile.getDatatypes();
		
		// First we find all valueSet Ids from the Datatypes.
		Set<String> dtVsIds = new HashSet<String>();
		for (Datatype dt : datatypes.getChildren()) {
			for (Component cmp : dt.getComponents()) {
				if(cmp.getTable() != null && cmp.getTable().length() != 0) {
					dtVsIds.add(cmp.getTable());
				}
			}
		}
		
		// Second we get all valueSet ids from the Tables.
		Set<String> vsIds = new HashSet<String>();
		for (Table valueSet : tables.getChildren()) {
			vsIds.add(valueSet.getId());
		}
		
		// Third we ensure each datatype can be found in the valueSets.
		for (String s : dtVsIds) {
//			assertTrue("datatype.vsId=" + s + "<==", vsIds.contains(s));
			if(!vsIds.contains(s)) {
				log.info("datatype.vsId=" + s);
			}
		}
		
		// Fourth we ensure each valueSet can be found in the dataypes.
//		for (String s : vsIds) {
//			assertTrue(dtVsIds.contains(s));
//		}
	}
	
	public void account4DataTypes(Profile profile) {
		Segments segs = profile.getSegments();
		Datatypes datatypes = profile.getDatatypes();
		
		// First we find all datatype Ids from the Segments.
		Set<String> segDtIds = new HashSet<String>();
		for (Segment seg : segs.getChildren()) {
			for (Field fld : seg.getFields()) {
				segDtIds.add(fld.getDatatype());
			}
		}

		// Second we get all datatype ids from the Datatypes.
		Set<String> dtIds = new HashSet<String>();
		for (Datatype dt : datatypes.getChildren()) {
			dtIds.add(dt.getId());
		}
		
		// Third we ensure each datatype can be found in the segments.
//		for (String s : dtIds) {
//			assertTrue(segDtIds.contains(s));
//		}
		
		// Fourth we ensure each segment can be found in the datatypes.
		for (String s : segDtIds) {
			assertTrue(dtIds.contains(s));
		}
	}
	
	public void account4Segments (Profile profile) {
		Segments segs = profile.getSegments();

		// First we list all the Segment ids.
		Set<String> segIds = new HashSet<String>();
		for (Segment seg : segs.getChildren()) {
			segIds.add(seg.getId());
		}

		// Second we list all the SegmentRefs from the Messages.
		Set<String> segRefs = null;
		Messages msgs = profile.getMessages();
		Iterator itr = msgs.getChildren().iterator();

		Message msg = null;
		while (itr.hasNext()) {
			msg = (Message) itr.next();
			segRefs = new HashSet<String>();
			segRefs.addAll(doGroup(msg.getChildren()));
		}

		// Third we check each segmentRef to be sure it has a corresponding
		// the Segment.id.
		for (String s : segRefs) {
			assertTrue(segIds.contains(s));
		}
		
//		for (String s : segIds) {
//			assertTrue(segRefs.contains(s));
//		}

//		log.info("segRefs=" + segRefs.size());
//		log.info("segRefCnt=" + segRefCnt);
//		log.info("segIds=" + segIds.size());
	}

	// A little recursion to get all SegmentRefs buried in Groups.
	Set<String> doGroup(List<SegmentRefOrGroup> sogs) {
		Set<String> refs = new HashSet<String>();

		for (SegmentRefOrGroup sog : sogs) {
			if (Constant.SEGMENTREF.equals(sog.getType())) {
				SegmentRef sr = (SegmentRef) sog;
				refs.add(sr.getRef());
				segRefCnt++;
			} else if (Constant.GROUP.equals(sog.getType())) {
				Group grp = (Group) sog;
				refs.addAll(doGroup(grp.getChildren()));
			} else {
				log.error("Neither SegRef nor Group sog=" + sog.getType() + "=");
			}
		}
		return refs;
	}

	@Test
	public void testComponentDataypes() {
		log.info("Running testComponentDataypes1...");

		DBCursor cur = coll.find();

		while (cur.hasNext()) {
			DBObject obj = cur.next();
			Profile profile = conv.convert(obj);
			String version = profile.getMetaData().getHl7Version();
			log.info("version=" + version + " scope=" + profile.getScope().name());
			List<String> dts = new ArrayList<String>();
			List<Component> cts = new ArrayList<Component>();
			Set<Component> ctsS = new HashSet<Component>();
			for (Datatype dt : profile.getDatatypes().getChildren()) {
				dts.add(dt.getId());
			}
			for (Datatype dt : profile.getDatatypes().getChildren()) {
				log.trace("dt=" + dt.getName() + " ct size=" + dt.getComponents().size());
				for (Component ct : dt.getComponents()) {
					cts.add(ct);
					ctsS.add(ct);
				}
			}
			log.info("children size()=" + profile.getDatatypes().getChildren().size());
			log.info("dts size()=" + dts.size());
			log.info("cts size()=" + cts.size());
			log.info("ctsS size()=" + ctsS.size());
			int cntCt = 0;
			for (Component ctId : cts) {
				assertTrue(dts.contains(ctId.getDatatype()));
				if (!dts.contains(ctId.getDatatype())) {
					log.info(cntCt++ + " " + "src" + " dts not contains=" + " id=" + ctId.getId());
				}
			}
			for (Component ctId : cts) {
				assertTrue(dts.contains(ctId.getDatatype()));
				if (!dts.contains(ctId.getDatatype())) {
					log.info(cntCt++ + " " + "src" + " dts not contains=" + " id=" + ctId.getId());
				}
			}
		}
	}

	@Test
	public void testFieldDatatypes() {
		log.info("Running testFieldDatatypes...");

		DBCursor cur = coll.find();

		while (cur.hasNext()) {
			DBObject obj = cur.next();
			Profile profile = conv.convert(obj);
			String version = profile.getMetaData().getHl7Version();
			log.info("version=" + version + " scope=" + profile.getScope().name());
			List<String> dts = new ArrayList<String>();
			for (Datatype dt : profile.getDatatypes().getChildren()) {
				dts.add(dt.getId());
			}
			log.info("segs size()=" + profile.getSegments().getChildren().size());
			for (Segment seg : profile.getSegments().getChildren()) {
				log.trace("seg=" + seg.getName() + " fld size=" + seg.getFields().size());
				for (Field fld : seg.getFields()) {
					assertTrue(dts.contains(fld.getDatatype()));
				}
			}
		}
	}
}
