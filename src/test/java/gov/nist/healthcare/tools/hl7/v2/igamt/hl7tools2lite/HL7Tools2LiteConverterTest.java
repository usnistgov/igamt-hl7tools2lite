package gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nist.healthcare.hl7tools.domain.IGLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Component;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Constant;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatype;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Field;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Group;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Message;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Messages;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Profile;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Segment;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentRef;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentRefOrGroup;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Segments;

public class HL7Tools2LiteConverterTest {

	Logger log = LoggerFactory.getLogger(HL7Tools2LiteConverterTest.class);
	
	// Verify that all entries from the CodeTableLibrary are put into 
	// HL7Tools2LiteConverter.mapTables.
//	@Test
	public void testCodeTableMap() {
//		HL7Tools2LiteConverter app = new HL7Tools2LiteConverter();
//		app.run();
//		IGLibrary ig  = app.igLibraries.get("2.5.1");
////		Profile profile = app.doVersion("2.5.1");
//		assertNotNull(profile);
//		int ctlSize = ig.getCodeTableLibrary().keySet().size();
//		int mapSize = app.mapTables.keySet().size();
//		assertEquals(ctlSize, mapSize);
	}
	
	@Test
	public void testComponentDataypes() {
//		HL7Tools2LiteConverter app = new HL7Tools2LiteConverter();
//		app.run();
//		for (Profile profile : app.profiles.values()) {
//			log.info("profile=" + profile.getMetaData().getHl7Version());
//			assertNotNull(profile);
//			List<String> dts = new ArrayList<String>();
//			for (Datatype dt : profile.getDatatypes().getChildren()) {
//				dts.add(dt.getId());
//			}
//			log.info("dt size()=" + profile.getDatatypes().getChildren().size());
//			for (Datatype dt : profile.getDatatypes().getChildren()) {
//				log.info("dt=" + dt.getName() + " ct size=" + dt.getComponents().size());
//				for (Component ct : dt.getComponents()) {
//					assertTrue(dts.contains(ct.getDatatype()));
//				}
//			}
//		}
	}
	
	@Test
	public void testFieldDatatypes() {
//		HL7Tools2LiteConverter app = new HL7Tools2LiteConverter();
//		app.run();
//		for (Profile profile : app.profiles.values()) {
//			log.info("profile=" + profile.getMetaData().getHl7Version());
//			assertNotNull(profile);
//			List<String> dts = new ArrayList<String>();
//			for (Datatype dt : profile.getDatatypes().getChildren()) {
//				dts.add(dt.getId());
//			}
//			log.info("segs size()=" + profile.getSegments().getChildren().size());
//			for (Segment seg : profile.getSegments().getChildren()) {
//				log.info("seg=" + seg.getName() + " fld size=" + seg.getFields().size());
//				for (Field fld : seg.getFields()) {
//					assertTrue(dts.contains(fld.getDatatype()));
//				}
//			}
//		}
	}
	
//	@Test
	public void testDatatypesMap() {
//		HL7Tools2LiteConverter app = new HL7Tools2LiteConverter();
//		app.run();
//		IGLibrary ig  = app.igLibraries.get("2.5.1");
//		Profile profile = app.doVersion("2.5.1");
//		assertNotNull(profile);
//		int dtlSize = ig.getDatatypeLibrary().keySet().size();
//		int mapSize = app.mapDatatypes.keySet().size();
//		assertEquals(dtlSize, mapSize);
	}

//	@Test
	public void testMessagesVsSegments() {
		HL7Tools2LiteConverter app = new HL7Tools2LiteConverter();
		app.run();
//		Profile p = app.profiles.get("2.7");
//		Segments segs = p.getSegments();
		
		// First we list all the Segment ids.
//		List<String> segIds = new ArrayList<String>();
//		for(Segment seg : segs.getChildren()) {
//			segIds.add(seg.getId());
//		}
		
		// Second we list all the SegmentRefs from the Messages.
//		List<String> segRefs = null;
//		Messages msgs = p.getMessages();
//		Iterator itr = msgs.getChildren().iterator();
//				
//		Message msg = null;
//		while(itr.hasNext()) {
//			msg = (Message)itr.next();
//			segRefs = new ArrayList<String>();
//			segRefs.addAll(doGroup(msg.getChildren()));
//		}
		
		// Third we check each segmentRef to be sure it has a corresponding the Segment.id.
//		for (String s : segRefs) {
//			assertTrue(segIds.contains(s));
//		}		
	}
	
	// A little recursion to get all SegmentRefs buried in Groups.
	List<String> doGroup(List<SegmentRefOrGroup> sogs) {
		List<String> refs = new ArrayList<String>();
		
		for (SegmentRefOrGroup sog : sogs) {
			if (Constant.SEGMENTREF == sog.getType()) {
				SegmentRef sr = (SegmentRef)sog;
				refs.add(sr.getRef());
			} else {
				Group grp = (Group) sog;
				refs.addAll(doGroup(grp.getChildren()));
			}
		}
		return refs;
	}
}
