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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Component;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Constant;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatype;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatypes;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Field;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Group;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Message;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Messages;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Segment;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentRef;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentRefOrGroup;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Segments;

/**
 * @author gcr1
 *
 */
public class UtilHL7Tools2Lite {

	private static final Logger log = LoggerFactory.getLogger(UtilHL7Tools2Lite.class);

	public static int countMessageRefs(Messages msgs) {
		return getMessageRefs(msgs).size();
	}

	public static Set<SegmentRef> getMessageRefs(Messages msgs) {

		Set<SegmentRef> segRefs = new HashSet<SegmentRef>();

		Iterator<Message> itr = msgs.getChildren().iterator();

		while (itr.hasNext()) {
			segRefs.addAll(getMessageRefs(itr.next()));
		}

		return segRefs;
	}

	public static Set<SegmentRef> getMessageRefs(Message msg) {
		return doGroup(msg.getChildren());
	}

	public static Set<SegmentRef> doGroup(List<SegmentRefOrGroup> sogs) {
		Set<SegmentRef> refs = new HashSet<SegmentRef>();

		for (SegmentRefOrGroup sog : sogs) {
			if (Constant.SEGMENTREF.equals(sog.getType())) {
				SegmentRef sr = (SegmentRef) sog;
				refs.add(sr);
			} else if (Constant.GROUP.equals(sog.getType())) {
				Group grp = (Group) sog;
				refs.addAll(doGroup(grp.getChildren()));
			} else {
				log.error("Neither SegRef nor Group sog=" + sog.getType() + "=");
			}
		}
		return refs;
	}

	public static int countFields(Segments segs) {
		return getFields(segs).size();
	}

	public static Set<Field> getFields(Segments segs) {
		
		Set<Field> flds = new HashSet<Field>();
		
		for (Segment seg : segs.getChildren()) {
			flds.addAll(getFields(seg));
		} 
		
		return flds;
	}

	public static Set<Field> getFields(Segment seg) {

		Set<Field> flds = new HashSet<Field>();

		for (Field fld : seg.getFields()) {
			flds.add(fld);
		}
		return flds;
	}

	public static int countComponents(Datatypes segs) {
		return getComponents(segs).size();
	}
	
	public static Set<String> getComponentIds(Datatypes dts) {
		Set<String> ids = new HashSet<String>();
		for (Component cpt : getComponents(dts)) {
			ids.add(cpt.getId());
		}
		return ids;
	}

	public static Set<Component> getComponents(Datatypes dts) {
		
		Set<Component> cpts = new HashSet<Component>();
		
		for (Datatype dt : dts.getChildren()) {
			cpts.addAll(getComponents(dt));
		} 
		
		return cpts;
	}

	public static Set<Component> getComponents(Datatype dt) {

		Set<Component> cpts = new HashSet<Component>();

		for (Component cmp : dt.getComponents()) {
			cpts.add(cmp);
		}
		return cpts;
	}

}
