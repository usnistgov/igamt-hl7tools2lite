package gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nist.healthcare.hl7tools.domain.CodeTableLibrary;
import gov.nist.healthcare.hl7tools.domain.Component;
import gov.nist.healthcare.hl7tools.domain.Datatype;
import gov.nist.healthcare.hl7tools.domain.DatatypeLibrary;
import gov.nist.healthcare.hl7tools.domain.Element;
import gov.nist.healthcare.hl7tools.domain.Field;
import gov.nist.healthcare.hl7tools.domain.IGLibrary;
import gov.nist.healthcare.hl7tools.domain.Message;
import gov.nist.healthcare.hl7tools.domain.MessageLibrary;
import gov.nist.healthcare.hl7tools.domain.Segment;
import gov.nist.healthcare.hl7tools.domain.SegmentLibrary;
import gov.nist.healthcare.hl7tools.service.HL7DBMockServiceImpl;
import gov.nist.healthcare.hl7tools.service.HL7DBService;
import gov.nist.healthcare.hl7tools.service.HL7DBServiceException;

public class ReferentailIntegrityMock implements Runnable {

	Logger log = LoggerFactory.getLogger(ReferentailIntegrityMock.class);
	String hl7Version;
	public HL7DBService service = new HL7DBMockServiceImpl();
	IGLibrary ig;

	public ReferentailIntegrityMock(String hl7Version) {
		super();
		this.hl7Version = hl7Version;
	}

	public ReferentailIntegrityMock() {
		super();
	}

	@Override
	public void run() {
		List<String[]> messageArrayList;
		try {
			messageArrayList = service.getMessageListByVersion(hl7Version);
			log.info("messageArrayList=" + messageArrayList.size());
			List<String> messageList = new ArrayList<String>();
			for (String[] ss : messageArrayList) {
				messageList.add(ss[0]);
			}
			ig = service.buildIGFromMessageList(hl7Version, messageList);
			checkMessages(ig.getMessageLibrary());
		} catch (HL7DBServiceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	boolean checkMessages(MessageLibrary msgLib) {
		boolean rval = true;
		for (Message msg : msgLib.values()) {
//			log.info("message=" + msg.toString());
			List<Element> els = msg.getChildren();
			checkElements(els);
		}
		return rval;
	}

	boolean checkElements(List<Element> els) {
		boolean rval = true;
		for (Element el : els) {
			rval = rval && checkElement(el);
		}
		return rval;
	}

	boolean checkElement(Element el) {
		boolean rval = true;
		switch (el.getType()) {
		case GROUP: {
			log.info("group=" + el.toString());
			rval = rval && checkElements(el.getChildren());
			break;
		}
		case SEGEMENT: {
			rval = rval && checkSegment(el.getSegment());
			break;
		}
		case CHOICE: {
			log.info("choice=" + el.toString());
			rval = rval && checkElements(el.getChildren());
			break;
		}
		default: {
			log.info("element not typed=" + el.toString());
			break;
		}
		}
		return rval;
	}

	boolean checkSegments(SegmentLibrary segLib) {
		boolean rval = true;
		for (Segment seg : segLib.values()) {
			checkSegment(seg);
			rval = rval && checkSegment(seg);
		}
		return rval;
	}

	boolean checkSegment(Segment seg) {
		boolean rval = ig.getSegmentLibrary().containsValue(seg);
		if (!rval) {
			log.info("seg not found=" + seg.toString());
		}
		if (seg.getFields() == null) {
			log.info("seg fields null=" + seg.toString());
			return true;
		}
		return rval && checkFields(seg.getFields());
	}

	boolean checkFields(List<Field> flds) {
		boolean rval = true;
		for (Field fld : flds) {
			boolean b = checkField(fld);
			if (!b) {
				log.info("fld not found=" + fld);
			}
			rval = rval && b;
		}
		return rval;
	}

	boolean checkField(Field fld) {
		Datatype dt = fld.getDatatype();
		return ig.getDatatypeLibrary().containsValue(dt);
	}

	boolean checkDatatypes(DatatypeLibrary dtLib) {
		boolean rval = true;
		for (Datatype dt : dtLib.values()) {
			rval = rval && checkDatatype(dt);
		}
		return rval;
	}

	boolean checkDatatype(Datatype dt) {
		boolean rval = ig.getDatatypeLibrary().containsValue(dt);
		if (!rval) {
			log.info("dt not found=" + dt.toString());
		}
		return rval && checkComponents(dt.getComponents());
	}

	boolean checkComponents(List<Component> cmps) {
		boolean rval = true;
		for (Component cmp : cmps) {
			boolean b = checkComponent(cmp);
			if (!b) {
				log.info("cmp not found=" + cmp);
			}
			rval = rval && b;
		}
		return rval;
	}

	boolean checkComponent(Component cmp) {
		CodeTableLibrary tabLib = ig.getCodeTableLibrary();
		return tabLib.containsValue(cmp);
	}

	public static void main(String[] args) {
		ReferentailIntegrityMock app;
		if (args.length > 0) {
			app = new ReferentailIntegrityMock(args[0]);
		} else {
			app = new ReferentailIntegrityMock();
		}
		app.run();
	}

}
