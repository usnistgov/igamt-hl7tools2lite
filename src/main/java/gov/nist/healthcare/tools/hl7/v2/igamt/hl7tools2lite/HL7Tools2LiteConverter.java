/**
 * This software was developed at the National Institute of Standards and Technology by employees
 * of the Federal Government in the course of their official duties. Pursuant to title 17 Section 105 of the
 * United States Code this software is not subject to copyright protection and is in the public domain.
 * This is an experimental system. NIST assumes no responsibility whatsoever for its use by other parties,
 * and makes no guarantees, expressed or implied, about its quality, reliability, or any other characteristic.
 * We would appreciate acknowledgment if the software is used. This software can be redistributed and/or
 * modified freely provided that any derivative works bear some notice that they are derived from it, and any
 * modified versions bear some notice that they have been modified.
 */
package gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;

import com.mongodb.MongoClient;

import gov.nist.healthcare.hl7tools.domain.HL7Table;
import gov.nist.healthcare.hl7tools.domain.IGLibrary;
import gov.nist.healthcare.hl7tools.service.HL7DBMockServiceImpl;
import gov.nist.healthcare.hl7tools.service.HL7DBService;
import gov.nist.healthcare.hl7tools.service.HL7DBServiceException;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Code;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Component;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.ContentDefinition;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatype;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatypes;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Extensibility;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Field;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Group;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.IGDocumentScope;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Message;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Messages;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Profile2;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.ProfileMetaData;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Segment;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentRef;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentRefOrGroup;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Segments;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Stability;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Table;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Tables;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Usage;

// Converts from old to new.  We read old *.json into an object graph rooted on IGLibrary then convert
// into a graph rooted on Profile.  We then write the output to a set of new *.json files.

public class HL7Tools2LiteConverter implements Runnable {

	Logger log = LoggerFactory.getLogger(HL7Tools2LiteConverter.class);

	public final static File OUTPUT_DIR = new File(System.getenv("IGAMT") + "/profiles");
	public static SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");

	public Profile2 profile;
	public IGLibrary ig;
	String version;

	public Map<String, Datatype> mapDatatypes;
	public Map<String, Segment> mapSegments;
	public Map<String, Segment> mapSegmentsById;
	public Map<String, Table> mapTables;

	public Map<String, Profile2> profiles = new HashMap<String, Profile2>();
	public Map<String, IGLibrary> igLibraries = new HashMap<String, IGLibrary>();

	public HL7DBService service = new HL7DBMockServiceImpl();
	List<String> versions;

	public void run() {
		MongoOperations mongoOps = null;
		mongoOps = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "igl"));
		log.info("start...");
		log.info("Dropping mongo collection profile...");
		mongoOps.dropCollection(Profile2.class);
		List<String> versions = service.getSupportedHL7Versions();
		// if (!OUTPUT_DIR.exists()) {
		// OUTPUT_DIR.mkdir();
		// }
		for (String version : versions) {
			this.version = version;
			Profile2 profile = doVersion(version);
			mongoOps.insert(profile);
			igLibraries.put(version, ig);
			profiles.put(version, profile);
		}
		log.info("...end");
	}

	public Profile2 doVersion(String version) {
		log.info("version=" + version);
		mapDatatypes = new HashMap<String, Datatype>();
		mapSegments = new HashMap<String, Segment>();
		mapSegmentsById = new HashMap<String, Segment>();
		mapTables = new HashMap<String, Table>();

		try {
			List<String[]> messageArrayList = service.getMessageListByVersion(version);
			log.info("messageArrayList=" + messageArrayList.size());
			List<String> messageList = new ArrayList<String>();
			for (String[] ss : messageArrayList) {
				messageList.add(ss[0]);
			}
			log.info("messageList=" + messageList.size());

			ig = service.buildIGFromMessageList(version, messageList);
			log.info("getMessageLibrary" + ig.getMessageLibrary().size());
			log.info("getSegmentLibrary" + ig.getSegmentLibrary().size());
			log.info("getDatatypeLibrary" + ig.getDatatypeLibrary().size());
			log.info("getCodeTableLibrary" + ig.getCodeTableLibrary().size());
			profile = convert(ig);
			profile.setId(generateId());
			profile.setScope(IGDocumentScope.HL7STANDARD);
			ProfileMetaData pmd = new ProfileMetaData();
			pmd.setOrgName("NIST");
//  			pmd.setIdentifier("Default Identifier");
			pmd.setHl7Version(version);
			pmd.setName("Default Name");
			profile.setMetaData(pmd);
		} catch (HL7DBServiceException e) {
			log.error("", e);
		}

		return profile;
	}

	public static void main(String[] args) {
		HL7Tools2LiteConverter app = new HL7Tools2LiteConverter();
		app.run();
	}

	Profile2 convert(IGLibrary i) {
		Profile2 p = new Profile2();
		Tables tabs = convertTables(i.getCodeTableLibrary());
		convertDataTypesFirstPass(i.getDatatypeLibrary());
		convertDataTypesSecondPass(i.getDatatypeLibrary());
		Datatypes dts = new Datatypes();
		Segments segs = convertSegments(i.getSegmentLibrary());
		Messages msgs = convertMessages(i.getMessageLibrary());
		Set<Datatype> dtSet = new HashSet<Datatype>(mapDatatypes.values());
		dts.setChildren(dtSet);
		p.setDatatypes(dts);
		p.setSegments(segs);
		p.setTables(tabs);
		p.setMessages(msgs);
		return p;
	}

	Messages convertMessages(gov.nist.healthcare.hl7tools.domain.MessageLibrary i) {
		Messages o = new Messages();
		int seq = 1;
		for (String key : i.keySet()) {
			gov.nist.healthcare.hl7tools.domain.Message msg = i.get(key);
			o.addMessage(convertMessage(msg, seq++));
		}

		return o;
	}

	Message convertMessage(gov.nist.healthcare.hl7tools.domain.Message i, int seq) {

		Message o = new Message();
		o.getChildren().addAll(convertElements(checkChildren(i.getChildren())));
		o.setDescription(i.getDescription());
		o.setEvent(i.getEvent());
		o.setIdentifier("Default" + i.getEvent() + "Identifier");
		o.setMessageType(i.getMessageType());
		o.setName(assembleMessageName(i));
		assert(o.getName() != null);
		o.setPosition(seq);
		o.setStructID(assembleMessageStructID(i));
//		o.setDate(sdf.format(new Date()));
		return o;
	}
	
	String assembleMessageName(gov.nist.healthcare.hl7tools.domain.Message i) {
		String rval = i.getMessageType() + "^" + i.getEvent() + "^" + assembleMessageStructID(i);
		assert(rval != null);
		return rval;
	}

	String assembleMessageStructID(gov.nist.healthcare.hl7tools.domain.Message i) {
		String rval = i.getMessageType() + "_" + i.getEvent();
		assert(rval != null);
		return rval;
	}

	List<SegmentRefOrGroup> convertElements(List<gov.nist.healthcare.hl7tools.domain.Element> i) {
		List<SegmentRefOrGroup> o = new ArrayList<SegmentRefOrGroup>();
		for (gov.nist.healthcare.hl7tools.domain.Element el : i) {
			o.add(convertElement(el));
		}
		return o;
	}

	SegmentRefOrGroup convertElement(gov.nist.healthcare.hl7tools.domain.Element i) {
		SegmentRefOrGroup o = null;
		switch (i.getType()) {
		case GROUP: {
			o = convertGroup(i);
			break;
		}
		case SEGEMENT: {
			o = convertSegmentRef(i);
			break;
		}
		default:
			log.error("Element was neither Group nor SegmentRef.");
			break;
		}
		return o;
	}

	Group convertGroup(gov.nist.healthcare.hl7tools.domain.Element i) {
		Group o = new Group();
		o.setComment(i.getComment());
		o.setMax(i.getMax());
		o.setMin(i.getMin());
		o.setName(i.getName());
		o.setPosition(i.getPosition());
		o.setUsage(convertUsage(i.getUsage()));
		o.getChildren().addAll(convertElements(checkChildren(i.getChildren())));
		return o;
	}

	List<gov.nist.healthcare.hl7tools.domain.Element> checkChildren(
			List<gov.nist.healthcare.hl7tools.domain.Element> i) {
		if (i == null) {
			return new ArrayList<gov.nist.healthcare.hl7tools.domain.Element>();
		} else
			return i;
	}

	SegmentRef convertSegmentRef(gov.nist.healthcare.hl7tools.domain.Element i) {
		SegmentRef o = new SegmentRef();
		o.setUsage(convertUsage(i.getUsage()));
		o.setMin(i.getMin());
		o.setMax(i.getMax());
		o.setPosition(i.getPosition());
		String key = i.getName();
		Segment ref = mapSegments.get(key);
		if (ref != null) {
			if (ref.getId() != null) {
				log.trace("settingRef id=" + ref.getId() + " from key=" + key);
				if (mapSegmentsById.get(ref.getId()) == null) {
					log.error("was null in mapSegmentsById id=" + ref.getId());
				}
				o.setRef(ref.getId());
			} else {
				log.error("segment.Id was null for element=" + i.getName());
			}
		} else {
			log.error("segment was null for element=" + i.getName());
		}
		return o;
	}

	Segments convertSegments(gov.nist.healthcare.hl7tools.domain.SegmentLibrary i) {
		Segments o = new Segments();
		for (String key : i.keySet()) {
			gov.nist.healthcare.hl7tools.domain.Segment sg = i.get(key);
			o.addSegment(convertSegment(sg));
		}

		return o;
	}

	Segment convertSegment(gov.nist.healthcare.hl7tools.domain.Element i) {
		return convertSegment(i.getSegment());
	}

	Segment convertSegment(gov.nist.healthcare.hl7tools.domain.Segment i) {
		Segment o = new Segment();
		o.setComment(i.getComment());
		o.setDescription(i.getDescription());
		o.setFields(convertFields(checkFieldList(i.getFields())));
		o.setLabel(i.getName());
		o.setName(i.getName());
		String key = i.getName();
		if (!mapSegments.containsKey(key)) {
			mapSegments.put(key, o);
			mapSegmentsById.put(o.getId(), o);
		} else {
			log.debug("Key=" + key + " already put id=" + o.getId());
		}
		return o;
	}

	List<gov.nist.healthcare.hl7tools.domain.Field> checkFieldList(List<gov.nist.healthcare.hl7tools.domain.Field> i) {
		if (i == null) {
			return new ArrayList<gov.nist.healthcare.hl7tools.domain.Field>();
		}
		return i;
	}

	List<Field> convertFields(List<gov.nist.healthcare.hl7tools.domain.Field> i) {
		List<Field> o = new ArrayList<Field>();
		for (gov.nist.healthcare.hl7tools.domain.Field f : i) {
			o.add(convertField(f));
		}
		return o;
	}

	Field convertField(gov.nist.healthcare.hl7tools.domain.Field i) {
		String s = i.getDatatype().getName();
		Field o = new Field();
		Datatype dt = mapDatatypes.get(s);
		o.setComment(i.getComment());
		o.setConfLength(Integer.toString(i.getConfLength()));
		o.setDatatype(dt.getId());
		o.setItemNo(i.getItemNo());
		o.setMax(i.getMax());
		o.setMin(i.getMin());
		o.setMinLength(i.getMinLength());
		o.setMaxLength(i.getMaxLength());
		o.setName(i.getDescription());
		o.setPosition(i.getPosition());
		// Some fields do not have a code table associated with them.
		gov.nist.healthcare.hl7tools.domain.CodeTable ct = i.getCodeTable();
		if (ct != null) {
			String key = i.getCodeTable().getKey();
			Table tab = mapTables.get(key);
			if (tab == null) {
				log.error("mapTables.get(" + key + ") was null");
			} else {
				o.setTable(tab.getId());
			}
		} else {
			log.debug("CodeTable for field is null. description=" + i.getDescription());
		}
		o.setUsage(convertUsage(i.getUsage()));

		return o;
	}

	Tables convertTables(gov.nist.healthcare.hl7tools.domain.CodeTableLibrary i) {
		Tables o = new Tables();
		for (String s : i.keySet()) {
			gov.nist.healthcare.hl7tools.domain.CodeTable ct = i.get(s);
			if (ct != null) {
				String key = ct.getKey();
				mapTables.put(key, null);
			} else {
				log.debug("CodeTable is null!!");
			}
		}
		for (String s : i.keySet()) {
			gov.nist.healthcare.hl7tools.domain.CodeTable ct = i.get(s);
			if (ct != null) {
				String key = ct.getKey();
				Table tab = convertTable(ct);
				mapTables.put(key, tab);
				o.addTable(tab);
			} else {
				log.debug("CodeTable is null!!");
			}
		}
		return o;
	}

	Table convertTable(gov.nist.healthcare.hl7tools.domain.CodeTable i) {

		Table o = new Table();

		o.setBindingIdentifier(i.getKey());
		o.setName(i.getDescription());
		o.setOid(i.getOid());
		o.setVersion(i.getVersion());
		o.setCodes(convertCodes(i.getCodes()));
		// Static OR Dynamic (enum) no condition as per Woo. Use Static.
		o.setStability(Stability.Static);
		// Use ContentDefinition.Extensional.
		o.setContentDefinition(ContentDefinition.Extensional);
		o.setDescription(i.getDescription());
		if (i instanceof HL7Table) {
			HL7Table ii = (HL7Table) i;
			// HL7 = Closed; User = Open (enum)
			Extensibility ext = ("HL7".equals(ii.getType()) ? Extensibility.Closed : Extensibility.Open);
			o.setExtensibility(ext);
		} else {
			log.debug("No type");
		}
		return o;
	}

	List<Code> convertCodes(List<gov.nist.healthcare.hl7tools.domain.Code> i) {
		List<Code> o = new ArrayList<Code>();
		if (i != null) {
			for (gov.nist.healthcare.hl7tools.domain.Code cd : i) {
				o.add(convertCode(cd));
			}
		} else {
			log.trace("code was null");
		}
		return o;
	}

	Code convertCode(gov.nist.healthcare.hl7tools.domain.Code i) {
		Code o = new Code();
		o.setValue(i.getValue());
		o.setLabel(i.getDescription());
		o.setCodeSystem(convertCodeSystem(i.getCodeSystem()));
		o.setCodeUsage(convertCodeUsage(i));
		o.setComments(i.getComment());
		return o;
	}

	String convertCodeUsage(gov.nist.healthcare.hl7tools.domain.Code i) {
		String usage = i.getUsage().getValue();
		if ("O".equalsIgnoreCase(usage) || usage == null) {
			return "P";
		} else {
			return usage;
		}
	}

	String convertCodeSystem(gov.nist.healthcare.hl7tools.domain.CodeSystem i) {
		return "HL7" + i.getName();
	}

	// We convert Dataypes in two passes.
	void convertDataTypesFirstPass(gov.nist.healthcare.hl7tools.domain.DatatypeLibrary i) {
		// First pass is necessary to build a collection of all DTs mapped by DT
		// label.
		Datatypes o = new Datatypes();
		List<String> dtIds = new ArrayList<String>();
		String dtId = "AAA"; // dummy value to avoid null;
		for (String key : i.keySet()) {
			gov.nist.healthcare.hl7tools.domain.Datatype dt = i.get(key);
			Datatype convertedDt = convertDatatypeFirstPass(dt);
			// Key = dt.label and Value = the Datatype;
			String key1 = convertedDt.getLabel();
			assert(key.equals(key1)) : "key=" + key + " key1=" + key1;
			mapDatatypes.put(key, convertedDt);
		}
		// dumpMap();
	}

	// We dumpMap as a debugging aid.
	void dumpMap() {
		for (Map.Entry<String, Datatype> entry : mapDatatypes.entrySet()) {
			log.info("key=" + entry.getKey() + " val=" + entry.getValue());
		}
	}
	
	void convertDataTypesSecondPass(gov.nist.healthcare.hl7tools.domain.DatatypeLibrary i) {
		// Here we get our ouput object from the map instead of creating one.
		// If we were to create one here it would have a different id from the one 
		// in the map.

		List<String> dtIds = new ArrayList<String>();
		String dtId = "AAA"; // dummy value to avoid null;
		for (String key : i.keySet()) {
			gov.nist.healthcare.hl7tools.domain.Datatype dt = i.get(key);
			Datatype convertedDt = mapDatatypes.get(key);
			// Key = dt.label and Value = the Datatype;
			String key1 = convertedDt.getLabel();
			assert(key.equals(key1)) : "key=" + key + " key1=" + key1;
			convertDatatypeSecondPass(dt, convertedDt);
		}
		// dumpMap();
	}

	// Called by the first pass. Here we do a partial conversion.
	Datatype convertDatatypeFirstPass(gov.nist.healthcare.hl7tools.domain.Datatype i) {
		Datatype o = new Datatype();
		o.setLabel(i.getName());
		o.setName(i.getName());
		o.setDescription(i.getDescription());
		o.setComment(i.getComment());
		o.setUsageNote(i.getUsageNotes());
//		o.setHl7Version(version);
		// We refrain from converting components until the second pass.
		// o.setComponents(convertComponents(i.getComponents()));
		return o;
	}
	
	// Called by the second pass. Here we complete the conversion.
	Datatype convertDatatypeSecondPass(gov.nist.healthcare.hl7tools.domain.Datatype i, Datatype o) {
		o.setComponents(convertComponents(i.getComponents()));
		log.info("dt=" + o.getLabel() + " components=" + o.getComponents().size());
		return o;
	}

	// Called by the convertDatatypeSecondPass pass.
	List<Component> convertComponents(List<gov.nist.healthcare.hl7tools.domain.Component> i) {
		List<Component> o = new ArrayList<Component>();
		if (i != null) {
			for (gov.nist.healthcare.hl7tools.domain.Component c : i) {
				Component convertedComponent = convertComponent(c);
				o.add(convertedComponent);
			}
		}
		
		return o;
	}

	Component convertComponent(gov.nist.healthcare.hl7tools.domain.Component i) {
		String key = i.getDatatype().getName();
		Component o = new Component();

		try {
			Datatype dt = mapDatatypes.get(key);
 			o.setComment(i.getComment());
			o.setConfLength(Integer.toString(i.getConfLength()));
			o.setDatatype(dt.getId());
			o.setMaxLength(i.getMaxLength());
			o.setMinLength(i.getMinLength());
			o.setPosition(i.getPosition());
			o.setName(i.getDescription());
		} catch (NullPointerException e) {
			log.error("Datatype name=" + key + " not found in mapDatatypes.");
		}

		gov.nist.healthcare.hl7tools.domain.CodeTable ct = i.getCodeTable();
		if (ct != null) {
			String key1 = i.getCodeTable().getKey();
			Table tab = mapTables.get(key1);
			if (tab == null) {
				log.info("mapTables.get(" + key1 + ") was null description=" + i.getDescription());
			} else {
				o.setTable(tab.getId());
			}
		} else {
			log.debug("CodeTable for field is null. description=" + i.getDescription());
		}

		o.setUsage(convertUsage(i.getUsage()));
		return o;
	}

	Usage convertUsage(gov.nist.healthcare.hl7tools.domain.Usage i) {
		return Usage.fromValue(i.getValue());
	}

	String generateId() {
		return ObjectId.get().toString();
	}

	String OidId() {
		return "{ \"$oid\" : " + "\"" + generateId() + "\"" + "}";
	}
}
