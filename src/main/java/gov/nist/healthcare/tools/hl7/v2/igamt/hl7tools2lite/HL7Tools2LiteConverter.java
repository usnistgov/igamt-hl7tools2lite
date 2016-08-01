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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import gov.nist.healthcare.hl7tools.domain.HL7Table;
import gov.nist.healthcare.hl7tools.domain.IGLibrary;
import gov.nist.healthcare.hl7tools.service.HL7DBMockServiceImpl;
import gov.nist.healthcare.hl7tools.service.HL7DBService;
import gov.nist.healthcare.hl7tools.service.HL7DBServiceException;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Code;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Component;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Constant;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Constant.SCOPE;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.ContentDefinition;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatype;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLibraryMetaData;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLink;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatypes;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Extensibility;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Field;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Group;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.IGDocument;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.IGDocumentScope;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Message;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Messages;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Profile;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.ProfileMetaData;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Segment;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentLibraryMetaData;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentLink;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentRef;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentRefOrGroup;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Stability;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Table;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.TableLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.TableLibraryMetaData;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.TableLink;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Usage;

// Converts from old to new.  We read old *.json into an object graph rooted on IGLibrary then convert
// into a graph rooted on Profile.  We then write the output to a set of new *.json files.

public class HL7Tools2LiteConverter implements Runnable {

	Logger log = LoggerFactory.getLogger(HL7Tools2LiteConverter.class);

	// public final static File OUTPUT_DIR = new File(System.getenv("IGAMT") +
	// "/profiles");
	// public static SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");

	public Profile profile;
	public IGLibrary ig;
	String hl7Version;

	public Map<String, Datatype> mapDatatypes;
	public Map<String, Segment> mapSegments;
	public Map<String, Segment> mapSegmentsById;
	public Map<String, Table> mapTables;

	public Map<String, Profile> profiles = new HashMap<String, Profile>();
	public Map<String, IGLibrary> igLibraries = new HashMap<String, IGLibrary>();

	public HL7DBService service = new HL7DBMockServiceImpl();
	List<String> hl7Versions;
	MongoClient mongo;
	MongoOperations mongoOps = null;

	public HL7Tools2LiteConverter(String[] hl7Versions) {
		super();
		this.hl7Versions = Arrays.asList(hl7Versions);
	}

	public HL7Tools2LiteConverter() {
		super();
		this.hl7Versions = service.getSupportedHL7Versions();
	}

	public void run() {
		mongoOps = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "igamt1"));
		mongo = new MongoClient("localhost", 27017);
		// db = mongo.getDB("igamt");
		// collIGD = db.getCollection("igdocument");
		log.info("start...");
		log.info("Dropping mongo collection profile...");
		mongoOps.dropCollection(Table.class);
		mongoOps.dropCollection(Datatype.class);
		mongoOps.dropCollection(Segment.class);
		mongoOps.dropCollection(TableLibrary.class);
		mongoOps.dropCollection(DatatypeLibrary.class);
		mongoOps.dropCollection(SegmentLibrary.class);
		mongoOps.dropCollection(Message.class);
		mongoOps.dropCollection(IGDocument.class);

		for (String hl7Version : hl7Versions) {
			try {
				this.hl7Version = hl7Version;
				Profile profile = doVersion(hl7Version);
				
			    List<Integer> poss1 = new ArrayList<Integer>();
			    for (Message msg : profile.getMessages().getChildren()) {
			    	poss1.add(msg.getPosition());
			    }
			    Collections.sort(poss1);
		    	log.info("poss1=" + poss1);
		    	
				IGDocument igd = createIGDocument();
				igd.addProfile(profile);
			    List<Integer> poss2 = new ArrayList<Integer>();
			    for (Message msg : profile.getMessages().getChildren()) {
			    	poss2.add(msg.getPosition());
			    }
			    Collections.sort(poss2);
		    	log.info("poss2=" + poss2);

		    	igd.getMetaData().setHl7Version(hl7Version);
				igd.getMetaData().setDate(Constant.mdy.format(new Date()));
				
				
				mongoOps.insertAll(igd.getProfile().getMessages().getChildren());
			    List<Integer> poss3 = new ArrayList<Integer>();
			    for (Message msg : igd.getProfile().getMessages().getChildren()) {
			    	poss3.add(msg.getPosition());
			    }
			    Collections.sort(poss3);
		    	log.info("poss3=" + poss3);
		    	
		    	
				mongoOps.insert(igd.getProfile().getTableLibrary());
				mongoOps.insert(igd.getProfile().getDatatypeLibrary());
				mongoOps.insert(igd.getProfile().getSegmentLibrary());
				mongoOps.insert(igd);
				igLibraries.put(hl7Version, ig);
			} catch (Exception e) {
				log.error("error", e);
			}
		}
		log.info("...end");
	}

	public Profile doVersion(String hl7Version) {
		log.info("hl7Version=" + hl7Version);
		mapDatatypes = new HashMap<String, Datatype>();
		mapSegments = new HashMap<String, Segment>();
		mapSegmentsById = new HashMap<String, Segment>();
		mapTables = new HashMap<String, Table>();

		try {
			List<String[]> messageArrayList = service.getMessageListByVersion(hl7Version);
			log.info("messageArrayList=" + messageArrayList.size());
			List<String> messageList = new ArrayList<String>();
			for (String[] ss : messageArrayList) {
				messageList.add(ss[0]);
			}
			log.info("messageList=" + messageList.size());

			ig = service.buildIGFromMessageList(hl7Version, messageList);
			log.info("getMessageLibrary" + ig.getMessageLibrary().size());
			log.info("getSegmentLibrary" + ig.getSegmentLibrary().size());
			log.info("getDatatypeLibrary" + ig.getDatatypeLibrary().size());
			log.info("getCodeTableLibrary" + ig.getCodeTableLibrary().size());
			profile = convert(ig);
			profile.setId(generateId());
			profile.setScope(IGDocumentScope.HL7STANDARD);
			ProfileMetaData pmd = createProfileMetadata();
			profile.setMetaData(pmd);
		} catch (HL7DBServiceException e) {
			log.error("", e);
		}

		return profile;
	}

	IGDocument createIGDocument() {
		IGDocument igd = new IGDocument();
		igd.setScope(IGDocumentScope.HL7STANDARD);
		return igd;
	}

	ProfileMetaData createProfileMetadata() {
		ProfileMetaData pmd = new ProfileMetaData();
		pmd.setOrgName("NIST");
		pmd.setHl7Version(hl7Version);
		pmd.setName("Default Name");
		return pmd;
	}

	public static void main(String[] args) {
		HL7Tools2LiteConverter app;
		if (args.length > 0) {
			app = new HL7Tools2LiteConverter(args);
		} else {
			app = new HL7Tools2LiteConverter();
		}
		app.run();
	}

	Profile convert(IGLibrary i) {
		Profile p = new Profile();
		TableLibrary tabLib = convertTables(i.getCodeTableLibrary());
		DatatypeLibrary dtLib = convertDatatypes(i.getDatatypeLibrary());
		SegmentLibrary segLib = convertSegments(i.getSegmentLibrary());
		Messages msgs = convertMessages(i.getMessageLibrary());

	    List<Integer> poss = new ArrayList<Integer>();
	    for (Message msg : msgs.getChildren()) {
	    	poss.add(msg.getPosition());
	    }
	    Collections.sort(poss);
    	log.info("poss=" + poss);
		
		p.setDatatypeLibrary(dtLib);
		p.setSegmentLibrary(segLib);
		p.setTableLibrary(tabLib);
		p.setMessages(msgs);
		return p;
	}

	Messages convertMessages(gov.nist.healthcare.hl7tools.domain.MessageLibrary i) {
		Messages o = new Messages();
		int seq = 1;
		for (String key : i.keySet()) {
			gov.nist.healthcare.hl7tools.domain.Message msg = i.get(key);
			o.addMessage(convertMessage(msg, seq));
			seq++;
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
		o.setHl7Version(hl7Version);
		assert (o.getName() != null);
//		o.setPosition(seq);
		o.setStructID(assembleMessageStructID(i));
		log.info("name=" + o.getName() + " pos=" + o.getPosition());
		return o;
	}

	String assembleMessageName(gov.nist.healthcare.hl7tools.domain.Message i) {
		String rval = i.getMessageType() + "^" + i.getEvent() + "^" + assembleMessageStructID(i);
		assert (rval != null);
		return rval;
	}

	String assembleMessageStructID(gov.nist.healthcare.hl7tools.domain.Message i) {
		String rval = i.getMessageType() + "_" + i.getEvent();
		assert (rval != null);
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
				o.setRef(new SegmentLink(ref.getId(), ref.getName(), ref.getExt()));
			} else {
				log.error("segment.Id was null for element=" + i.getName());
			}
		} else {
			log.error("segment was null for element=" + i.getName());
		}
		return o;
	}

	SegmentLibrary convertSegments(gov.nist.healthcare.hl7tools.domain.SegmentLibrary i) {
		SegmentLibrary o = new SegmentLibrary();
		for (String key : i.keySet()) {
			gov.nist.healthcare.hl7tools.domain.Segment sg = i.get(key);
			Segment seg = convertSegment(sg);
			o.addSegment(new SegmentLink(seg.getId(), seg.getName(), seg.getExt()));
		}
		o.setMetaData(createSegmentLibraryMetaData());
		return o;
	}

	SegmentLibraryMetaData createSegmentLibraryMetaData() {
		SegmentLibraryMetaData segMetaData = new SegmentLibraryMetaData();
		segMetaData.setSegmentLibId(UUID.randomUUID().toString());
		segMetaData.setDate(Constant.mdy.format(new Date()));
		segMetaData.setHl7Version(hl7Version);
		segMetaData.setOrgName("NIST");
		return segMetaData;
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
		o.setHl7Version(hl7Version);
		o.setScope(SCOPE.HL7STANDARD);
		String key = i.getName();
		if (!mapSegments.containsKey(key)) {
			mapSegments.put(key, o);
			mongoOps.insert(o);
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
		Integer confLength = i.getConfLength();
		o.setConfLength(confLength < 0 ? Integer.toString(confLength) : "1");
		o.setDatatype(new DatatypeLink(dt.getId(), dt.getName(), dt.getExt()));
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
				o.setTable(new TableLink(tab.getId(), tab.getName()));
			}
		} else {
			log.debug("CodeTable for field is null. description=" + i.getDescription());
		}
		o.setUsage(convertUsage(i.getUsage()));

		return o;
	}

	TableLibrary convertTables(gov.nist.healthcare.hl7tools.domain.CodeTableLibrary i) {
		TableLibrary o = new TableLibrary();
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
				mongoOps.insert(tab);
				o.addTable(new TableLink(tab.getId(), tab.getBindingIdentifier()));
				mapTables.put(key, tab);
			} else {
				log.debug("CodeTable is null!!");
			}
		}
		o.setMetaData(createTableLibraryMetaData());
		return o;
	}

	TableLibraryMetaData createTableLibraryMetaData() {
		TableLibraryMetaData tabMetaData = new TableLibraryMetaData();
		tabMetaData.setDate(Constant.mdy.format(new Date()));
		tabMetaData.setHl7Version(hl7Version);
		tabMetaData.setTableLibId(UUID.randomUUID().toString());
		tabMetaData.setOrgName("NIST");
		return tabMetaData;
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
		o.setHl7Version(hl7Version);
		o.setScope(SCOPE.HL7STANDARD);
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

	DatatypeLibrary convertDatatypes(gov.nist.healthcare.hl7tools.domain.DatatypeLibrary i) {
		convertDataTypesFirstPass(i);
		convertDataTypesSecondPass(i);
		DatatypeLibrary o = new DatatypeLibrary();
		for (Datatype dt : mapDatatypes.values()) {
			o.addDatatype(new DatatypeLink(dt.getId(), dt.getName(), dt.getExt()));
		}
		o.setMetaData(createDatatypeLibraryMetaData());
		return o;
	}

	DatatypeLibraryMetaData createDatatypeLibraryMetaData() {
		DatatypeLibraryMetaData dtMetaData = new DatatypeLibraryMetaData();
		dtMetaData.setDatatypeLibId(UUID.randomUUID().toString());
		dtMetaData.setDate(Constant.mdy.format(new Date()));
		dtMetaData.setHl7Version(hl7Version);
		dtMetaData.setName(Constant.DefaultDatatypeLibrary);
		dtMetaData.setOrgName("NIST");
		return dtMetaData;
	};

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
			mongoOps.insert(convertedDt);
			// Key = dt.label and Value = the Datatype;
			String key1 = convertedDt.getLabel();
			assert (key.equals(key1)) : "key=" + key + " key1=" + key1;
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
		// If we were to create one here it would have a different id from the
		// one
		// in the map.

		List<String> dtIds = new ArrayList<String>();
		String dtId = "AAA"; // dummy value to avoid null;
		for (String key : i.keySet()) {
			gov.nist.healthcare.hl7tools.domain.Datatype dt = i.get(key);
			Datatype convertedDt = mapDatatypes.get(key);
			// Key = dt.label and Value = the Datatype;
			String key1 = convertedDt.getLabel();
			assert (key.equals(key1)) : "key=" + key + " key1=" + key1;
			Datatype convertedDt2 = convertDatatypeSecondPass(dt, convertedDt);
			mongoOps.save(convertedDt2);
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
		o.setHl7Version(hl7Version);
		o.setScope(SCOPE.HL7STANDARD);
		// We refrain from converting components until the second pass.
		// o.setComponents(convertComponents(i.getComponents()));
		return o;
	}

	// Called by the second pass. Here we complete the conversion.
	Datatype convertDatatypeSecondPass(gov.nist.healthcare.hl7tools.domain.Datatype i, Datatype o) {
		o.setComponents(convertComponents(i.getComponents()));
		// log.info("dt=" + o.getLabel() + " components=" +
		// o.getComponents().size());
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
		Component o = new Component();
		String key = "";
		try {
			gov.nist.healthcare.hl7tools.domain.Datatype iDt = i.getDatatype();
			if (iDt != null) {
				key = iDt.getName();
			}
			try {
				DatatypeLink link = null;
				Datatype dt = mapDatatypes.get(key);
				if (dt == null) {
					link = new DatatypeLink(null, null, null);
				} else {
					if (dt.getId() == null) {
						throw new NullPointerException("dt=" + dt.toString());
					}
					link = new DatatypeLink(dt.getId(), dt.getName(), dt.getExt());
				}
				o.setComment(i.getComment());
				Integer confLength = i.getConfLength();
				o.setConfLength(confLength < 0 ? Integer.toString(confLength) : "1");
				o.setDatatype(link);
				o.setMaxLength(i.getMaxLength());
				o.setMinLength(i.getMinLength());
				o.setPosition(i.getPosition());
				o.setName(i.getDescription());
			} catch (NullPointerException e) {
				log.error("Datatype name=" + key + " not found in mapDatatypes.", e);
			}
		} catch (NullPointerException e) {
			log.error("Component datatype=" + i.getId() + " " + i.getDescription() + " not found.", e);
		}

		gov.nist.healthcare.hl7tools.domain.CodeTable ct = i.getCodeTable();
		if (ct != null) {
			String key1 = i.getCodeTable().getKey();
			Table tab = mapTables.get(key1);
			if (tab == null) {
				log.info("mapTables.get(" + key1 + ") was null description=" + i.getDescription());
			} else {
				o.setTable(new TableLink(tab.getId(), tab.getName()));
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

	private String readMongoId(DBObject source) {
		if (source.get("_id") != null) {
			if (source.get("_id") instanceof ObjectId) {
				return ((ObjectId) source.get("_id")).toString();
			} else {
				return (String) source.get("_id");
			}
		} else if (source.get("id") != null) {
			if (source.get("id") instanceof ObjectId) {
				return ((ObjectId) source.get("id")).toString();
			} else {
				return (String) source.get("id");
			}
		}
		return null;
	}

}
