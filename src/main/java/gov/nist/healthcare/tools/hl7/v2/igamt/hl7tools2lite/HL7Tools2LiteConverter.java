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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.bson.types.ObjectId;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

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
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DocumentMetaData;
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

	static Logger log = LoggerFactory.getLogger(HL7Tools2LiteConverter.class);

	CmdLineParser CLI = new CmdLineParser(this);
	public Profile profile;
	public IGLibrary ig;

	public Map<String, Datatype> mapDatatypes;
	public Map<String, Segment> mapSegments;
	public Map<String, Segment> mapSegmentsById;
	public Map<String, Table> mapTables;

	public Map<String, Profile> profiles = new HashMap<String, Profile>();
	public Map<String, IGLibrary> igLibraries = new HashMap<String, IGLibrary>();

	@Option(name = "-d", required = true, usage = "String value of database name")
	String dbName = "igamt";

	@Option(name = "-i", required = false, usage = "boolean value, If present, use existing ids else create new ids.")
	boolean existing;

	@Option(name = "-v", handler = StringArrayOptionHandler.class, required = true, usage = "String values of hl7Versions to process.")
	String[] hl7Versions;

	String hl7Version;

	public HL7DBService service = new HL7DBMockServiceImpl();

	MongoClient mongo;
	MongoOperations mongoOps;
	IGDocument igd;

	public HL7Tools2LiteConverter(String[] args) throws CmdLineException {
		super();
		try {
			CLI.parseArgument(args);
			mongo = new MongoClient("localhost", 27017);
			mongoOps = new MongoTemplate(new SimpleMongoDbFactory(mongo, dbName));
		} catch (CmdLineException e) {
			CLI.printUsage(System.out);
			throw e;
		}
		log.info("Main==>");
	}

	public void run() {
		log.info("start...");

		if (!existing) {
			log.info("Dropping mongo collection profile...");
			mongoOps.dropCollection(Table.class);
			mongoOps.dropCollection(Datatype.class);
			mongoOps.dropCollection(Segment.class);
			mongoOps.dropCollection(TableLibrary.class);
			mongoOps.dropCollection(DatatypeLibrary.class);
			mongoOps.dropCollection(SegmentLibrary.class);
			mongoOps.dropCollection(Message.class);
			mongoOps.dropCollection(IGDocument.class);
		}

		for (String hl7Version : hl7Versions) {
			try {
				this.hl7Version = hl7Version;
				ig = null;
				igd = null;
				Profile profile = doVersion(hl7Version);

				getIgd().addProfile(profile);
				getIgd().getMetaData().setHl7Version(hl7Version);
				getIgd().getMetaData().setDate(Constant.mdy.format(new Date()));

				for (Message msg : getIgd().getProfile().getMessages().getChildren()) {
					mongoOps.save(msg);
				}

				mongoOps.save(getIgd().getProfile().getTableLibrary());
				mongoOps.save(getIgd().getProfile().getDatatypeLibrary());
				mongoOps.save(getIgd().getProfile().getSegmentLibrary());
				mongoOps.save(getIgd());
				igLibraries.put(hl7Version, getIg());
			} catch (Exception e) {
				log.error("error", e);
			}
		}
		log.info("...end");
	}

	public Profile doVersion(String hl7Version) {
		log.info("hl7Version=" + hl7Version);

		try {
			List<String[]> messageArrayList = service.getMessageListByVersion(hl7Version);
			log.info("messageArrayList=" + messageArrayList.size());
			List<String> messageList = new ArrayList<String>();
			for (String[] ss : messageArrayList) {
				messageList.add(ss[0]);
			}
			log.info("messageList=" + messageList.size());

			log.info("getMessageLibrary" + getIg().getMessageLibrary().size());
			log.info("getSegmentLibrary" + getIg().getSegmentLibrary().size());
			log.info("getDatatypeLibrary" + getIg().getDatatypeLibrary().size());
			log.info("getCodeTableLibrary" + getIg().getCodeTableLibrary().size());
			profile = convert(getIg());
			profile.setId(generateId());
			profile.setScope(IGDocumentScope.HL7STANDARD);
			// ProfileMetaData pmd = createProfileMetaData();
			// profile.setMetaData(pmd);
		} catch (HL7DBServiceException e) {
			log.error("", e);
		}

		return profile;
	}

	IGLibrary getIg() {
		if (ig == null) {
			try {
				ig = service.buildIGFromMessageList(hl7Version, getMessageList());
			} catch (HL7DBServiceException e) {
				log.error("", e);
			}
		}
		return ig;
	}

	List<String> getMessageList() throws HL7DBServiceException {
		List<String[]> messageArrayList = service.getMessageListByVersion(hl7Version);
		log.info("messageArrayList=" + messageArrayList.size());
		List<String> messageList = new ArrayList<String>();
		for (String[] ss : messageArrayList) {
			messageList.add(ss[0]);
		}
		log.info("messageList=" + messageList.size());
		return messageList;
	}

	Map<String, Datatype> getMapDatatypes() {
		if (mapDatatypes == null) {
			mapDatatypes = new HashMap<String, Datatype>();
		}
		return mapDatatypes;
	}

	Map<String, Segment> getMapSegments() {
		if (mapSegments == null) {
			mapSegments = new HashMap<String, Segment>();
		}
		return mapSegments;
	}

	Map<String, Segment> getMapSegmentsById() {
		if (mapSegmentsById == null) {
			mapSegmentsById = new HashMap<String, Segment>();
		}
		return mapSegmentsById;
	}

	Map<String, Table> getMapTables() {
		if (mapTables == null) {
			mapTables = new HashMap<String, Table>();
		}
		return mapTables;
	}

	IGDocument getIgd() {
		if (igd == null) {
			igd = acquireIGDocument();
		}
		return igd;
	}

	IGDocument acquireIGDocument() {
		IGDocument igd = null;
		if (existing) {
			Criteria where = Criteria.where("scope").is(Constant.SCOPE.HL7STANDARD).and("metaData.hl7Version")
					.is(hl7Version);
			Query qry = Query.query(where);
			igd = mongoOps.findOne(qry, IGDocument.class);
		}
		if (igd == null) {
			log.error("No IGD found for " + hl7Version);
		}
		return igd;
	}

	DocumentMetaData createDocumentMetaData() {
		DocumentMetaData meta = new DocumentMetaData();
		meta.setOrgName("NIST");
		meta.setHl7Version(hl7Version);
		meta.setName("Default Name");
		meta.setDate(Constant.mdy.format(new Date()));
		return meta;
	}

	void setIgd(IGDocument igd) {
		this.igd = igd;
	}

	Profile acquireProfile() {
		Profile profile = null;
		if (existing && getIgd() != null) {
			profile = getIgd().getProfile();
		}
		if (profile == null) {
			log.error("No profile found for " + hl7Version);
		}
		return profile;
	}

	ProfileMetaData createProfileMetaData() {
		ProfileMetaData pmd = new ProfileMetaData();
		pmd.setOrgName("NIST");
		pmd.setHl7Version(hl7Version);
		pmd.setName("Default Name");
		return pmd;
	}

	public static void main(String[] args) {
		HL7Tools2LiteConverter app;
		try {
			app = new HL7Tools2LiteConverter(args);
			app.run();
		} catch (CmdLineException e) {
			log.error("", e);
		}
	}

	Profile convert(IGLibrary i) {
		Profile p = getIgd().getProfile();
		TableLibrary tabLib = convertTables(i.getCodeTableLibrary());
		DatatypeLibrary dtLib = convertDatatypes(i.getDatatypeLibrary());
		SegmentLibrary segLib = convertSegments(i.getSegmentLibrary());
		Messages msgs = convertMessages(i.getMessageLibrary());

		// List<Integer> poss = new ArrayList<Integer>();
		// for (Message msg : msgs.getChildren()) {
		// poss.add(msg.getPosition());
		// }
		// Collections.sort(poss);
		// log.info("poss=" + poss);

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

		String structID = assembleMessageStructID(i);
		Message o = acquireMessage(structID);
		int recur = 0;
		log.info("message structID=" + structID + " " + seq);
		o.getChildren().addAll(convertElements(checkChildren(i.getChildren()), recur));
		o.setDescription(i.getDescription());
		o.setEvent(i.getEvent().trim());
		o.setIdentifier("Default" + i.getEvent().trim() + "Identifier");
		o.setMessageType(i.getMessageType());
		o.setName(assembleMessageName(i));
		o.setScope(SCOPE.HL7STANDARD);
		o.setHl7Version(hl7Version);
		o.setStructID(structID);
		return o;
	}

	Message acquireMessage(String structID) {
		if (existing) {
			for (Message msg : getIgd().getProfile().getMessages().getChildren()) {
				if (structID.equals(msg.getStructID())) {
					return msg;
				}
			}
			log.error("Not found structID=" + structID);
		}
		return new Message();
	}

	String assembleMessageName(gov.nist.healthcare.hl7tools.domain.Message i) {
		String rval = i.getMessageType() + "^" + i.getEvent() + "^" + assembleMessageStructID(i);
		assert (rval != null);
		return rval;
	}

	String assembleMessageStructID(gov.nist.healthcare.hl7tools.domain.Message i) {
		String type = i.getMessageType();
		String event = i.getEvent();
		String rval = i.getMessageType() + (event != null && event.trim().length() > 0 ? "_" + event : "");
		assert (rval != null);
		return rval;
	}

	List<SegmentRefOrGroup> convertElements(List<gov.nist.healthcare.hl7tools.domain.Element> i, int recur) {
		List<SegmentRefOrGroup> o = new ArrayList<SegmentRefOrGroup>();
		for (gov.nist.healthcare.hl7tools.domain.Element el : i) {
			o.add(convertElement(el, recur));
		}
		return o;
	}

	SegmentRefOrGroup convertElement(gov.nist.healthcare.hl7tools.domain.Element i, int recur) {
		SegmentRefOrGroup o = null;
		switch (i.getType()) {
		case GROUP: {
			o = convertGroup(i, recur);
			break;
		}
		case SEGEMENT: {
			o = convertSegmentRef(i, recur);
			break;
		}
		default:
			log.error("Element was neither Group nor SegmentRef.");
			break;
		}
		return o;
	}

	Group convertGroup(gov.nist.healthcare.hl7tools.domain.Element i, int recur) {
		Group o = new Group();
		o.setComment(i.getComment());
		o.setMax(i.getMax());
		o.setMin(i.getMin());
		o.setName(i.getName());
		o.setPosition(i.getPosition());
		o.setUsage(convertUsage(i.getUsage()));
		o.getChildren().addAll(convertElements(checkChildren(i.getChildren()), recur++));
//		log.info("grp " + String.format("%" + recur + 1 * 2 + "d", recur) + " name=" + o.getName());
		return o;
	}

	List<gov.nist.healthcare.hl7tools.domain.Element> checkChildren(
			List<gov.nist.healthcare.hl7tools.domain.Element> i) {
		if (i == null) {
			return new ArrayList<gov.nist.healthcare.hl7tools.domain.Element>();
		} else
			return i;
	}

	SegmentRef convertSegmentRef(gov.nist.healthcare.hl7tools.domain.Element i, int recur) {
		SegmentRef o = new SegmentRef();
		o.setUsage(convertUsage(i.getUsage()));
		o.setMin(i.getMin());
		o.setMax(i.getMax());
		o.setPosition(i.getPosition());
		String key = i.getName();
		Segment ref = getMapSegments().get(key);
		if (ref != null) {
			if (ref.getId() != null) {
				log.trace("settingRef id=" + ref.getId() + " from key=" + key);
				if (getMapSegmentsById().get(ref.getId()) == null) {
					log.error("was null in mapSegmentsById id=" + ref.getId());
				}
				o.setRef(new SegmentLink(ref.getId(), ref.getName(), ref.getExt()));
			} else {
				log.error("segment.Id was null for element=" + i.getName());
			}
		} else {
			log.error("segment was null for element=" + i.getName());
		}
//		log.info("ref " + String.format("%" + recur + 1 * 2 + "d", recur) + " name=" + ref.getName());
		return o;
	}

	SegmentLibrary convertSegments(gov.nist.healthcare.hl7tools.domain.SegmentLibrary i) {
		SegmentLibrary o = acquireSegmentLibrary();
		for (String key : i.keySet()) {
			gov.nist.healthcare.hl7tools.domain.Segment sg = i.get(key);
			Segment seg = convertSegment(sg);
			seg.getLibIds().add(o.getId());
			mongoOps.save(seg);
			o.addSegment(new SegmentLink(seg.getId(), seg.getName(), seg.getExt()));
		}
		return o;
	}

	SegmentLibrary acquireSegmentLibrary() {
		if (existing) {
			return getIgd().getProfile().getSegmentLibrary();
		}
		SegmentLibrary o = new SegmentLibrary();
		o.setMetaData(createSegmentLibraryMetaData());
		mongoOps.save(o);
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
		Segment o = acquireSegment(i.getName());
		o.setComment(i.getComment());
		o.setDescription(i.getDescription());
		o.setFields(convertFields(checkFieldList(i.getFields())));
		o.setLabel(i.getName());
		o.setName(i.getName());
		o.setHl7Version(hl7Version);
		o.setScope(SCOPE.HL7STANDARD);
		String key = i.getName();
		if (!getMapSegments().containsKey(key)) {
			getMapSegments().put(key, o);
			mongoOps.save(o);
			getMapSegmentsById().put(o.getId(), o);
		} else {
			log.debug("Key=" + key + " already put id=" + o.getId());
		}
		return o;
	}

	Segment acquireSegment(String name) {
		Segment seg = null;
		if (existing) {
			Criteria where = Criteria.where("scope").is(Constant.SCOPE.HL7STANDARD).and("hl7Version").is(hl7Version)
					.and("name").is(name);
			Query qry = Query.query(where);
			seg = mongoOps.findOne(qry, Segment.class);
		}
		if (seg == null) {
			if (existing) {
				log.info("Segment not found in database. name=" + name);
			}
			seg = new Segment();
		}
		return seg;
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
		Datatype dt = getMapDatatypes().get(s);
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
			Table tab = getMapTables().get(key);
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
		TableLibrary o = acquireTableLibrary();
		for (String s : i.keySet()) {
			gov.nist.healthcare.hl7tools.domain.CodeTable ct = i.get(s);
			if (ct != null) {
				String key = ct.getKey();
				getMapTables().put(key, null);
			} else {
				log.debug("CodeTable is null!!");
			}
		}
		for (String s : i.keySet()) {
			gov.nist.healthcare.hl7tools.domain.CodeTable ct = i.get(s);
			if (ct != null) {
				String key = ct.getKey();
				Table tab = convertTable(ct);
				tab.getLibIds().add(o.getId());
				mongoOps.save(tab);
				o.addTable(new TableLink(tab.getId(), tab.getBindingIdentifier()));
				getMapTables().put(key, tab);
			} else {
				log.debug("CodeTable is null!!");
			}
		}
		return o;
	}

	TableLibrary acquireTableLibrary() {
		if (existing) {
			return getIgd().getProfile().getTableLibrary();
		}
		TableLibrary o = new TableLibrary();
		o.setMetaData(createTableLibraryMetaData());
		mongoOps.save(o);
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

		Table o = acquireTable(i.getKey());

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

	public Table acquireTable(String bindingIdentifier) {
		Table tab = null;
		if (existing) {
			Criteria where = Criteria.where("scope").is(Constant.SCOPE.HL7STANDARD).and("hl7Version").is(hl7Version)
					.and("bindingIdentifier").is(bindingIdentifier);
			Query qry = Query.query(where);
			tab = mongoOps.findOne(qry, Table.class);
		}
		if (tab == null) {
			if (existing) {
				log.info("Table not found in database. bindingIdentifier=" + bindingIdentifier);
			}
			tab = new Table();
		}
		return tab;
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
		DatatypeLibrary o = acquireDatatypeLibrary();
		convertDataTypesFirstPass(i);
		convertDataTypesSecondPass(i);
		for (Datatype dt : getMapDatatypes().values()) {
			dt.getLibIds().add(o.getId());
			mongoOps.save(dt);
			o.addDatatype(new DatatypeLink(dt.getId(), dt.getName(), dt.getExt()));
		}
		return o;
	}

	DatatypeLibrary acquireDatatypeLibrary() {
		if (existing) {
			return getIgd().getProfile().getDatatypeLibrary();
		}
		DatatypeLibrary o = new DatatypeLibrary();
		o.setMetaData(createDatatypeLibraryMetaData());
		mongoOps.save(o);
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
		// Datatypes o = new Datatypes();
		// List<String> dtIds = new ArrayList<String>();
		// String dtId = "AAA"; // dummy value to avoid null;
		for (String key : i.keySet()) {
			gov.nist.healthcare.hl7tools.domain.Datatype dt = i.get(key);
			Datatype convertedDt = convertDatatypeFirstPass(dt);
			mongoOps.save(convertedDt);
			// Key = dt.label and Value = the Datatype;
			String key1 = convertedDt.getLabel();
			assert (key.equals(key1)) : "key=" + key + " key1=" + key1;
			getMapDatatypes().put(key, convertedDt);
		}
		// dumpMap();
	}

	// We dumpMap as a debugging aid.
	void dumpMap() {
		for (Map.Entry<String, Datatype> entry : getMapDatatypes().entrySet()) {
			log.info("key=" + entry.getKey() + " val=" + entry.getValue());
		}
	}

	void convertDataTypesSecondPass(gov.nist.healthcare.hl7tools.domain.DatatypeLibrary i) {
		// Here we get our ouput object from the map instead of creating one.
		// If we were to create one here it would have a different id from the
		// one
		// in the map.

		// List<String> dtIds = new ArrayList<String>();
		// String dtId = "AAA"; // dummy value to avoid null;
		for (String key : i.keySet()) {
			gov.nist.healthcare.hl7tools.domain.Datatype dt = i.get(key);
			Datatype convertedDt = getMapDatatypes().get(key);
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
		Datatype o = acquireDatatype(i.getName());
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

	public Datatype acquireDatatype(String name) {
		Datatype dt = null;
		if (existing) {
			Criteria where = Criteria.where("scope").is(Constant.SCOPE.HL7STANDARD).and("hl7Version").is(hl7Version)
					.and("name").is(name);
			Query qry = Query.query(where);
			dt = mongoOps.findOne(qry, Datatype.class);
		}
		if (dt == null) {
			if (existing) {
				log.info("Datatype not found in database. name=" + name);
			}
			dt = new Datatype();
		}
		return dt;
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
				Datatype dt = getMapDatatypes().get(key);
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
			Table tab = getMapTables().get(key1);
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
