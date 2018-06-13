/**
 * This software was developed at the National Institute of Standards and Technology by employees of
 * the Federal Government in the course of their official duties. Pursuant to title 17 Section 105
 * of the United States Code this software is not subject to copyright protection and is in the
 * public domain. This is an experimental system. NIST assumes no responsibility whatsoever for its
 * use by other parties, and makes no guarantees, expressed or implied, about its quality,
 * reliability, or any other characteristic. We would appreciate acknowledgment if the software is
 * used. This software can be redistributed and/or modified freely provided that any derivative
 * works bear some notice that they are derived from it, and any modified versions bear some notice
 * that they have been modified.
 */
package gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
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

import gov.nist.healthcare.hl7tools.domain.IGLibrary;
import gov.nist.healthcare.hl7tools.service.HL7DBMockServiceImpl;
import gov.nist.healthcare.hl7tools.service.HL7DBService;
import gov.nist.healthcare.hl7tools.service.HL7DBServiceException;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Code;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Comment;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Component;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Constant;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Constant.SCOPE;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Constant.STATUS;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Constant.SourceType;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.ContentDefinition;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DataElement;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatype;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLibraryMetaData;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLink;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DocumentMetaData;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DynamicMappingDefinition;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DynamicMappingItem;
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
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.ValueSetBinding;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.ValueSetOrSingleCodeBinding;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.VariesMapItem;

// Converts from old to new. We read old *.json into an object graph rooted on IGLibrary then
// convert
// into a graph rooted on Profile. We then write the output to a set of new *.json files.

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

  @Option(name = "-d", aliases = "--database", required = true, usage = "Name of the database.")
  String dbName = "igamt";

  @Option(name = "-use", aliases = "--use", required = false,
      usage = "If present, use existing ids else create new ids.")
  boolean existing;

  @Option(name = "-v", aliases = "--versions", handler = StringArrayOptionHandler.class,
      required = true, usage = "String values of hl7Versions to process.")
  String[] hl7Versions;

  @Option(name = "-h", aliases = "--help", usage = "Print usage.")
  boolean help;

  String hl7Version;

  public HL7DBService service = new HL7DBMockServiceImpl();

  MongoClient mongo;
  MongoOperations mongoOps;
  IGDocument igd;

  POIFSFileSystem fs = null;
  Workbook thirdLevelWorkbook = null;
  Workbook ce_Workbook = null;

  FileOutputStream fileOut = null;
  boolean write = true;



  HashMap<String, Map<String, String>> threeLevelsDatatypesSubstituionMap = null;

  private void initThreeLevelsDatatypesSubstituionMap() {
    threeLevelsDatatypesSubstituionMap = new HashMap<String, Map<String, String>>();

    Map<String, String> versionMap = new HashMap<String, String>();
    threeLevelsDatatypesSubstituionMap.put("2.3", versionMap);
    versionMap.put("CE", "ST");
    versionMap.put("HD", "IS");

    versionMap = new HashMap<String, String>();
    threeLevelsDatatypesSubstituionMap.put("2.3.1", versionMap);
    versionMap.put("CE", "ST");
    versionMap.put("HD", "IS");

    versionMap = new HashMap<String, String>();
    threeLevelsDatatypesSubstituionMap.put("2.4", versionMap);
    versionMap.put("CE", "ST");
    versionMap.put("TS", "ST");


    versionMap = new HashMap<String, String>();
    threeLevelsDatatypesSubstituionMap.put("2.5", versionMap);
    versionMap.put("CE", "ST");
    versionMap.put("TS", "DTM");


    versionMap = new HashMap<String, String>();
    threeLevelsDatatypesSubstituionMap.put("2.5.1", versionMap);
    versionMap.put("CE", "ST");
    versionMap.put("TS", "DTM");

    versionMap = new HashMap<String, String>();
    threeLevelsDatatypesSubstituionMap.put("2.6", versionMap);
    versionMap.put("CWE", "ST");

  }



  public HL7Tools2LiteConverter(String[] args) throws CmdLineException {
    super();
    try {
      CLI.parseArgument(args);
      if (help) {
        throw new CmdLineException(CLI, "", null);
      }
      mongo = new MongoClient("localhost", 27017);
      mongoOps = new MongoTemplate(new SimpleMongoDbFactory(mongo, dbName));
      initThreeLevelsDatatypesSubstituionMap();
    } catch (CmdLineException e) {
      CLI.printUsage(System.out);
      System.exit(0);
    }
    log.info("Main==>");
  }

  @Override
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


    thirdLevelWorkbook = new HSSFWorkbook();
    ce_Workbook = new HSSFWorkbook();
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
        fixDatatypeRecursion(hl7Version);
        igLibraries.put(hl7Version, getIg());
        fixCE_(hl7Version);
        fixStandardThirdLevelComplexDatatype(hl7Version);
      } catch (Exception e) {
        log.error("error", e);
      }
    }

    try {
      for (String hl7Version : hl7Versions) {
        this.hl7Version = hl7Version;
        addMissingDatatypes(hl7Version);
        changeDR_DTMtoDR_NIST(hl7Version);
      }
    } catch (Exception e) {
      log.error("error", e);
    }

    FileOutputStream fileOut;
    try {
      File file = new File("./src/main/resources/ThirdLevelDatatypeResults.xls");
      if (!file.exists()) {
        file.createNewFile();
      }
      fileOut = new FileOutputStream(file);
      thirdLevelWorkbook.write(fileOut);
      fileOut.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    try {
      File file = new File("./src/main/resources/CE_Results.xls");
      if (!file.exists()) {
        file.createNewFile();
      }
      fileOut = new FileOutputStream(file);
      ce_Workbook.write(fileOut);
      fileOut.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }



    // pw.flush();
    // pw.close();
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
      Criteria where = Criteria.where("scope").is(Constant.SCOPE.HL7STANDARD)
          .and("metaData.hl7Version").is(hl7Version);
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
    meta.setStatus(STATUS.PUBLISHED.toString());
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
    pmd.setStatus(STATUS.PUBLISHED.toString());
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
    log.debug("message structID=" + structID + " " + seq);
    o.getChildren().clear();
    List<SegmentRefOrGroup> newSegmentRefOrGroup =
        convertElements(checkChildren(i.getChildren()), o, null);
    o.getChildren().clear();
    o.getChildren().addAll(newSegmentRefOrGroup);
    o.setDescription(i.getDescription());
    o.setEvent(i.getEvent().trim());
    o.setIdentifier("Default " + i.getEvent().trim() + " Identifier");
    o.setMessageType(i.getMessageType());
    o.setName(assembleMessageName(i));
    o.setScope(SCOPE.HL7STANDARD);
    o.setStatus(STATUS.PUBLISHED);
    o.setHl7Version(hl7Version);
    o.setStructID(structID);
    o.setHl7Section(i.getSection());
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
    String rval =
        i.getMessageType() + (event != null && event.trim().length() > 0 ? "_" + event : "");
    assert (rval != null);
    return rval;
  }

  List<SegmentRefOrGroup> convertElements(List<gov.nist.healthcare.hl7tools.domain.Element> i,
      Message message, String parentPath) {
    List<SegmentRefOrGroup> o = new ArrayList<SegmentRefOrGroup>();
    for (gov.nist.healthcare.hl7tools.domain.Element el : i) {
      o.add(convertElement(el, message, parentPath));
    }
    return o;
  }

  SegmentRefOrGroup convertElement(gov.nist.healthcare.hl7tools.domain.Element i, Message message,
      String parentPath) {
    SegmentRefOrGroup o = null;
    switch (i.getType()) {
      case GROUP: {
        o = convertGroup(i, message, parentPath);
        break;
      }
      case SEGEMENT: {
        o = convertSegmentRef(i, message, parentPath);
        break;
      }
      default:
        log.error("Element was neither Group nor SegmentRef.");
        break;
    }
    return o;
  }

  Group convertGroup(gov.nist.healthcare.hl7tools.domain.Element i, Message message,
      String parentPath) {
    Group o = new Group();
    o.setMax(i.getMax());
    o.setMin(i.getMin());
    o.setName(i.getName());

    String newGroupName = o.getName();
    newGroupName = newGroupName.replaceAll(" ", "_");
    String[] groupNameSplit = newGroupName.split("\\.");
    if (groupNameSplit.length > 0) {
      newGroupName = groupNameSplit[groupNameSplit.length - 1];
      o.setName(newGroupName);
    }

    o.setPosition(i.getPosition());
    o.setUsage(convertUsage(i.getUsage()));
    String oPath = getSegRefOrGroupPath(parentPath, o);
    if (i.getComment() != null && !i.getComment().equals("")) {
      Comment comment = new Comment();
      comment.setDescription(i.getComment());
      comment.setLastUpdatedDate(new Date());
      comment.setLocation(oPath);
      message.addComment(comment);
    }
    o.getChildren().addAll(convertElements(checkChildren(i.getChildren()), message, oPath));
    checkIntegrity(o);
    return o;
  }

  List<gov.nist.healthcare.hl7tools.domain.Element> checkChildren(
      List<gov.nist.healthcare.hl7tools.domain.Element> i) {
    if (i == null) {
      return new ArrayList<gov.nist.healthcare.hl7tools.domain.Element>();
    } else
      return i;
  }

  SegmentRef convertSegmentRef(gov.nist.healthcare.hl7tools.domain.Element i, Message message,
      String parentPath) {
    SegmentRef o = new SegmentRef();
    o.setUsage(convertUsage(i.getUsage()));
    o.setMin(i.getMin());
    o.setMax(i.getMax());
    o.setPosition(i.getPosition());
    String key = i.getName();
    Segment ref = getMapSegments().get(key);
    if (i.getComment() != null && !i.getComment().equals("")) {
      Comment comment = new Comment();
      comment.setDescription(i.getComment());
      comment.setLastUpdatedDate(new Date());
      comment.setLocation(getSegRefOrGroupPath(parentPath, o));
      message.addComment(comment);
    }
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
    checkIntegrity(o);
    return o;
  }


  String getSegRefOrGroupPath(String parentPath, SegmentRefOrGroup o) {
    return parentPath == null ? o.getPosition() + "" : parentPath + "." + o.getPosition();
  }


  SegmentLibrary convertSegments(gov.nist.healthcare.hl7tools.domain.SegmentLibrary i) {
    SegmentLibrary o = acquireSegmentLibrary();
    o.setScope(SCOPE.HL7STANDARD);
    getMapSegments().clear();
    getMapSegmentsById().clear();
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
    o.setScope(SCOPE.HL7STANDARD);
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
    if (o.getDescription() == null || o.getDescription().equals("")) {
      if (o.getName().equals("UB1")) {
        o.setDescription("UB82");
      }
    }

    o.setLabel(i.getName());
    o.setName(i.getName());
    o.setHl7Version(hl7Version);
    o.setScope(SCOPE.HL7STANDARD);
    o.setStatus(STATUS.PUBLISHED);
    o.setHl7Section(i.getSection());


    if (o.getName().equals("OBX")) {
      DynamicMappingDefinition dmDefinition = new DynamicMappingDefinition();
      VariesMapItem structure = new VariesMapItem();
      structure.setHl7Version(hl7Version);
      structure.setSegmentName(o.getName());
      structure.setTargetLocation("5");
      structure.setReferenceLocation("2");
      dmDefinition.setMappingStructure(structure);
      dmDefinition.setDynamicMappingItems(new ArrayList<DynamicMappingItem>());
      o.setDynamicMappingDefinition(dmDefinition);
    }


    convertFields(checkFieldList(i.getFields()), o);
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
      Criteria where = Criteria.where("scope").is(Constant.SCOPE.HL7STANDARD).and("hl7Version")
          .is(hl7Version).and("name").is(name);
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

  List<gov.nist.healthcare.hl7tools.domain.Field> checkFieldList(
      List<gov.nist.healthcare.hl7tools.domain.Field> i) {
    if (i == null) {
      return new ArrayList<gov.nist.healthcare.hl7tools.domain.Field>();
    }
    return i;
  }

  void convertFields(List<gov.nist.healthcare.hl7tools.domain.Field> i, Segment parent) {
    List<Field> o = new ArrayList<Field>();
    for (gov.nist.healthcare.hl7tools.domain.Field f : i) {
      o.add(convertField(f, parent));
    }
    parent.setFields(o);
  }

  Field convertField(gov.nist.healthcare.hl7tools.domain.Field i, Segment parent) {
    String s = i.getDatatype().getName();
    Field o = new Field();
    Datatype dt = getMapDatatypes().get(s);
    // Integer confLength = i.getConfLength();
    // o.setConfLength(confLength < 0 ? Integer.toString(confLength) : "1");
    o.setConfLength(
        i.getConfLength() != null && i.getConfLength().equals("-1") ? "" : i.getConfLength());
    if ("0".equals(o.getConfLength())) {
      o.setConfLength(DataElement.LENGTH_NA);
    }
    o.setDatatype(new DatatypeLink(dt.getId(), dt.getName(), dt.getExt()));
    o.setItemNo(i.getItemNo());
    o.setMax(i.getMax());
    o.setMin(i.getMin());
    o.setMinLength(i.getMinLength() + "");
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
        // o.getTables().add(new TableLink(tab.getId(), tab.getBindingIdentifier()));
        ValueSetBinding vsb = new ValueSetBinding();
        vsb.setLocation(o.getPosition() + "");
        vsb.setTableId(tab.getId());
        vsb.setUsage(o.getUsage());

        boolean found = false;
        for (ValueSetOrSingleCodeBinding b : parent.getValueSetBindings()) {
          if (b instanceof ValueSetBinding && b.getTableId().equals(vsb.getTableId())) {
            found = true;
          }
        }
        if (!found) {
          parent.addValueSetBinding(vsb);
        }
      }
    } else {
      log.debug("CodeTable for field is null. description=" + i.getDescription());
    }
    o.setUsage(convertUsage(i.getUsage()));
    if (o.getUsage().equals(Usage.B) || o.getUsage().equals(Usage.W)) {
      o.setUsage(Usage.X);
    }

    if (i.getComment() != null && !i.getComment().equals("")) {
      Comment comment = new Comment();
      comment.setDescription(i.getComment());
      comment.setLastUpdatedDate(new Date());
      comment.setLocation(i.getPosition() + "");
      parent.addComment(comment);
    }
    checkIntegrity(o);
    return o;
  }


  TableLibrary convertTables(gov.nist.healthcare.hl7tools.domain.CodeTableLibrary i) {
    TableLibrary o = acquireTableLibrary();
    o.setScope(SCOPE.HL7STANDARD);
    getMapTables().clear();
    for (String s : i.keySet()) {
      gov.nist.healthcare.hl7tools.domain.CodeTable ct = i.get(s);
      if (ct != null) {
        String key = ct.getKey();
        getMapTables().put(key, null);
      } else {
        log.info("1 CodeTable is null!!");
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
        log.info("2 CodeTable is null!!");
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

    Set<String> codeSystems = new HashSet<String>();
    for (Code c : o.getCodes()) {
      if (c.getCodeSystem() != null && !c.getCodeSystem().isEmpty()) {
        codeSystems.add(c.getCodeSystem());
      }
    }
    o.setCodeSystems(codeSystems);

    // Static OR Dynamic (enum) no condition as per Woo. Use Static.
    o.setStability(Stability.fromValue("Undefined"));
    // Use ContentDefinition.Extensional.
    o.setContentDefinition(ContentDefinition.fromValue("Undefined"));
    o.setExtensibility(Extensibility.fromValue("Undefined"));
    o.setDescription(null);
    o.setHl7Version(hl7Version);
    o.setScope(SCOPE.HL7STANDARD);
    o.setStatus(STATUS.PUBLISHED);
    o.setDefPreText(o.getDescription());
    o.setSourceType(SourceType.INTERNAL);

    int numberOfCodes = o.getCodes().size();
    o.setNumberOfCodes(numberOfCodes);


    // if (i instanceof HL7Table) {
    // HL7Table ii = (HL7Table) i;
    // // HL7 = Closed; User = Open (enum)
    // Extensibility ext = ("HL7".equals(ii.getType()) ? Extensibility.Closed : Extensibility.Open);
    // o.setExtensibility(ext);
    // } else {
    // log.debug("No type");
    // }
    return o;
  }

  public Table acquireTable(String bindingIdentifier) {
    Table tab = null;
    if (existing) {
      Criteria where = Criteria.where("scope").is(Constant.SCOPE.HL7STANDARD).and("hl7Version")
          .is(hl7Version).and("bindingIdentifier").is(bindingIdentifier);
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
    if (o.getCodeUsage() == null) {
      o.setCodeUsage("P");
    } else if (!o.getCodeUsage().equals("R") && !o.getCodeUsage().equals("P")
        && !o.getCodeUsage().equals("E")) {
      o.setCodeUsage("P");
    }
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
    o.setScope(SCOPE.HL7STANDARD);
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
    getMapDatatypes().clear();
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
    if (o.getDescription() == null || o.getDescription().equals("")) {
      o.setDescription("No Description");
    }
    o.setComment(i.getComment());
    o.setUsageNote(i.getUsageNotes());
    o.setHl7Version(hl7Version);
    o.getHl7versions().add(hl7Version);
    o.setScope(SCOPE.HL7STANDARD);
    o.setStatus(STATUS.PUBLISHED);
    o.setHl7Section(i.getSection());
    // We refrain from converting components until the second pass.
    // o.setComponents(convertComponents(i.getComponents()));
    return o;
  }

  Datatype findDRDTM(String hl7Version) {
    Datatype d = getDataType("DR_NIST", hl7Version);
    if (d == null) {
      d = getDataType("DR", hl7Version);
      d.setName("DR_NIST");
      d.setId(null);
      d.setScope(SCOPE.HL7STANDARD);
      List<Component> components = d.getComponents();
      if (components != null && !components.isEmpty()) {
        Datatype dtm = getDataType("DTM", hl7Version);
        if (dtm != null) {
          for (Component component : components) {
            if (component.getDatatype() != null && component.getDatatype().getName() != null
                && component.getDatatype().getName().equals("TS")) {
              component.setDatatype(new DatatypeLink(dtm.getId(), dtm.getName(), dtm.getExt()));
            }
          }
        }
      }
      mongoOps.save(d);
    }
    return d;
  }

  private Datatype getDataType(String name, String hl7Version) {
    Criteria where = Criteria.where("scope").is(Constant.SCOPE.HL7STANDARD).and("hl7Version")
        .is(hl7Version).and("name").is(name);
    Query qry = Query.query(where);
    Datatype dt = mongoOps.findOne(qry, Datatype.class);
    return dt;
  }



  public Datatype acquireDatatype(String name) {
    Datatype dt = null;
    try {
      if (existing) {
        dt = getDataType(name, hl7Version);
      }
      if (dt == null) {
        if (existing) {
          log.info("Datatype not found in database. name=" + name);
        }
        dt = new Datatype();
      }
    } catch (RuntimeException e) {
      System.out.println("Failed to aquire datatype " + name);
      e.printStackTrace();
    } catch (Exception e) {
      System.out.println("Failed to aquire datatype " + name);
      e.printStackTrace();
    }
    return dt;
  }

  // Called by the second pass. Here we complete the conversion.
  Datatype convertDatatypeSecondPass(gov.nist.healthcare.hl7tools.domain.Datatype i, Datatype o) {
    convertComponents(i.getComponents(), o);
    return o;
  }

  // Called by the convertDatatypeSecondPass pass.
  void convertComponents(List<gov.nist.healthcare.hl7tools.domain.Component> i, Datatype parent) {
    List<Component> o = new ArrayList<Component>();
    if (i != null) {
      for (gov.nist.healthcare.hl7tools.domain.Component c : i) {
        Component convertedComponent = convertComponent(c, parent);
        o.add(convertedComponent);
      }
    }
    parent.setComponents(o);
  }

  Component convertComponent(gov.nist.healthcare.hl7tools.domain.Component i, Datatype parent) {
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
        o.setConfLength(
            i.getConfLength() != null && i.getConfLength().equals("-1") ? "" : i.getConfLength());
        if ("0".equals(o.getConfLength())) {
          o.setConfLength(DataElement.LENGTH_NA);
        }
        o.setDatatype(link);
        o.setMaxLength(i.getMaxLength());
        o.setMinLength(i.getMinLength() + "");
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
        // o.getTables().add(new TableLink(tab.getId(), tab.getBindingIdentifier()));
        ValueSetBinding vsb = new ValueSetBinding();
        vsb.setLocation(o.getPosition() + "");
        vsb.setUsage(o.getUsage());
        vsb.setTableId(tab.getId());

        boolean found = false;
        for (ValueSetOrSingleCodeBinding b : parent.getValueSetBindings()) {
          if (b instanceof ValueSetBinding && b.getTableId().equals(vsb.getTableId())) {
            found = true;
          }
        }
        if (!found) {
          parent.addValueSetBinding(vsb);
        }

      }
    } else {
      log.debug("CodeTable for component is null. description=" + i.getDescription());
    }

    if (i.getComment() != null && !i.getComment().equals("")) {
      Comment comment = new Comment();
      comment.setDescription(i.getComment());
      comment.setLastUpdatedDate(new Date());
      comment.setLocation(o.getPosition() + "");
      parent.addComment(comment);
    }


    o.setUsage(convertUsage(i.getUsage()));
    if (o.getUsage().equals(Usage.B) || o.getUsage().equals(Usage.W)) {
      o.setUsage(Usage.X);
    }
    checkIntegrity(o);
    return o;
  }

  Usage convertUsage(gov.nist.healthcare.hl7tools.domain.Usage i) {
    return Usage.fromValue(i.getValue());
  }



  void checkIntegrity(Component o) {
    checkLength(o.getMinLength(), o.getMaxLength());

  }

  void checkIntegrity(Field o) {
    checkLength(o.getMinLength(), o.getMaxLength());
    checkCard(o.getMin(), o.getMax());
  }

  void checkIntegrity(SegmentRefOrGroup o) {
    checkCard(o.getMin(), o.getMax());
  }

  void checkLength(String minLength, String maxLength) {
    try {
      if (maxLength != null) {
        Integer max = Integer.valueOf(maxLength);
        Integer min = Integer.valueOf(minLength);
        if (min > max) {
          throw new IllegalArgumentException(
              "Min Length " + minLength + " is greather than Max Length " + maxLength);
        }
      }
    } catch (NumberFormatException e) {
    }
  }

  void checkCard(Integer minCard, String maxCard) {
    try {
      if (maxCard != null && !"*".equals(maxCard)) {
        Integer max = Integer.valueOf(maxCard);
        if (minCard > max) {
          throw new IllegalArgumentException(
              "Min Card " + minCard + " is greather than Max Card " + maxCard);
        }
      }
    } catch (NumberFormatException e) {
    }
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



  private void dtmTots(String hl7Version) {

    Criteria where = Criteria.where("metaData.hl7Version").is(hl7Version);
    Query qry = Query.query(where);
    List<IGDocument> igs = mongoOps.find(qry, IGDocument.class);
    for (IGDocument igDoc : igs) {
      DatatypeLibrary dtLib = igDoc.getProfile().getDatatypeLibrary();
      Datatype d = getDataType("DR", hl7Version);
      if (d != null) {
        Datatype ts = getDataType("TS", hl7Version);
        List<Component> components = d.getComponents();
        if (components != null && !components.isEmpty() && ts != null) {
          for (Component component : components) {
            if (component.getDatatype() != null && component.getDatatype().getName() != null
                && component.getDatatype().getName().equals("DTM") && ts != null) {
              component.setDatatype(new DatatypeLink(ts.getId(), ts.getName(), ts.getExt()));
            }
          }
          dtLib.addDatatype(new DatatypeLink(ts.getId(), ts.getName(), ts.getExt()));
        }
        mongoOps.save(d);
      }
      mongoOps.save(igDoc);
    }

  }



  private Datatype finDatatype(String id) {
    Criteria where = Criteria.where("_id").is(new ObjectId(id));
    Query qry = Query.query(where);
    return mongoOps.findOne(qry, Datatype.class);
  }


  private Table findTable(String id) {
    Criteria where = Criteria.where("id").is(new ObjectId(id));
    Query qry = Query.query(where);
    return mongoOps.findOne(qry, Table.class);
  }

  public List<Segment> findSegmentByIds(Set<String> ids) {
    Criteria where = Criteria.where("id").in(ids);
    Query qry = Query.query(where);
    List<Segment> segments = mongoOps.find(qry, Segment.class);
    return segments;
  }

  public List<Datatype> findDatatypeByIds(Set<String> ids) {
    Criteria where = Criteria.where("id").in(ids);
    Query qry = Query.query(where);
    List<Datatype> datatypes = mongoOps.find(qry, Datatype.class);
    return datatypes;
  }

  public Datatype findDatatypeById(String id) {
    Criteria where = Criteria.where("id").in(id);
    Query qry = Query.query(where);
    Datatype datatype = mongoOps.findOne(qry, Datatype.class);
    return datatype;
  }

  public Datatype findDatatypeByName(String name, String hl7Version) {
    List<Datatype> datatypes =
        findByNameAndVersionAndScope(name, hl7Version, SCOPE.HL7STANDARD.toString());
    Datatype dt = datatypes != null && !datatypes.isEmpty() ? datatypes.get(0) : null;
    return dt;
  }



  public Segment findSegmentById(String id) {
    Criteria where = Criteria.where("id").in(id);
    Query qry = Query.query(where);
    Segment segment = mongoOps.findOne(qry, Segment.class);
    return segment;
  }



  private boolean contains(DatatypeLink link, DatatypeLibrary datatypeLibrary) {
    if (datatypeLibrary.getChildren() != null) {
      for (DatatypeLink datatypeLink : datatypeLibrary.getChildren()) {
        if (datatypeLink.getId() != null && datatypeLink.getId().equals(link.getId())) {
          return true;
        }
      }
    }
    return false;
  }

  private void fixDatatypeRecursion(IGDocument document) {
    DatatypeLibrary datatypeLibrary = document.getProfile().getDatatypeLibrary();
    Datatype withdrawn = getWithdrawnDatatype(document.getProfile().getMetaData().getHl7Version());
    DatatypeLink withdrawnLink =
        new DatatypeLink(withdrawn.getId(), withdrawn.getName(), withdrawn.getExt());
    Set<String> datatypeIds = new HashSet<String>();
    for (DatatypeLink datatypeLink : datatypeLibrary.getChildren()) {
      datatypeIds.add(datatypeLink.getId());
    }
    List<Datatype> datatypes = findDatatypeByIds(datatypeIds);
    for (Datatype datatype : datatypes) {
      if (datatype != null) {
        List<Component> components = datatype.getComponents();
        if (components != null && !components.isEmpty()) {
          for (Component component : components) {
            DatatypeLink componentDatatypeLink = component.getDatatype();
            if (componentDatatypeLink != null && componentDatatypeLink.getId() != null
                && componentDatatypeLink.getId().equals(datatype.getId())) {
              component.setDatatype(withdrawnLink);
              if (!contains(withdrawnLink, datatypeLibrary)) {
                datatypeLibrary.addDatatype(withdrawnLink);
              }
            }
          }
        }
      }
    }

    for (Datatype dt : datatypes) {
      mongoOps.save(dt);
    }
    mongoOps.save(datatypeLibrary);
  }



  private Datatype getWithdrawnDatatype(String hl7Version) {
    List<Datatype> datatypes =
        findByNameAndVersionAndScope("-", hl7Version, SCOPE.HL7STANDARD.toString());
    Datatype dt = datatypes != null && !datatypes.isEmpty() ? datatypes.get(0) : null;
    if (dt == null) {
      dt = new Datatype();
      dt.setName("-");
      dt.setDescription("withdrawn");
      dt.setHl7versions(Arrays.asList(new String[] {hl7Version}));
      dt.setHl7Version(hl7Version);
      dt.setScope(SCOPE.HL7STANDARD);
      dt.setDateUpdated(Calendar.getInstance().getTime());
      dt.setStatus(STATUS.PUBLISHED);
      dt.setPrecisionOfDTM(3);
      mongoOps.save(dt);
    }
    return dt;
  }

  public List<Datatype> findByNameAndVersionAndScope(String name, String hl7Version, String scope) {
    Criteria where = Criteria.where("name").is(name).andOperator(
        Criteria.where("hl7Version").is(hl7Version).andOperator(Criteria.where("scope").is(scope)));
    Query qry = Query.query(where);
    List<Datatype> datatypes = mongoOps.find(qry, Datatype.class);
    return datatypes;
  }



  private void fixDatatypeRecursion(String hl7Version) {
    Criteria where = Criteria.where("metaData.hl7Version").is(hl7Version);
    Query qry = Query.query(where);
    List<IGDocument> igDocuments = mongoOps.find(qry, IGDocument.class);
    for (IGDocument document : igDocuments) {
      fixDatatypeRecursion(document);
    }
  }


  private void fixCE_(String hl7Version) throws IOException {
    Criteria where = Criteria.where("profile.metaData.hl7Version").is(hl7Version);
    Query qry = Query.query(where);
    List<IGDocument> igDocuments = mongoOps.find(qry, IGDocument.class);
    org.apache.poi.ss.usermodel.Sheet sheet = ce_Workbook.createSheet(hl7Version);
    HashMap<String, String> toBeRemoved = new HashMap<String, String>();

    for (IGDocument document : igDocuments) {
      String title = null;
      String author = null;
      if (document.getScope().equals(IGDocumentScope.HL7STANDARD)) {
        title = "HL7 Standard " + hl7Version;
        author = "N/A";
      } else {
        author = document.getAccountId() + "";
        title = document.getMetaData().getTitle();
      }
      HashMap<String, String> results = fixCE_(document);
      toBeRemoved.putAll(results);
      saveCE_Found(title, author, results, sheet);
    }

    for (String id : toBeRemoved.keySet()) {
      Query query = Query.query(Criteria.where("id").is(id));
      mongoOps.remove(query, Datatype.class);
    }
  }

  /**
   * 
   * @param hl7Version
   * @throws IOException
   */
  private void changeDR_DTMtoDR_NIST(String hl7Version) throws IOException {
    Criteria where = Criteria.where("profile.metaData.hl7Version").is(hl7Version);
    Query qry = Query.query(where);
    HashMap<String, String> toBeRemoved = new HashMap<String, String>();
    List<IGDocument> igDocuments = mongoOps.find(qry, IGDocument.class);
    for (IGDocument document : igDocuments) {
      toBeRemoved.putAll(changeDR_DTMtoDR_NIST(document));
    }
    for (String id : toBeRemoved.keySet()) {
      Query query = Query.query(Criteria.where("id").is(id));
      mongoOps.remove(query, Datatype.class);
    }
  }



  private void fixStandardThirdLevelComplexDatatype(String hl7Version) throws IOException {
    Criteria where = Criteria.where("profile.metaData.hl7Version").is(hl7Version).and("scope")
        .is(IGDocumentScope.HL7STANDARD);
    Query qry = Query.query(where);
    List<IGDocument> igDocuments = mongoOps.find(qry, IGDocument.class);
    org.apache.poi.ss.usermodel.Sheet sheet = thirdLevelWorkbook.createSheet(hl7Version);
    for (IGDocument document : igDocuments) {
      HashMap<String, String> tmp = listSegmentThirdLevelComplexDatatype(document);
      String title = null;
      String author = null;
      if (document.getScope().equals(IGDocumentScope.HL7STANDARD)) {
        title = "HL7 Standard " + hl7Version;
        author = "N/A";
      } else {
        author = document.getAccountId() + "";
        title = document.getMetaData().getTitle();
      }
      saveThirdLevelDatatypeResults(title, author, tmp, sheet);
      fixThirdLevelDatatype(document);
      tmp = listSegmentThirdLevelComplexDatatype(document);
      if (tmp != null && !tmp.isEmpty()) {
        throw new IllegalArgumentException("Third level datatype found for " + title);
      }
    }
  }


  private void addMissingDatatypes(String hl7Version) throws IOException {
    Criteria where = Criteria.where("profile.metaData.hl7Version").is(hl7Version);
    Query qry = Query.query(where);
    List<IGDocument> igDocuments = mongoOps.find(qry, IGDocument.class);
    for (IGDocument document : igDocuments) {
      addMissingDatatypes(document);
    }
  }



  private void saveThirdLevelDatatypeResults(String title, String author,
      HashMap<String, String> results, org.apache.poi.ss.usermodel.Sheet sheet) throws IOException {
    if (results != null && !results.isEmpty()) {
      Row loc = sheet.createRow((short) sheet.getLastRowNum() + 1);
      Cell rCell1 = loc.createCell(0);
      rCell1.setCellValue("Title");
      Cell rCell2 = loc.createCell(1);
      rCell2.setCellValue("Author");

      loc = sheet.createRow((short) sheet.getLastRowNum() + 1);
      rCell1 = loc.createCell(0);
      rCell1.setCellValue(title);
      rCell2 = loc.createCell(1);
      rCell2.setCellValue(author);


      loc = sheet.createRow((short) sheet.getLastRowNum() + 1);
      rCell1 = loc.createCell(0);
      rCell1.setCellValue("Component");
      rCell2 = loc.createCell(1);
      rCell2.setCellValue("Datatype");


      for (String location : results.keySet()) {
        loc = sheet.createRow((short) sheet.getLastRowNum() + 1);
        rCell1 = loc.createCell(0);
        rCell1.setCellValue(location);
        rCell2 = loc.createCell(1);
        rCell2.setCellValue(results.get(location));
      }
    }
  }


  private void saveCE_Found(String title, String author, HashMap<String, String> results,
      org.apache.poi.ss.usermodel.Sheet sheet) throws IOException {
    if (results != null && !results.isEmpty()) {
      Row loc = sheet.createRow((short) sheet.getLastRowNum() + 1);
      Cell rCell1 = loc.createCell(0);
      rCell1.setCellValue("Title");
      Cell rCell2 = loc.createCell(1);
      rCell2.setCellValue("Author");

      loc = sheet.createRow((short) sheet.getLastRowNum() + 1);
      rCell1 = loc.createCell(0);
      rCell1.setCellValue(title);
      rCell2 = loc.createCell(1);
      rCell2.setCellValue(author);


      loc = sheet.createRow((short) sheet.getLastRowNum() + 1);
      rCell1 = loc.createCell(0);
      rCell1.setCellValue("CE_*");


      for (String id : results.keySet()) {
        loc = sheet.createRow((short) sheet.getLastRowNum() + 1);
        rCell2 = loc.createCell(0);
        rCell2.setCellValue(results.get(id));
      }
    }
  }



  private void drTodrdtm(String hl7Version) {
    Criteria where = Criteria.where("metaData.hl7Version").is(hl7Version);
    Query qry = Query.query(where);
    List<IGDocument> igs = mongoOps.find(qry, IGDocument.class);
    for (IGDocument igDoc : igs) {
      DatatypeLibrary dtLib = igDoc.getProfile().getDatatypeLibrary();
      DatatypeLink drtmLink = null;
      for (DatatypeLink dtLink : dtLib.getChildren()) {
        if (dtLink != null && dtLink.getName() != null && !dtLink.getName().equals("-")) {
          Datatype d1 = finDatatype(dtLink.getId());
          if (d1 != null) {
            List<Component> components = d1.getComponents();
            if (components != null && !components.isEmpty()) {
              for (Component c1 : components) {
                DatatypeLink link1 = c1.getDatatype();
                if (link1 != null && link1.getId() != null && link1.getName().equals("DR")) {
                  Datatype prev = finDatatype(link1.getId());
                  if (prev.getScope().equals(SCOPE.HL7STANDARD) || "".equals(prev.getExt())
                      || null == prev.getExt()) {
                    if (drtmLink == null) {
                      Datatype drdtm = findDRDTM(hl7Version);
                      drtmLink = new DatatypeLink(drdtm.getId(), drdtm.getName(), drdtm.getExt());
                    }
                    c1.setDatatype(drtmLink);
                  }
                }
              }
              mongoOps.save(d1);
            }
          }
        }
      }
      if (drtmLink != null) {
        dtLib.getChildren().add(drtmLink);
        mongoOps.save(dtLib);
      }
    }
  }



  private Datatype createOrGetNistFlavor(Datatype datatype) {
    Datatype found = getDataType(datatype.getName() + "_NIST", datatype.getHl7Version());
    if (found == null) {
      datatype.setId(null);
      datatype.setName(datatype.getName() + "_NIST");
      datatype.setLabel(datatype.getName());
      datatype.setPurposeAndUse(
          "The data type flavor is a NIST generated datatype to fix the 5-level datatype issue in the HL7 v2.x base standard for certain elements that use this data type. For more details, please refer to documentation explaining the issue and the resolution.");
      mongoOps.save(datatype);
      return datatype;
    }
    return found;
  }



  private String getSubstitute(String name) {
    if (name != null) {
      Map<String, String> versionMap = threeLevelsDatatypesSubstituionMap.get(hl7Version);
      if (versionMap != null) {
        return versionMap.get(name);
      }
    }
    return null;
  }



  private HashMap<String, String> listSegmentThirdLevelComplexDatatype(Segment segment) {
    HashMap<String, String> results = new HashMap<String, String>();
    if (segment != null && segment.getScope().equals(SCOPE.HL7STANDARD)) {
      List<Field> fields = segment.getFields();
      if (fields != null && !fields.isEmpty()) {
        for (Field field : fields) {
          DatatypeLink fieldDatatypeLink = field.getDatatype(); // CQ
          if (fieldDatatypeLink != null && fieldDatatypeLink.getId() != null) {
            Datatype fieldDatatype = findDatatypeById(fieldDatatypeLink.getId());
            List<Component> components = fieldDatatype.getComponents();
            if (components != null && !components.isEmpty()) {
              for (Component component : components) {
                DatatypeLink componentDatatypeLink = component.getDatatype(); // CE
                if (componentDatatypeLink != null && componentDatatypeLink.getId() != null) {
                  Datatype compDatatype = findDatatypeById(componentDatatypeLink.getId());
                  if (compDatatype.getComponents() != null
                      && !compDatatype.getComponents().isEmpty()) {
                    for (Component subComponent : compDatatype.getComponents()) {
                      DatatypeLink subComponentDatatypeLink = subComponent.getDatatype(); // CE
                      if (subComponentDatatypeLink != null
                          && subComponentDatatypeLink.getId() != null) {
                        Datatype subComponentDatatype =
                            findDatatypeById(subComponentDatatypeLink.getId());
                        if (subComponentDatatype.getComponents() != null
                            && !subComponentDatatype.getComponents().isEmpty()) {
                          results.put(
                              segment.getName() + "-" + field.getPosition() + "."
                                  + component.getPosition() + "." + subComponent.getPosition(),
                              subComponentDatatype.getName());
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    return results;
  }


  private HashMap<String, String> fixCE_(IGDocument document) {
    DatatypeLibrary datatypeLibrary = document.getProfile().getDatatypeLibrary();
    SegmentLibrary segmentLibrary = document.getProfile().getSegmentLibrary();

    // Set<String> datatypeIds = new HashSet<String>();
    HashMap<String, String> toBeRemoved = new HashMap<String, String>();
    String hl7Version = document.getMetaData().getHl7Version();
    Set<DatatypeLink> newLinks = new HashSet<DatatypeLink>();
    for (DatatypeLink datatypeLink : datatypeLibrary.getChildren()) {
      // datatypeIds.add(datatypeLink.getId());
      Datatype datatype = findDatatypeById(datatypeLink.getId());
      if (datatype != null) {
        if (datatype != null && datatype.getId() != null && datatype.getName().startsWith("CE_")
            && datatype.getScope().equals(SCOPE.HL7STANDARD)) {
          toBeRemoved.put(datatype.getId(), datatype.getName());
        } else {
          List<Component> components = datatype.getComponents();
          if (components != null && !components.isEmpty()) {
            boolean substituted = false;
            for (Component component : components) {
              DatatypeLink componentDatatypeLink = component.getDatatype();
              Datatype ce = findDatatypeById(componentDatatypeLink.getId());
              if (ce != null && ce.getId() != null && ce.getName().startsWith("CE_")
                  && ce.getScope().equals(SCOPE.HL7STANDARD)) {
                Datatype substitute = getDataType("CE", hl7Version);
                if (substitute != null) {
                  toBeRemoved.put(ce.getId(), ce.getName());
                  DatatypeLink newLink = new DatatypeLink(substitute.getId(), substitute.getName(),
                      substitute.getExt());
                  component.setDatatype(newLink);
                  if (!contains(newLinks, newLink)) {
                    newLinks.add(newLink);
                  }
                  substituted = true;
                }
              }
            }
            if (substituted) {
              mongoOps.save(datatype);
            }
          }
        }
      }
    }


    for (SegmentLink segmentLink : segmentLibrary.getChildren()) {
      Segment segment = findSegmentById(segmentLink.getId());
      if (segment != null) {
        List<Field> fields = segment.getFields();
        if (fields != null && !fields.isEmpty()) {
          boolean substituted = false;
          for (Field field : fields) {
            DatatypeLink fieldDatatypeLink = field.getDatatype(); // CQ
            Datatype ce = findDatatypeById(fieldDatatypeLink.getId());
            if (ce != null && ce.getId() != null && ce.getName().startsWith("CE_")
                && ce.getScope().equals(SCOPE.HL7STANDARD)) {
              Datatype substitute = getDataType("CE", hl7Version);
              if (substitute != null) {
                toBeRemoved.put(ce.getId(), ce.getName());
                DatatypeLink newLink =
                    new DatatypeLink(substitute.getId(), substitute.getName(), substitute.getExt());
                field.setDatatype(newLink);
                if (!contains(newLinks, newLink)) {
                  newLinks.add(newLink);
                }
                substituted = true;
              }
            }
          }
          if (substituted) {
            mongoOps.save(segment);
          }
        }
      }
    }


    for (DatatypeLink link : datatypeLibrary.getChildren()) {
      if (!toBeRemoved.keySet().contains((link.getId())) && !contains(newLinks, link)) {
        newLinks.add(link);
      }
    }

    datatypeLibrary.setChildren(newLinks);
    mongoOps.save(datatypeLibrary);
    mongoOps.save(document);


    return toBeRemoved;
  }


  private HashMap<String, String> changeDR_DTMtoDR_NIST(IGDocument document) {
    DatatypeLibrary datatypeLibrary = document.getProfile().getDatatypeLibrary();
    SegmentLibrary segmentLibrary = document.getProfile().getSegmentLibrary();

    // Set<String> datatypeIds = new HashSet<String>();
    HashMap<String, String> toBeRemoved = new HashMap<String, String>();
    Set<DatatypeLink> newLinks = new HashSet<DatatypeLink>();
    for (DatatypeLink datatypeLink : datatypeLibrary.getChildren()) {
      // datatypeIds.add(datatypeLink.getId());
      Datatype datatype = findDatatypeById(datatypeLink.getId());
      if (datatype != null) {
        if (datatype != null && datatype.getId() != null && datatype.getName().equals("DR_DTM")
            && datatype.getScope().equals(SCOPE.HL7STANDARD)) {
          toBeRemoved.put(datatype.getId(), datatype.getName());
        } else {
          List<Component> components = datatype.getComponents();
          if (components != null && !components.isEmpty()) {
            boolean substituted = false;
            for (Component component : components) {
              DatatypeLink componentDatatypeLink = component.getDatatype();
              Datatype d = findDatatypeById(componentDatatypeLink.getId());
              if (d != null && d.getId() != null && d.getName().equals("DR_DTM")
                  && d.getScope().equals(SCOPE.HL7STANDARD)) {
                Datatype substitute = getDataType("DR_NIST", d.getHl7Version());
                if (substitute != null) {
                  toBeRemoved.put(d.getId(), d.getName());
                  DatatypeLink newLink = new DatatypeLink(substitute.getId(), substitute.getName(),
                      substitute.getExt());
                  component.setDatatype(newLink);
                  if (!contains(newLinks, newLink)) {
                    newLinks.add(newLink);
                  }
                  substituted = true;
                }
              }
            }
            if (substituted) {
              mongoOps.save(datatype);
            }
          }
        }
      }
    }


    for (SegmentLink segmentLink : segmentLibrary.getChildren()) {
      Segment segment = findSegmentById(segmentLink.getId());
      if (segment != null) {
        List<Field> fields = segment.getFields();
        if (fields != null && !fields.isEmpty()) {
          boolean substituted = false;
          for (Field field : fields) {
            DatatypeLink fieldDatatypeLink = field.getDatatype(); // CQ
            Datatype ce = findDatatypeById(fieldDatatypeLink.getId());
            if (ce != null && ce.getId() != null && ce.getName().equals("DR_DTM")
                && ce.getScope().equals(SCOPE.HL7STANDARD)) {
              Datatype substitute = getDataType("DR_NIST", ce.getHl7Version());
              if (substitute != null) {
                toBeRemoved.put(ce.getId(), ce.getName());
                DatatypeLink newLink =
                    new DatatypeLink(substitute.getId(), substitute.getName(), substitute.getExt());
                field.setDatatype(newLink);
                if (!contains(newLinks, newLink)) {
                  newLinks.add(newLink);
                }
                substituted = true;
              }
            }
          }
          if (substituted) {
            mongoOps.save(segment);
          }
        }
      }
    }


    for (DatatypeLink link : datatypeLibrary.getChildren()) {
      if (!toBeRemoved.keySet().contains((link.getId())) && !contains(newLinks, link)) {
        newLinks.add(link);
      }
    }

    datatypeLibrary.setChildren(newLinks);
    mongoOps.save(datatypeLibrary);
    mongoOps.save(document);


    return toBeRemoved;
  }



  private boolean contains(Set<DatatypeLink> links, DatatypeLink link) {
    for (DatatypeLink tmp : links) {
      if (tmp != null && link != null && tmp.getId() != null && tmp.getId().equals(link.getId())) {
        return true;
      }
    }
    return false;
  }



  private void fixThirdLevelDatatype(IGDocument document) {
    DatatypeLibrary datatypeLibrary = document.getProfile().getDatatypeLibrary();
    Set<String> datatypeIds = new HashSet<String>();
    Set<DatatypeLink> newLinks = new HashSet<DatatypeLink>();
    for (DatatypeLink datatypeLink : datatypeLibrary.getChildren()) {
      datatypeIds.add(datatypeLink.getId());
      Datatype datatype = findDatatypeById(datatypeLink.getId());
      HashMap<String, String> results = new HashMap<String, String>();
      if (datatype != null) {
        List<Component> components = datatype.getComponents();
        if (components != null && !components.isEmpty()) {
          for (Component component : components) {
            DatatypeLink componentDatatypeLink = component.getDatatype(); // CQ
            if (componentDatatypeLink != null && componentDatatypeLink.getId() != null) {

              if (datatypeLibrary.findOne(componentDatatypeLink.getId()) == null
                  && !contains(newLinks, componentDatatypeLink)) {
                newLinks.add(componentDatatypeLink);
              }

              Datatype compDatatype = findDatatypeById(componentDatatypeLink.getId());
              List<Component> subComponents = compDatatype.getComponents();
              boolean substituted = false;
              if (subComponents != null && !subComponents.isEmpty()) {
                for (Component subComponent : subComponents) {
                  DatatypeLink component2DatatypeLink = subComponent.getDatatype(); // CE
                  if (component2DatatypeLink != null && component2DatatypeLink.getId() != null) {

                    if (datatypeLibrary.findOne(component2DatatypeLink.getId()) == null
                        && !contains(newLinks, component2DatatypeLink)) {
                      newLinks.add(component2DatatypeLink);
                    }


                    Datatype subComponentDatatype =
                        findDatatypeById(component2DatatypeLink.getId());


                    if (subComponentDatatype.getScope().equals(SCOPE.HL7STANDARD)
                        && subComponentDatatype.getComponents() != null
                        && !subComponentDatatype.getComponents().isEmpty()) {
                      results.put(datatype.getName() + "-" + component.getPosition() + "."
                          + subComponent.getPosition(), subComponentDatatype.getName());
                      String substituteName = getSubstitute(component2DatatypeLink.getName());
                      if (substituteName != null) {
                        Datatype substitute = findDatatypeByName(substituteName,
                            subComponentDatatype.getHl7Version());
                        DatatypeLink substituteLink = new DatatypeLink(substitute.getId(),
                            substitute.getName(), substitute.getExt());
                        substituted = true;
                        subComponent.setDatatype(substituteLink);
                        if (datatypeLibrary.findOne(substituteLink.getId()) == null
                            && !contains(newLinks, substituteLink)) {
                          newLinks.add(substituteLink);
                        }
                      }
                    }
                  }
                }
              }
              if (substituted) {
                Datatype nistFlavor = createOrGetNistFlavor(compDatatype);
                DatatypeLink newLink =
                    new DatatypeLink(nistFlavor.getId(), nistFlavor.getName(), nistFlavor.getExt());
                component.setDatatype(newLink);
                if (datatypeLibrary.findOne(newLink.getId()) == null
                    && !contains(newLinks, newLink)) {
                  newLinks.add(newLink);
                }

              }
            }
          }
        }
        mongoOps.save(datatype);
      }
    }


    if (!newLinks.isEmpty()) {
      for (DatatypeLink link : newLinks) {
        if (datatypeLibrary.findOne(link.getId()) == null) {
          datatypeLibrary.addDatatype(link);
        }
      }
    }

    mongoOps.save(datatypeLibrary);
    mongoOps.save(document);

  }


  private void addMissingDatatypes(IGDocument document) {
    DatatypeLibrary datatypeLibrary = document.getProfile().getDatatypeLibrary();
    Set<DatatypeLink> newLinks = new HashSet<DatatypeLink>();
    for (DatatypeLink datatypeLink : datatypeLibrary.getChildren()) {
      Datatype datatype = findDatatypeById(datatypeLink.getId());
      if (datatype != null) {
        List<Component> components = datatype.getComponents();
        if (components != null && !components.isEmpty()) {
          for (Component component : components) {
            DatatypeLink componentDatatypeLink = component.getDatatype(); // CQ
            if (componentDatatypeLink != null && componentDatatypeLink.getId() != null) {
              if (datatypeLibrary.findOne(componentDatatypeLink.getId()) == null
                  && !contains(newLinks, componentDatatypeLink)) {
                newLinks.add(componentDatatypeLink);
              }
              Datatype compDatatype = findDatatypeById(componentDatatypeLink.getId());
              List<Component> subComponents = compDatatype.getComponents();
              if (subComponents != null && !subComponents.isEmpty()) {
                for (Component subComponent : subComponents) {
                  DatatypeLink component2DatatypeLink = subComponent.getDatatype(); // CE
                  if (component2DatatypeLink != null && component2DatatypeLink.getId() != null) {
                    if (datatypeLibrary.findOne(component2DatatypeLink.getId()) == null
                        && !contains(newLinks, component2DatatypeLink)) {
                      newLinks.add(component2DatatypeLink);
                    }
                  }
                }
              }
            }
          }
        }
        mongoOps.save(datatype);
      }
    }


    if (!newLinks.isEmpty()) {
      for (DatatypeLink link : newLinks) {
        if (datatypeLibrary.findOne(link.getId()) == null) {
          datatypeLibrary.addDatatype(link);
        }
      }
    }
    mongoOps.save(datatypeLibrary);
    mongoOps.save(document);
  }



  private HashMap<String, String> listSegmentThirdLevelComplexDatatype(IGDocument document) {
    SegmentLibrary segmentLibrary = document.getProfile().getSegmentLibrary();
    HashMap<String, String> results = new HashMap<String, String>();
    for (SegmentLink segmentLink : segmentLibrary.getChildren()) {
      Segment segment = findSegmentById(segmentLink.getId());
      results.putAll(listSegmentThirdLevelComplexDatatype(segment));
    }
    return results;
  }



}
