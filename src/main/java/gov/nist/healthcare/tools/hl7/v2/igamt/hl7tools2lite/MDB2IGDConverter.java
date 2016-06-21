package gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

import gov.nist.healthcare.hl7tools.domain.HL7Table;
import gov.nist.healthcare.hl7tools.service.util.mock.hl7.domain.Code;
import gov.nist.healthcare.hl7tools.service.util.mock.hl7.domain.Component;
import gov.nist.healthcare.hl7tools.service.util.mock.hl7.domain.DataElement;
import gov.nist.healthcare.hl7tools.service.util.mock.hl7.domain.Datatype;
import gov.nist.healthcare.hl7tools.service.util.mock.hl7.domain.Element;
import gov.nist.healthcare.hl7tools.service.util.mock.hl7.domain.Event;
import gov.nist.healthcare.hl7tools.service.util.mock.hl7.domain.Field;
import gov.nist.healthcare.hl7tools.service.util.mock.hl7.domain.Group;
import gov.nist.healthcare.hl7tools.service.util.mock.hl7.domain.Interaction;
import gov.nist.healthcare.hl7tools.service.util.mock.hl7.domain.Message;
import gov.nist.healthcare.hl7tools.service.util.mock.hl7.domain.MessageType;
import gov.nist.healthcare.hl7tools.service.util.mock.hl7.domain.Segment;
import gov.nist.healthcare.hl7tools.service.util.mock.hl7.domain.Usage;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Constant;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.ContentDefinition;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Extensibility;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Stability;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Table;

// gcr

@org.springframework.stereotype.Component
public class MDB2IGDConverter implements Runnable {

	Logger log = LoggerFactory.getLogger(MDB2IGDConverter.class);

	public final static File OUTPUT_DIR = new File("src/main/resources/hl7db");

	static final String ROOT_GROUP_NAME = "ROOT";
	static final String MISSING_GROUP_NAME = "NA";

	JdbcTemplate jdbcTemplate;

	String hl7Version;

	static File versionDir;

	ObjectMapper mapper = new ObjectMapper();

	int groupIncr = 1707;
	int componentIncr;
	int fieldIncr;
	int elementIncr = 6943;

	Map<String, Group> grpByMsg = new HashMap<String, Group>();
	Map<String, Group> grpByName = new HashMap<String, Group>();
	List<String> segIds =  new ArrayList<String>();

	public MDB2IGDConverter(String hl7Version) {
		try {
			SimpleDriverDataSource dataSource = new SimpleDriverDataSource();
			dataSource.setDriver(new com.mysql.jdbc.Driver());
			dataSource.setUrl("jdbc:mysql://localhost/mdb");
			dataSource.setUsername("root");

			this.jdbcTemplate = new JdbcTemplate(dataSource);
			this.hl7Version = hl7Version;
			mapper.setSerializationInclusion(Include.NON_NULL);
		} catch (SQLException e) {
			log.error("", e);
		}
	}

	public void run() {
		if (!OUTPUT_DIR.exists()) {
			OUTPUT_DIR.mkdir();
		}
		versionDir = new File(OUTPUT_DIR, hl7Version);
		if (!versionDir.exists()) {
			versionDir.mkdir();
		}


		log.info("Doing Table...");
		Helper<Table> hlpTable = new Helper<Table>();
		List<Table> tables = (hlpTable.fetch(SQL4_TABLE(), RM4_TABLE));
		
		for (Table tab : tables) {
			
		}

		
//		log.info("For version=" + hl7Version);
//		log.info("Doing MessageType...");
//		Helper<MessageType> hlpMessageType = new Helper<MessageType>();
//		List<MessageType> messageTypes = (hlpMessageType.fetch(SQL4_MESSAGETYPE(), RM4_MESSAGETYPE));
//		hlpMessageType.write(messageTypes, "message_types");

//		log.info("Doing Message...");
//		Helper<Message> hlpMessage = new Helper<Message>();
//		List<Message> messages = (hlpMessage.fetch(SQL4_MESSAGE(), RM4_MESSAGE));
//		log.info(" writing messages=" + messages.size());
//		hlpMessage.write(messages, "messages");

//		log.info("Doing Group...");
//		Helper<Group> hlpGroup = new Helper<Group>();
//		List<Group> groups = (hlpGroup.fetch(SQL4_GROUP(), RM4_GROUP));
//		List<Group> groups1 = assembleGroups(groups);
//		log.info(" writing groups1=" + groups1.size());
//		hlpGroup.write(groups1, "groups");

//		log.info("Doing Segment...");
//		Helper<Segment> hlpSegment = new Helper<Segment>();
//		List<Segment> segments = (hlpSegment.fetch(SQL4_SEGMENT(), RM4_SEGMENT));
//		for (Segment seg : segments) {
//			segIds.add(seg.getId());
//		}
//		log.info(" writing segments=" + segments.size());
//		hlpSegment.write(segments, "segments");

//		log.info("Doing Datatype...");
//		Helper<Datatype> hlpDatatype = new Helper<Datatype>();
//		List<Datatype> datatypes = (hlpDatatype.fetch(SQL4_DATATYPE(), RM4_DATATYPE));
//		log.info(" writing datatypes=" + datatypes.size());
//		hlpDatatype.write(datatypes, "datatypes");

//		log.info("Doing Component...");
//		Helper<Component> hlpComponent = new Helper<Component>();
//		List<Component> components = (hlpComponent.fetch(SQL4_COMPONENT(), RM4_COMPONENT));
//		log.info(" writing components=" + components.size());
//		hlpComponent.write(components, "components");

//		log.info("Doing Field...");
//		Helper<Field> hlpField = new Helper<Field>();
//		List<Field> fields = (hlpField.fetch(SQL4_FIELD(), RM4_FIELD));
//		log.info(" writing fields=" + fields.size());
//		hlpField.write(fields, "fields");

//		log.info("Doing Code...");
//		Helper<Code> hlpCode = new Helper<Code>();
//		List<Code> codes = (hlpCode.fetch(SQL4_CODE(), RM4_CODE));
//		hlpCode.write(codes, "codes");

//		log.info("Doing DataElement...");
//		Helper<DataElement> hlpDataElement = new Helper<DataElement>();
//		List<DataElement> dataelements = (hlpDataElement.fetch(SQL4_DATAELEMENT(), RM4_DATAELEMENT));
//		log.info(" writing dataelements=" + dataelements.size());
//		hlpDataElement.write(dataelements, "data_elements");


//		log.info("Doing Element...");
//		Helper<Element> hlpElement = new Helper<Element>();
//		Helper<InterimElement> hlpInterimElement = new Helper<InterimElement>();
//		List<InterimElement> interimelements = (hlpInterimElement.fetch(SQL4_ELEMENT(), RM4_ELEMENT));
//		List<Element> elements = postProcessElements(interimelements);
//		log.info(" writing elements=" + elements.size());
//		hlpElement.write(elements, "elements");

//		log.info("Doing Event...");
//		Helper<Event> hlpEvent = new Helper<Event>();
//		List<Event> events = (hlpEvent.fetch(SQL4_EVENT(), RM4_EVENT));
//		hlpEvent.write(events, "events");

//		log.info("Doing Interaction...");
//		Helper<Interaction> hlpInteraction = new Helper<Interaction>();
//		List<Interaction> interactions = (hlpInteraction.fetch(SQL4_INTERACTION(), RM4_INTERACTION));
//		hlpInteraction.write(interactions, "interactions");

		log.info("...Done");

	}
	
	Table convertTable(gov.nist.healthcare.hl7tools.service.util.mock.hl7.domain.Table i) {

		Table o = new Table();

		o.setBindingIdentifier(i.getId());
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
	
	String SQL4_TABLE() {
		StringBuilder bld = new StringBuilder();
		bld.append("SELECT t.`table_id`, t.`description_as_pub`, t.`table_type`, t.`oid_codesystem`, t.`section`");
		bld.append(" FROM hl7versions v");
		bld.append(" INNER JOIN hl7tables t ON v.`version_id` = t.`version_id`");
		bld.append(" WHERE v.`hl7_version` = ");
		bld.append("'");
		bld.append(hl7Version);
		bld.append("'");
		bld.append(" AND t.`table_id` > 0");
		bld.append(" ORDER BY t.`table_id`");
		bld.append(";");
		String rval = bld.toString();
		return rval;
	}

	final RowMapper<Table> RM4_TABLE = new RowMapper<Table>() {

		public Table mapRow(ResultSet rs, int rowNum) throws SQLException {
			Table o = new Table();
			o.setBindingIdentifier(rs.getString("table_id"));
			o.setName(rs.getString("description_as_pub"));
			o.setDescription(rs.getString("description_as_pub"));
			o.setOid(rs.getString("oid_codesystem"));
			o.setScope(Constant.SCOPE.HL7STANDARD);
			o.setStability(Stability.Static);
			o.setContentDefinition(ContentDefinition.Extensional);
			o.setExtensibility(Extensibility.Closed);
			o.setHl7Version(hl7Version);
			return o;
		}
	};
	
	List<Element> postProcessElements(List<InterimElement> ies) {

		List<Element> rval = new ArrayList<Element>();
		GroupStacker stk = new GroupStacker();
		int position = 1;
		Integer groupId = null;
		int parentId = 1;
		int state = 0;
		int nullconts = 0;
		int openconts = 0;
		int closeconts = 0;
		int incdecconts = 0;

		for (InterimElement ie : ies) {
			if (ie.getGroupName() == null && ie.getSegmentId() == null) {
				nullconts++;
				continue;
			}
			if ("|".equals(ie.getSegmentId())) {
				closeconts++;
				continue;
			}
			if (!segIds.contains(ie.getSegmentId())) {
				continue;
			}
			String key = assembleGroupKey(ie);
			Group group = grpByName.get(key);
			if (group != null) {
				int incdec = stk.waw(group, ie.getSegmentId());
				state += incdec;
				if (incdec < 0) {
					incdecconts++;
					continue;
				}
				if (state > 0) {
					if (group.isRoot()) {
						parentId = stk.getCurrGroupId();
						ie.setParentId(parentId);
					} else {
						ie.setParentId(parentId);
						groupId = parentId = stk.getCurrGroupId();
						ie.setGroupId(groupId);
					}
				} else {
					if (group.isRoot()) {
						if (parentId != group.getId()) {
							position = 1;
							parentId = group.getId();
						}
					} else {
						position = 1;
						int idx = ies.indexOf(ie);
						InterimElement next = ies.get(idx + 1);
						String nextKey = assembleGroupKey(next);
						Group nextGroup = grpByName.get(nextKey);
						if (nextGroup != null) {
							parentId = nextGroup.getId();
						}
					}
					ie.setParentId(parentId);
				}
			} else {
				log.info("null4=" + key);
				for (String key1 : grpByName.keySet()) {
					log.info("key1=" + key1);
				}
			}
			ie.setPosition(position++);
			if (ie.getParentId() == null) {
				throw new NullPointerException();
			}
			Element el = (Element) ie;
			if (isEmbraced(el.getSegmentId())) {
				el.setSegmentId(null);
			}
			rval.add(el);
			// log.info(String.format("%" + state + 1 * 2 + "d", state) + " p="
			// + group.getName() + " el=" + ie.getSegmentId() + " g=" +
			// ie.groupName + " " + stk.stack);
			log.info(String.format("%" + state + 1 * 2 + "d", state) + ie.toString());
			// log.trace("1 ie key=" + key);
			// log.trace("1 ie id=" + ie.getId());
			// log.trace("1 ie parent=" + ie.getParentId());
			// log.trace("1 ie segment=" + ie.getSegmentId());
			// log.trace("1 ie group=" + ie.getGroupId());
			// log.trace("1 ie position=" + ie.getPosition());
			// log.debug(((Element) ie).toString());
		}
		log.info("ies=" + ies.size());
		log.info("nullconts=" + nullconts);
		log.info("openconts=" + openconts);
		log.info("closeconts=" + closeconts);
		log.info("incdecconts=" + incdecconts);
		return rval;
	}

	boolean isEmbraced(String segId) {
		return segId.contains("{") || segId.contains("[") || segId.contains("<") || segId.contains("}") || segId.contains("]") || segId.contains(">");
	}
	
	List<Group> assembleGroups(List<Group> groups) {
		String currentGroup = null;
		List<Group> rval = new ArrayList<Group>();
		for (Group group : groups) {
			if (!group.getName().equals(currentGroup)) {
				if (group.isRoot()) {
					currentGroup = null;
				} else {
					currentGroup = group.getName();
				}
				rval.add(group);
				String key = group.getName();
				grpByName.put(key, group);
			}
		}
		return rval;
	}

	String assembleGroupKey(InterimElement ie) {
		String groupName = ie.getGroupName();
		if (groupName == null || groupName.trim().length() == 0) {
			if (isEmbraced(ie.getSegmentId())) {
				groupName = MISSING_GROUP_NAME;
				// We need to get the missing group into the map set.
				Group group = new Group();
				group.setId(++groupIncr);
				group.setName(groupName);
				grpByName.put(ie.messageStucture + "." + groupName, group);
			} else {
				groupName = ROOT_GROUP_NAME;
			}
		}
		return ie.messageStucture + "." + groupName;
	}

	public static void main(String[] args) {
		for (String arg : args) {
			MDB2IGDConverter app = new MDB2IGDConverter(arg);
			app.run();
		}
	}

	public class Helper<T> {

		void write(List<T> list, String fileName) {
			File outfile = new File(versionDir, fileName + ".json");
			try {
				Writer writer = new FileWriter(outfile);
				mapper.writerWithDefaultPrettyPrinter().writeValue(writer, list);
			} catch (IOException e) {
				e.printStackTrace();
				log.error("", e);
			}
		}

		List<T> fetch(String sqlSelect, RowMapper<T> rMapper) {
			List<T> list = jdbcTemplate.query(sqlSelect, rMapper);
			return list;
		}
	}

	public class GroupStacker {

		Stack<Group> stack = new Stack<Group>();

		int waw(Group group, String segId) {
			if (group.isRoot()) {
				return 0;
			}
			if (!stack.empty() && segId.contains("}") || segId.contains("]") || segId.contains(">")) {
				stack.pop();
				return -1;
			} else {
				stack.push(group);
				return 1;
			}
			// if (!stack.empty() && stack.peek().equals(group)) {
			// stack.pop();
			// return -1;
			// } else {
			// stack.push(group);
			// return 1;
			// }
		}

		Integer getCurrGroupId() {
			if (stack.empty()) {
				return 1;
			}
			return stack.peek().getId();
		}

		Group getCurrGroup() {
			if (stack.empty()) {
				return null;
			}
			return grpByName.get(stack.peek());
		}

	}

	String SQL4_MESSAGETYPE() {
		StringBuilder bld = new StringBuilder();
		bld.append("SELECT m.message_type, m.description, m.section");
		bld.append(" FROM hl7messagetypes m, hl7versions v");
		bld.append(" WHERE m.version_id = v.version_id");
		bld.append(" AND v.hl7_version = ");
		bld.append("'");
		bld.append(hl7Version);
		bld.append("'");
		bld.append(";");
		return bld.toString();
	}

	final RowMapper<MessageType> RM4_MESSAGETYPE = new RowMapper<MessageType>() {

		public MessageType mapRow(ResultSet rs, int rowNum) throws SQLException {
			MessageType messageType = new MessageType();
			messageType.setId(rs.getString("message_type"));
			messageType.setDescription(rs.getString("description"));
			messageType.setSection(rs.getString("section"));

			return messageType;
		}
	};

	String SQL4_MESSAGE() {
		StringBuilder bld = new StringBuilder();
		bld.append("SELECT m.`message_type`, e.`event_code`, i.`message_structure`, m.`section`");
		bld.append(" FROM hl7versions v ");
		bld.append(" INNER JOIN hl7messagetypes m ON v.`version_id` = m.`version_id`");
		bld.append(" INNER JOIN hl7msgstructids i ON v.`version_id` = i.`version_id`");
		bld.append(" INNER JOIN hl7events e ON v.`version_id` = e.`version_id`");
		bld.append(" WHERE v.`hl7_version` = '2.8.2'");
		bld.append(" AND concat(m.`message_type`, '_', e.`event_code`) = i.`message_structure`");
		bld.append(" ORDER BY m.message_type");
		bld.append(";");

		// bld.append("SELECT m.`message_structure`, m.`section`,
		// e.`event_code`, e.`message_structure_snd`");
		// bld.append(" FROM hl7versions v INNER JOIN hl7msgstructids m ON
		// v.version_id = m.version_id");
		// bld.append(" INNER JOIN hl7eventmessagetypes e ON v.`version_id` =
		// e.`version_id`");
		// bld.append(" WHERE v.`hl7_version` = ");
		// bld.append("'");
		// bld.append(hl7Version);
		// bld.append("'");
		// bld.append(" AND m.`message_structure` = e.`message_structure_snd`");

		return bld.toString();
	}

	final RowMapper<Message> RM4_MESSAGE = new RowMapper<Message>() {

		public Message mapRow(ResultSet rs, int rowNum) throws SQLException {
			Message message = new Message();
			message.setId(rs.getString("message_structure"));
			message.setSection(rs.getString("section"));
			message.setEvent_id(rs.getString("event_code"));
			message.setMsg_type_id(rs.getString("message_type"));
			return message;
		}
	};

	String SQL4_GROUP() {
		StringBuilder bld = new StringBuilder();
		bld.append("SELECT m.`message_structure`, m.`seq_no`, m.`groupname`, m.`seg_code`");
		bld.append(" FROM hl7versions v INNER JOIN hl7msgstructidsegments m ON v.version_id = m.version_id");
		bld.append(" WHERE v.`hl7_version` = ");
		bld.append("'");
		bld.append(hl7Version);
		bld.append("'");
		bld.append(" AND m.`version_id` = v.`version_id`");
		bld.append(" AND (m.`seg_code` = 'MSH' || length(m.`groupname`) > 0)");
		bld.append(" ORDER BY m.`message_structure`, m.`seq_no`");
		bld.append(";");
		String rval = bld.toString();
		return rval;
	}

	final RowMapper<Group> RM4_GROUP = new RowMapper<Group>() {

		public Group mapRow(ResultSet rs, int rowNum) throws SQLException {
			Group grp = new Group();
			grp.setId(++groupIncr);
			grp.setRoot(("MSH".equals(rs.getString("seg_code"))) ? true : false);
			String segId = rs.getString("seg_code");
			grp.setChoice("<".equals(segId) ? true : false);
			grp.setMessageId(rs.getString("message_structure"));
			grp.setName((grp.isRoot() ? (rs.getString("message_structure") + ".ROOT")
					: rs.getString("message_structure") + "." + rs.getString("groupname")));
			return grp;
		}
	};

	String SQL4_SEGMENT() {
		StringBuilder bld = new StringBuilder();
		bld.append("SELECT s.seg_code, s.description, s.section");
		bld.append(" FROM hl7versions v INNER JOIN hl7segments s ON v.version_id = s.version_id");
		bld.append(" WHERE v.`hl7_version` = ");
		bld.append("'");
		bld.append(hl7Version);
		bld.append("'");
		bld.append(" AND s.visible = 'TRUE'");
		bld.append(" ORDER BY s.seg_code");
		bld.append(";");
		String rval = bld.toString();
		return rval;
	}

	final RowMapper<Segment> RM4_SEGMENT = new RowMapper<Segment>() {

		public Segment mapRow(ResultSet rs, int rowNum) throws SQLException {
			Segment seg = new Segment();
			seg.setId(rs.getString("seg_code"));
			seg.setDescription(rs.getString("description"));
			seg.setSection(rs.getString("section"));
			return seg;
		}
	};

	String SQL4_FIELD() {
		StringBuilder bld = new StringBuilder();
		bld.append("SELECT s.`seg_code`, s.`data_item`, s.`req_opt`, s.`seq_no`");
		bld.append(" FROM hl7segmentdataelements s INNER JOIN hl7versions v ON v.version_id = s.version_id");
		bld.append(" WHERE v.`hl7_version` = ");
		bld.append("'");
		bld.append(hl7Version);
		bld.append("'");
		bld.append(" ORDER BY s.`seg_code`");
		bld.append(";");
		String rval = bld.toString();
		return rval;
	}

	final RowMapper<Field> RM4_FIELD = new RowMapper<Field>() {

		public Field mapRow(ResultSet rs, int rowNum) throws SQLException {
			Field fld = new Field();
			fld.setId(++fieldIncr);
			fld.setSegmentId(rs.getString("seg_code"));
			String s = String.format("%05d", rs.getInt("data_item"));
			fld.setDataElementId(s);
			fld.setPosition(rs.getInt("seq_no"));
			String usage1 = rs.getString("req_opt");
			String usage2 = usage1.length() > 0 ? usage1.substring(0, 1) : "";
			fld.setUsage(Usage.fromValue(usage2));
			fld.setMin(0);
			fld.setMax("1");
			return fld;
		}
	};

	String SQL4_DATATYPE() {
		StringBuilder bld = new StringBuilder();
		bld.append("SELECT d.data_structure, d.`description`, d.`section`, d.`elementary`");
		bld.append(" FROM hl7datastructures d, hl7versions v");
		bld.append(" WHERE v.`hl7_version` = ");
		bld.append("'");
		bld.append(hl7Version);
		bld.append("'");
		bld.append(" AND d.`version_id` = v.`version_id`");
		bld.append(" ORDER BY d.data_structure");
		bld.append(";");
		String rval = bld.toString();
		return rval;
	}

	final RowMapper<Datatype> RM4_DATATYPE = new RowMapper<Datatype>() {

		public Datatype mapRow(ResultSet rs, int rowNum) throws SQLException {
			Datatype dt = new Datatype();
			dt.setId(rs.getString("data_structure").toUpperCase());
			dt.setDescription(rs.getString("description"));
			dt.setPrimitive(rs.getBoolean("elementary"));
			String s = rs.getString("section");
			dt.setSection(s.length() > 0 ? s : null);
			return dt;
		}
	};

	String SQL4_COMPONENT() {
		StringBuilder bld = new StringBuilder();
		// bld.append("SET @rownum = 0;");
		bld.append(
				"SELECT d.`data_structure`, c.`data_type_code`, c.`description`, d.`modification`, d.`min_length`, d.`max_length`, d.`conf_length`, c.`table_id`");
		bld.append(" FROM hl7versions v");
		bld.append(" INNER JOIN hl7datastructurecomponents d ON v.`version_id` = d.`version_id`");
		bld.append(" INNER JOIN hl7components c ON v.`version_id` = c.`version_id`");
		bld.append(" WHERE v.`hl7_version` = ");
		bld.append("'");
		bld.append(hl7Version);
		bld.append("'");
		bld.append(" AND d.comp_no = c.comp_no");
		bld.append(" ORDER BY d.data_structure");
		bld.append(";");
		String rval = bld.toString();
		return rval;
	}

	final RowMapper<Component> RM4_COMPONENT = new RowMapper<Component>() {

		public Component mapRow(ResultSet rs, int rowNum) throws SQLException {
			Component cmp = new Component();
			cmp.setId(++componentIncr);
			cmp.setParentDatatypeId(rs.getString("data_structure"));
			cmp.setDatatypeId(rs.getString("data_type_code"));
			cmp.setPosition(componentIncr);
			cmp.setDescription(rs.getString("description"));
			cmp.setUsage(Usage.fromValue(rs.getString("modification")));
			cmp.setMinLength(rs.getInt("min_length"));
			cmp.setMaxLength(rs.getInt("max_length"));
			String s = rs.getString("conf_length").replaceAll("[#=]", "");
			int confLength = (s.length() > 0) ? new Integer(s) : 0;
			cmp.setConfLength(confLength);
			String tableId = rs.getString("table_id");
			cmp.setTableId("0".equals(tableId) ? null : tableId);
			return cmp;
		}
	};

	String SQL4_CODE() {
		StringBuilder bld = new StringBuilder();
		bld.append(" SELECT distinct tv.`table_id`, tv.`table_value`, tv.`description_as_pub`");
		bld.append(" FROM hl7versions v");
		bld.append(" INNER JOIN hl7tablevalues tv ON v.`version_id` = tv.`version_id`");
		bld.append(" WHERE v.`hl7_version` = ");
		bld.append("'");
		bld.append(hl7Version);
		bld.append("'");
		bld.append(" ORDER BY tv.`table_id`");
		bld.append(";");
		String rval = bld.toString();
		return rval;
	}

	final RowMapper<Code> RM4_CODE = new RowMapper<gov.nist.healthcare.hl7tools.service.util.mock.hl7.domain.Code>() {

		public Code mapRow(ResultSet rs, int rowNum) throws SQLException {
			Code cd = new Code();
			String s = String.format("%04d", rs.getInt("table_id"));
			cd.setTableId(s);
			cd.setName(rs.getString("table_value"));
			cd.setDescription(rs.getString("description_as_pub"));
			cd.setUsage(Usage.fromValue("F"));
			return cd;
		}
	};

	String SQL4_DATAELEMENT() {
		StringBuilder bld = new StringBuilder();
		bld.append(
				"SELECT de.`data_item`, de.`data_structure`, de.`description` , de.`min_length`, de.`max_length`, de.`conf_length`, de.`table_id`, de.`section`");
		bld.append(" FROM hl7versions v");
		bld.append(" INNER JOIN hl7dataelements de ON v.`version_id` = de.`version_id`");
		bld.append(" WHERE v.`hl7_version` = ");
		bld.append("'");
		bld.append(hl7Version);
		bld.append("'");
		bld.append(" ORDER BY de.`data_item`");
		bld.append(";");
		String rval = bld.toString();
		return rval;
	}

	final RowMapper<DataElement> RM4_DATAELEMENT = new RowMapper<gov.nist.healthcare.hl7tools.service.util.mock.hl7.domain.DataElement>() {

		public DataElement mapRow(ResultSet rs, int rowNum) throws SQLException {
			DataElement de = new DataElement();
			String s = String.format("%05d", rs.getInt("data_item"));
			de.setId(s);
			de.setDatatypeId(rs.getString("data_structure").toUpperCase());
			de.setDescription(rs.getString("description"));
			String min = rs.getString("min_length");
			de.setMinLength(min != null && min.length() > 0 ? new Integer(min) : 0);
			String max = rs.getString("max_length");
			de.setMaxLength(max != null && max.length() > 0 ? new Integer(max) : 0);
			String conf = rs.getString("conf_length").replaceAll("[=#]", "");
			String conf1 = conf.contains("..") ? conf.substring(conf.lastIndexOf(".") + 1) : conf;
			de.setConfLength(conf1 != null && conf1.length() > 0 ? new Integer(conf1) : -1);
			String s1 = rs.getString("table_id");
			String s2 = "0".equals(s1) ? null : String.format("%04d", new Integer(s1));
			de.setTableId(s2);
			de.setSection(rs.getString("section"));
			return de;
		}
	};

	String SQL4_ELEMENT() {
		StringBuilder bld = new StringBuilder();
		bld.append(
				"SELECT m.`message_structure`, m.`seq_no`, m.`groupname`, m.`seg_code`, m.`modification`, m.`optional`, m.`repetitional`");
		bld.append(" FROM hl7versions v INNER JOIN hl7msgstructidsegments m ON v.version_id = m.version_id");
		bld.append(" WHERE v.`hl7_version` = ");
		bld.append("'");
		bld.append(hl7Version);
		bld.append("'");
		bld.append(";");
		String rval = bld.toString();
		return rval;
	}

	final RowMapper<InterimElement> RM4_ELEMENT = new RowMapper<InterimElement>() {

		public InterimElement mapRow(ResultSet rs, int rowNum) throws SQLException {
			InterimElement el = new InterimElement();
			el.setId(++elementIncr);
			el.setMessageStucture(rs.getString("message_structure"));
			String groupname = rs.getString("groupname");
			el.setGroupName(groupname.trim().length() > 0 ? groupname.toUpperCase() : null);
			String segCode = rs.getString("seg_code");
			el.setSegmentId(segCode);
			// el.setSegmentId(segCode.contains("{") || segCode.contains("}") ||
			// segCode.contains("[") || segCode.contains("]") ||
			// segCode.contains("<") || segCode.contains(">") ? null : segCode);
			el.setGroupState(rs.getString("seg_code"));
			el.setMin(rs.getBoolean("optional") == true ? 0 : 1);
			el.setMax(rs.getBoolean("repetitional") == true ? "*" : "1");
			el.setUsage(Usage.fromValue(rs.getString("modification")));
			return el;
		}
	};

	String SQL4_EVENT() {
		StringBuilder bld = new StringBuilder();
		bld.append("SELECT e.`event_code`, e.`description`, e.`section`");
		bld.append(" FROM hl7versions v ");
		bld.append(" INNER JOIN hl7events e ON v.`version_id` = e.`version_id`");
		bld.append(" WHERE v.`hl7_version` = ");
		bld.append("'");
		bld.append(hl7Version);
		bld.append("'");
		bld.append(" ORDER BY e.`event_code`");
		bld.append(";");
		String rval = bld.toString();
		return rval;
	}

	final RowMapper<Event> RM4_EVENT = new RowMapper<Event>() {

		public Event mapRow(ResultSet rs, int rowNum) throws SQLException {
			Event evt = new Event();
			evt.setId(rs.getString("event_code"));
			evt.setDescription(rs.getString("description"));
			evt.setSection(rs.getString("section"));
			return evt;
		}
	};

	String SQL4_INTERACTION() {
		StringBuilder bld = new StringBuilder();
		bld.append("SELECT e.`event_code`, e.`message_structure_snd`, e.`message_structure_return`");
		bld.append(" FROM hl7versions v ");
		bld.append(" INNER JOIN hl7eventmessagetypes e ON v.`version_id` = e.`version_id`");
		bld.append(" WHERE v.`hl7_version` = ");
		bld.append("'");
		bld.append(hl7Version);
		bld.append("'");
		bld.append(" AND e.`message_structure_snd` IS NOT NULL");
		bld.append(" ORDER BY e.`event_code`");
		bld.append(";");

		String rval = bld.toString();
		return rval;
	}

	final RowMapper<Interaction> RM4_INTERACTION = new RowMapper<Interaction>() {

		public Interaction mapRow(ResultSet rs, int rowNum) throws SQLException {
			Interaction it = new Interaction();
			it.setEventId(rs.getString("event_code"));
			it.setNumber(1);
			it.setSenderMsg(rs.getString("message_structure_snd"));
			it.setReceiverMsg(rs.getString("message_structure_return"));
			return it;
		}
	};
}
