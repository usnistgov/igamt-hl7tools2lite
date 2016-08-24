 SELECT distinct tv.`table_id`, tv.`table_value`, tv.`description_as_pub`
 FROM hl7versions v
 INNER JOIN hl7tablevalues tv ON v.`version_id` = tv.`version_id`
 WHERE v.`hl7_version` = '2.8.2'
 ORDER BY tv.`table_id`
;
SELECT dc.`data_structure`, c.`data_type_code`, c.`description`, dc.`req_opt`, dc.`min_length`, dc.`max_length`, dc.`conf_length`, c.`table_id`
 FROM hl7versions v
 INNER JOIN hl7datastructures d ON v.`version_id` = d.`version_id`
 INNER JOIN hl7datastructurecomponents dc ON v.`version_id` = dc.`version_id`
 INNER JOIN hl7components c ON v.`version_id` = c.`version_id`
 WHERE v.`hl7_version` = '2.8.2'
 AND d.`data_structure` = dc.`data_structure`
 AND dc.comp_no = c.comp_no
 AND d.`elementary` = 'FALSE'
;
SELECT de.`data_item`, de.`data_structure`, de.`description` , de.`min_length`, de.`max_length`, de.`conf_length`, de.`table_id`, de.`section`
 FROM hl7versions v
 INNER JOIN hl7dataelements de ON v.`version_id` = de.`version_id`
 WHERE v.`hl7_version` = '2.8.2'
 ORDER BY de.`data_item`
;
SELECT m.`message_structure`, m.`seq_no`, m.`groupname`, m.`seg_code`, m.`modification`, m.`optional`, m.`repetitional`
 FROM hl7versions v 
 INNER JOIN hl7msgstructidsegments m ON v.version_id = m.version_id
 WHERE v.`hl7_version` = '2.8.2'
;
SELECT e.`event_code`, e.`description`, e.`section`
 FROM hl7versions v 
 INNER JOIN hl7events e ON v.`version_id` = e.`version_id`
 WHERE v.`hl7_version` = '2.8.2'
 ORDER BY e.`event_code`
;
SELECT s.`seg_code`, s.`data_item`, s.`req_opt`, s.`seq_no`
 FROM hl7segmentdataelements s 
 INNER JOIN hl7versions v ON v.version_id = s.version_id
 WHERE v.`hl7_version` = '2.8.2'
 ORDER BY s.`seg_code`
;
SELECT m.`message_structure`, m.`seq_no`, m.`groupname`, m.`seg_code`
 FROM hl7versions v 
 INNER JOIN hl7msgstructidsegments m ON v.version_id = m.version_id
 WHERE v.`hl7_version` = '2.8.2'
 AND m.`version_id` = v.`version_id`
 AND (m.`seg_code` = 'MSH' || length(m.`groupname`) > 0)
 ORDER BY m.`message_structure`, m.`seq_no`
;
SELECT e.`event_code`, e.`message_structure_snd`, e.`message_structure_return`
 FROM hl7versions v 
 INNER JOIN hl7eventmessagetypes e ON v.`version_id` = e.`version_id`
 WHERE v.`hl7_version` = '2.8.2'
 AND e.`message_structure_snd` IS NOT NULL
 ORDER BY e.`event_code`
;
SELECT m.`message_type`, '' as event_code, i.`message_structure`, m.`section`
 FROM hl7versions v 
 INNER JOIN hl7messagetypes m ON v.`version_id` = m.`version_id`
 INNER JOIN hl7msgstructids i ON v.`version_id` = i.`version_id`
 INNER JOIN hl7events e ON v.`version_id` = e.`version_id`
 WHERE v.`hl7_version` = '2.8.2'
 AND m.`message_type` = i.`message_structure`
 LIMIT 1
;
SELECT m.`message_type`, e.`event_code`, i.`message_structure`, m.`section`
 FROM hl7versions v 
 INNER JOIN hl7messagetypes m ON v.`version_id` = m.`version_id`
 INNER JOIN hl7msgstructids i ON v.`version_id` = i.`version_id`
 INNER JOIN hl7events e ON v.`version_id` = e.`version_id`
 WHERE v.`hl7_version` = '2.8.2'
 AND concat(m.`message_type`, '_', e.`event_code`) = i.`message_structure`
 ORDER BY m.message_type
;
SELECT m.message_type, m.description, m.section
 FROM hl7messagetypes m, hl7versions v
 WHERE m.version_id = v.version_id
 AND v.hl7_version = '2.8.2'
;
SELECT s.seg_code, s.description, s.section
 FROM hl7versions v INNER JOIN hl7segments s ON v.version_id = s.version_id
 WHERE v.`hl7_version` = '2.8.2'
 AND s.visible = 'TRUE'
 ORDER BY s.seg_code
;
SELECT t.`table_id`, t.`description_as_pub`, t.`table_type`, t.`oid_codesystem`, t.`section`
 FROM hl7versions v
 INNER JOIN hl7tables t ON v.`version_id` = t.`version_id`
 WHERE v.`hl7_version` = '2.8.2'
 AND t.`table_id` > 0
 ORDER BY t.`table_id`
;
