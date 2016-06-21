SELECT d.`data_type_code`, d.`description`, d.`section`, d.`elementary`
FROM hl7datastructures d, hl7versions v
WHERE v.`hl7_version` = '2.8.2'
AND d.`version_id` = v.`version_id`

SELECT s.`seg_code`, s.`data_item`, s.`req_opt`
FROM hl7segmentdataelements s INNER JOIN hl7versions v ON v.version_id = s.version_id 
WHERE v.`hl7_version` = '2.8.2';

SELECT v.version_id, v.hl7_version, s.seg_code, s.description, s.visible
FROM hl7versions v INNER JOIN hl7segments s ON v.version_id = s.version_id
WHERE v.`hl7_version` = "2.8.2"
AND s.visible = 'TRUE';

SELECT HL7Versions.version_id, HL7Versions.hl7_version,HL7Segments.seg_code, HL7Segments.description, HL7Segments.visibleFROM HL7Versions INNER JOIN HL7Segments ON HL7Versions.version_id =HL7Segments.version_idWHERE (((HL7Versions.version_id)=92) AND ((HL7Segments.visible)='TRUE'));SELECT tv.`table_id`, tv.`table_value`, tv.`description_as_pub`
FROM hl7versions v 
INNER JOIN hl7tablevalues tv ON v.`version_id` = tv.`version_id`
WHERE v.`hl7_version` = '2.8.2';

SELECT de.`data_item`, de.`description` , de.`min_length`, de.`max_length`, de.`conf_length`, de.`table_id`, de.`section`
FROM hl7versions v 
INNER JOIN hl7dataelements de ON v.`version_id` = de.`version_id`
WHERE v.`hl7_version` = '2.8.2';

SELECT * FROM hl7versions v INNER JOIN hl7dataelements de ON v.`version_id` = de.`version_id` WHERE v.`hl7_version` = '2.8.2' and de.`conf_length` like '%..%';

SET @row_number = 0;
SELECT (@row_number := @row_number + 1) AS id, m.`message_structure`, m.`seq_no`, m.`groupname`, m.`seg_code`, m.`modification`, m.`optional`, m.`repetitional`, m.`version_id` 
FROM hl7versions v INNER JOIN hl7msgstructidsegments m ON v.version_id = m.version_id 
WHERE v.`hl7_version` = '2.8.2';

SELECT e.`event_code`, e.`description`, e.`section`
FROM hl7versions v 
INNER JOIN hl7events e ON v.`version_id` = e.`version_id`
WHERE v.`hl7_version` = '2.8.2'
ORDER BY e.`event_code`;

SELECT e.`event_code`, 1, e.`message_structure_snd`, e.`message_structure_return`
FROM hl7versions v 
INNER JOIN hl7eventmessagetypes e ON v.`version_id` = e.`version_id`
WHERE v.`hl7_version` = '2.8.2'
ORDER BY e.`event_code`;

SELECT e.`event_code`, e.`message_structure_snd`, e.`message_structure_return` 
 FROM hl7versions v  
 INNER JOIN hl7eventmessagetypes e ON v.`version_id` = e.`version_id`
 WHERE v.`hl7_version` = '2.8.2'  
 AND e.`message_structure_snd` IS NOT NULL
 ORDER BY e.`event_code`;
 
SELECT m.`message_structure` , m.`description`, m.`section`, e.`event_code`, e.`message_structure_snd` 
FROM hl7versions v 
INNER JOIN hl7msgstructids m ON v.`version_id` = m.`version_id` 
INNER JOIN hl7eventmessagetypes e ON v.`version_id` = e.`version_id` 
WHERE v.`hl7_version` = '2.8.2'
AND m.`message_structure` = e.`message_structure_snd`;

create or replace view emtypes282 as 
SELECT s.`event_code`, s.`message_type`, s.`groupname`, s.`seg_code`, s.`modification`, s.`optional`, s.`repetitional`
FROM hl7versions v INNER JOIN hl7eventmessagetypesegments s ON v.version_id = s.version_id
WHERE v.`hl7_version` = "2.8.2";

create or replace view emtype282 as 
SELECT s.`event_code`, s.`message_structure_snd`, s.`message_structure_return`
FROM hl7versions v INNER JOIN hl7eventmessagetypes s ON v.version_id = s.version_id
WHERE v.`hl7_version` = "2.8.2";

create or replace view msg282 as 
SELECT s.`message_structure`, s.`description`
FROM hl7versions v INNER JOIN hl7msgstructids s ON v.version_id = s.version_id
WHERE v.`hl7_version` = "2.8.2";

create or replace view seg282 as 
SELECT v.version_id, v.hl7_version, s.seg_code, s.description, s.visible
FROM hl7versions v INNER JOIN hl7segments s ON v.version_id = s.version_id
WHERE v.`hl7_version` = "2.8.2"
AND s.visible = 'TRUE'
ORDER BY s.seg_code;

create or replace view ele282 as
SET @row_number = 6943;
SELECT (@row_number := @row_number + 1) AS id, m.`message_structure`, m.`seq_no`, m.`groupname`, m.`seg_code`, m.`modification`, m.`optional`, m.`repetitional`, m.`version_id` 
FROM hl7versions v INNER JOIN hl7msgstructidsegments m ON v.version_id = m.version_id 
WHERE v.`hl7_version` = '2.8.2';
