SELECT d.`data_type_code`, d.`description`, d.`section`, d.`elementary`
FROM hl7datastructures d, hl7versions v
WHERE v.`hl7_version` = '2.8.2'
AND d.`version_id` = v.`version_id`;

SELECT sd.`seg_code`, sd.`data_item`, sd.`req_opt`
FROM hl7versions v  
INNER JOIN hl7segmentdataelements sd ON v.version_id = sd.version_id 
INNER JOIN hl7segments s ON v.version_id = s.version_id
WHERE v.`hl7_version` = '2.8.2'
AND s.`seg_code` = sd.`seg_code`


SELECT v.version_id, v.hl7_version, s.seg_code, s.description, s.visible
FROM hl7versions v 
INNER JOIN hl7segments s ON v.version_id = s.version_id
WHERE v.`hl7_version` = "2.8.2"
AND s.visible = 'TRUE';

SELECT HL7Versions.version_id, HL7Versions.hl7_version,HL7Segments.seg_code, HL7Segments.description, HL7Segments.visibleFROM HL7Versions v INNER JOIN HL7Segments s ON v.version_id = s.version_idWHERE (((v.version_id)=92) AND ((s.visible)='TRUE'));SELECT tv.`table_id`, tv.`table_value`, tv.`description_as_pub`
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
FROM hl7versions v 
INNER JOIN hl7msgstructidsegments m ON v.version_id = m.version_id 
INNER JOIN hl7segments s ON v.version_id = s.version_id
WHERE v.`hl7_version` = '2.8.2'
AND s.`seg_code` = m.`seg_code`;

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
 
SELECT m.`message_type`, e.`event_code`, i.`message_structure`, m.`section`
FROM hl7versions v 
INNER JOIN hl7messagetypes m ON v.`version_id` = m.`version_id` 
INNER JOIN hl7msgstructids i ON v.`version_id` = i.`version_id` 
INNER JOIN hl7events e ON v.`version_id` = e.`version_id` 
WHERE v.`hl7_version` = '2.8.2'
AND concat(m.`message_type`, '_', e.`event_code`) = i.`message_structure`
ORDER BY m.`message_type`;

create or replace view emtype282 as 
SELECT s.`event_code`, s.`message_structure_snd`, s.`message_structure_return`
FROM hl7versions v INNER JOIN hl7eventmessagetypes s ON v.version_id = s.version_id
WHERE v.`hl7_version` = "2.8.2";

create or replace view msgt282 as 
SELECT s.`message_type`, s.`description`, s.`section`
FROM hl7versions v INNER JOIN hl7messagetypes s ON v.version_id = s.version_id
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

SELECT de.`data_item`, de.`data_structure`, de.`description` , de.`min_length`, de.`max_length`, de.`conf_length`, de.`table_id`, de.`section` FROM hl7versions v INNER JOIN hl7dataelements de ON v.`version_id` = de.`version_id` WHERE v.`hl7_version` = '2.8.2' ORDER BY de.`data_item`;


SELECT dc.`data_structure`, c.`data_type_code`, c.`description`, dc.`modification`, dc.`min_length`, dc.`max_length`, dc.`conf_length`, c.`table_id`
FROM hl7versions v 
INNER JOIN hl7datastructures d ON v.`version_id` = d.`version_id` 
INNER JOIN hl7datastructurecomponents dc ON v.`version_id` = dc.`version_id` 
INNER JOIN hl7components c ON v.`version_id` = c.`version_id` 
WHERE v.`hl7_version` = '2.8' 
AND d.`data_structure` = dc.`data_structure`
AND dc.comp_no = c.comp_no
AND d.`elementary` = 'FALSE';

SELECT d.data_structure, d.`description`, d.`section`, d.`elementary` 
FROM hl7datastructures d, hl7versions v 
WHERE v.`hl7_version` = '2.8.2' 
AND d.`version_id` = v.`version_id` 
ORDER BY d.data_structure;


SELECT dc.`data_structure`, c.`data_type_code`, length(c.`data_type_code`) len, v.`hl7_version`, c.`description`, dc.`modification`, dc.`min_length`, dc.`max_length`, dc.`conf_length`, c.`table_id`
FROM hl7versions v 
INNER JOIN hl7datastructures d ON v.`version_id` = d.`version_id` 
INNER JOIN hl7datastructurecomponents dc ON v.`version_id` = dc.`version_id` 
INNER JOIN hl7components c ON v.`version_id` = c.`version_id` 
WHERE v.`hl7_version` IN ('2.7.1', '2.8', '2.8.1', '2.8.2')
AND d.`data_structure` = dc.`data_structure`
AND dc.comp_no = c.comp_no
AND d.`elementary` = 'FALSE'
having len > 3;