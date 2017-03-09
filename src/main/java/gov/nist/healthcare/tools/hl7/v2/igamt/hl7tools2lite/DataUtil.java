package gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite;

import java.util.List;

import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Component;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Field;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Group;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentRefOrGroup;

/**
 * 
 * @author haffo
 *
 */
public class DataUtil {

   
  /**
   * 
   * @param newSegmentRefOrGroups
   * @param oldSegmentRefOrGroups
   */
  public static void reuseSegmentRefOrGroupIdsIfExist( List<SegmentRefOrGroup> newSegmentRefOrGroups,List<SegmentRefOrGroup> oldSegmentRefOrGroups){
    for(SegmentRefOrGroup newSegmentRefOrGroup: newSegmentRefOrGroups){
      SegmentRefOrGroup oldSegmentRefOrGroup = findSegmentRefOrGroupByPosition(newSegmentRefOrGroup.getPosition(), oldSegmentRefOrGroups);
       if(oldSegmentRefOrGroup !=null){
         newSegmentRefOrGroup.setId(oldSegmentRefOrGroup.getId());
         if(newSegmentRefOrGroup instanceof Group && oldSegmentRefOrGroup instanceof Group){
           Group newGroup = (Group) newSegmentRefOrGroup;
           Group oldGroup = (Group) oldSegmentRefOrGroup;
           reuseSegmentRefOrGroupIdsIfExist(newGroup.getChildren(),oldGroup.getChildren());
         }
       }
    }
 }
 
  /**
   * 
   * @param position
   * @param segmentRefOrGroups
   * @return
   */
  public static SegmentRefOrGroup findSegmentRefOrGroupByPosition(int position, List<SegmentRefOrGroup> segmentRefOrGroups){
    if(segmentRefOrGroups != null){
    for(SegmentRefOrGroup f: segmentRefOrGroups){
         if(f.getPosition() == position){
           return f;
         }
    }
    }
   return null;
 }
  
  
  /**
   * 
   * @param newComponents
   * @param oldComponents
   */
  public static void reuseComponentIdsIfExist( List<Component> newComponents,List<Component> oldComponents){
    for(Component f: newComponents){
      Component oldComponent = findComponentByPosition(f.getPosition(), oldComponents);
       if(oldComponent !=null){
         f.setId(oldComponent.getId());
       }
    }
 }
 
  /**
   * 
   * @param position
   * @param components
   * @return
   */
 public static Component findComponentByPosition(int position, List<Component> components){
   if(components != null){
   for(Component f: components){
        if(f.getPosition() == position){
          return f;
        }
   }
   }
  return null;
}
 
 /**
  * 
  * @param newFields
  * @param oldFields
  */
  public static void reuseFieldIdsIfExist( List<Field> newFields,List<Field> oldFields){
     for(Field f: newFields){
        Field oldField = findFieldByPosition(f.getPosition(), oldFields);
        if(oldField !=null){
          f.setId(oldField.getId());
        }
     }
  }
  
  /**
   * 
   * @param position
   * @param fields
   * @return
   */
  public static Field findFieldByPosition(int position, List<Field> fields){
    if(fields != null){
    for(Field f: fields){
         if(f.getPosition() == position){
           return f;
         }
    }
    }
   return null;
 }
  
  
}
