package life.genny.services;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.persistence.NoResultException;
import javax.transaction.Transactional;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import life.genny.qwanda.Ask;
import life.genny.qwanda.Question;
import life.genny.qwanda.QuestionQuestion;
import life.genny.qwanda.attribute.Attribute;
import life.genny.qwanda.attribute.AttributeLink;
import life.genny.qwanda.attribute.EntityAttribute;
import life.genny.qwanda.datatype.DataType;
import life.genny.qwanda.entity.BaseEntity;
import life.genny.qwanda.entity.EntityEntity;
import life.genny.qwanda.exception.BadDataException;
import life.genny.qwanda.message.QBaseMSGMessageTemplate;
import life.genny.qwanda.validation.Validation;
import life.genny.qwanda.validation.ValidationList;
import life.genny.qwandautils.GennySheets;

/**
 * @author acrow
 *
 */
public class ProjectsLoading {
  /**
   * Stores logger object.
   */
  protected static final Logger log = org.apache.logging.log4j.LogManager
      .getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());


 
  public static Map<String,Map>  loadIntoMap(final String hostSheetId, final String secret, File credentialPath) {
	  GennySheets sheets = new GennySheets(secret, hostSheetId, credentialPath);
	  
	    List<Map> dataMaps = null;
	    Integer countDown = 10;
	    while (countDown > 0) {
	      try {
	        dataMaps = sheets.hostingImport();
	        break;
	      } catch (Exception ee) { // java.util.NoSuchElementException e |
	                               // java.net.SocketTimeoutException ee
	        log.error("Load from Google Doc failed, trying again in 3 sec");
	        try {
	          Thread.sleep(3000);
	        } catch (InterruptedException e1) {

	        }
	        countDown--;
	      }
	    }
	    
	    Map<String,Map> returnMap = new HashMap<String,Map>();
	    for (Map data : dataMaps) {
	    	String code = (String)data.get("code");
	    	if (!StringUtils.isBlank(code)) {
	    		returnMap.put(code, data);
	    	}
	    }
	  
	  return returnMap;
  }

 


  private static Boolean getBooleanFromString(final String booleanString) {
    if (booleanString == null) {
      return false;
    }

    if ("TRUE".equalsIgnoreCase(booleanString.toUpperCase())
        || "YES".equalsIgnoreCase(booleanString.toUpperCase())
        || "T".equalsIgnoreCase(booleanString.toUpperCase())
        || "Y".equalsIgnoreCase(booleanString.toUpperCase())
        || "1".equalsIgnoreCase(booleanString)) {
      return true;
    }
    return false;

  }
  

}
