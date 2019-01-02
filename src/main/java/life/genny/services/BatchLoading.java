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
 * @author helios
 *
 */
public class BatchLoading {
  /**
   * Stores logger object.
   */
  protected static final Logger log = org.apache.logging.log4j.LogManager
      .getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());

  private static BaseEntityService2 service;

  public static int id = 1;
  
  private static boolean isSynchronise;
  

  private static String table;


  public BatchLoading(BaseEntityService2 service) {
    this.service = service;
  }

  private final String secret = System.getenv("GOOGLE_CLIENT_SECRET");
  private final String hostingSheetId = System.getenv("GOOGLE_HOSTING_SHEET_ID");
  File credentialPath =
      new File(System.getProperty("user.home"), ".genny/sheets.googleapis.com-java-quickstart");
  public GennySheets sheets = new GennySheets(secret, hostingSheetId, credentialPath);

  public static Map<String, Object> savedProjectData;
  
  public static final String REALM = updateRealm();
  
  public static final String DEFAULT_REALM = "genny";
  
  List<String> projectList = new ArrayList<>();

  /**
   * Upsert Validation to database
   * 
   * @param project
   */
  public void validations(Map<String, Object> project) {
    if (project.get("validations") == null) {
      return;
    }
    ValidatorFactory factory = javax.validation.Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();
    ((HashMap<String, HashMap>) project.get("validations")).entrySet().stream().forEach(data -> {
      Map<String, Object> validations = data.getValue();
      String regex = null;
      
      regex = (String) validations.get("regex");
      if (regex!=null) {
          regex = regex.replaceAll("^\"|\"$", "");
      }
      String code = ((String) validations.get("code")).replaceAll("^\"|\"$", "");;
      if ("VLD_AU_DRIVER_LICENCE_NO".equalsIgnoreCase(code)) {
          System.out.println("detected VLD_AU_DRIVER_LICENCE_NO");
      }
      String name = ((String) validations.get("name")).replaceAll("^\"|\"$", "");;
      String recursiveStr = (String) validations.get("recursive");
      String multiAllowedStr = (String) validations.get("multi_allowed");
      String groupCodesStr = (String) validations.get("group_codes");
      Boolean recursive = getBooleanFromString(recursiveStr);
      Boolean multiAllowed = getBooleanFromString(multiAllowedStr);

      Validation val = null;

      if (code.startsWith(Validation.getDefaultCodePrefix() + "SELECT_")) {
        val = new Validation(code, name, groupCodesStr, recursive, multiAllowed);
      } else {
        val = new Validation(code, name, regex);

      }
      //val.setRealm(REALM);
      System.out.print("code " + code + ",name:" + name + ",val:" + val + ", grp="
          + (groupCodesStr != null ? groupCodesStr : "X"));

      Set<ConstraintViolation<Validation>> constraints = validator.validate(val);
      for (ConstraintViolation<Validation> constraint : constraints) {
        System.out.println(constraint.getPropertyPath() + " " + constraint.getMessage());
      }
      service.upsert(val);
    });
  }

  /**
   * Upsert Attribute to database
   * 
   * @param project
   * @param dataTypeMap
   */
  public void attributes(Map<String, Object> project, Map<String, DataType> dataTypeMap) {
    if (project.get("attributes") == null) {
      return;
    }
    ValidatorFactory factory = javax.validation.Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();

    ((HashMap<String, HashMap>) project.get("attributes")).entrySet().stream().forEach(data -> {
      try {
        Map<String, Object> attributes = data.getValue();
         String code = ((String) attributes.get("code")).replaceAll("^\"|\"$", "");;
        String dataType = ((String) attributes.get("dataType")).replaceAll("^\"|\"$", "");;
        String name = ((String) attributes.get("name")).replaceAll("^\"|\"$", "");;
        DataType dataTypeRecord = dataTypeMap.get(dataType);
        ((HashMap<String, HashMap>) project.get("dataType")).get(dataType);
        String privacyStr = (String) attributes.get("privacy");
        if (privacyStr != null) {
                privacyStr = privacyStr.toUpperCase();
        }
        Boolean privacy = "TRUE".equalsIgnoreCase(privacyStr);
        if (privacy) {
                System.out.println("Attribute "+code+" has default privacy");
        }
        String descriptionStr = (String) attributes.get("description");
        String helpStr = (String) attributes.get("help");
        String placeholderStr = (String) attributes.get("placeholder");
        String defaultValueStr = (String) attributes.get("defaultValue");
        Attribute attr = new Attribute(code, name, dataTypeRecord);
        attr.setDefaultPrivacyFlag(privacy);
        attr.setDescription(descriptionStr);
        attr.setHelp(helpStr);
        attr.setPlaceholder(placeholderStr);
        attr.setDefaultValue(defaultValueStr);
        //attr.setRealm(REALM);
        Set<ConstraintViolation<Attribute>> constraints = validator.validate(attr);
        for (ConstraintViolation<Attribute> constraint : constraints) {
          System.out.println(constraint.getPropertyPath() + " " + constraint.getMessage());
        }
        service.upsert(attr);
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
  }

  /**
   * Initialized Map of DataTypes
   * 
   * @param project
   * @return
   */
  public Map<String, DataType> dataType(Map<String, Object> project) {
    if (project.get("dataType") == null) {
      return null;
    }
    final Map<String, DataType> dataTypeMap = new HashMap<>();
    ((HashMap<String, HashMap>) project.get("dataType")).entrySet().stream().forEach(data -> {
      Map<String, Object> dataType = data.getValue();
      String validations = (String) dataType.get("validations");
      String code = ((String) dataType.get("code")).replaceAll("^\"|\"$", "");;
      String name = ((String) dataType.get("name")).replaceAll("^\"|\"$", "");;
      String inputmask = (String) dataType.get("inputmask");
      final ValidationList validationList = new ValidationList();
      validationList.setValidationList(new ArrayList<Validation>());
      if (validations != null) {
        final String[] validationListStr = validations.split(",");
        for (final String validationCode : validationListStr) {
          try {
            Validation validation = service.findValidationByCode(validationCode);
              validationList.getValidationList().add(validation);
        } catch (NoResultException e) {
            log.error("Could not load Validation "+validationCode);
        }
        }
      }
      if (!dataTypeMap.containsKey(code)) {
        final DataType dataTypeRecord = new DataType(name, validationList, name, inputmask);
        dataTypeMap.put(code, dataTypeRecord);
      }
    });
    return dataTypeMap;
  }

  /**
   * Upsert BaseEntity to Database
   * 
   * @param project
   */
  public void baseEntitys(Map<String, Object> project) {
    if (project.get("baseEntitys") == null) {
      return;
    }
    ValidatorFactory factory = javax.validation.Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();

    ((HashMap<String, HashMap>) project.get("baseEntitys")).entrySet().stream().forEach(data -> {
      Map<String, Object> baseEntitys = data.getValue();
      String code = ((String) baseEntitys.get("code")).replaceAll("^\"|\"$", "");;
      String name = ((String) baseEntitys.get("name")).replaceAll("^\"|\"$", "");;
      BaseEntity be = new BaseEntity(code, name);
      //be.setRealm(REALM);
      Set<ConstraintViolation<BaseEntity>> constraints = validator.validate(be);
      for (ConstraintViolation<BaseEntity> constraint : constraints) {
        System.out.println(constraint.getPropertyPath() + " " + constraint.getMessage());
      }

      service.upsert(be);
    });
  }

  /**
   * Upsert BaseEntities with Attributes
   * 
   * @param project
   */
  public void baseEntityAttributes(Map<String, Object> project) {
    if (project.get("attibutesEntity") == null) {
      return;
    }
    ((HashMap<String, HashMap>) project.get("attibutesEntity")).entrySet().stream()
        .forEach(data -> {
          Map<String, Object> baseEntityAttr = data.getValue();
          String attributeCode = null;
             try {
				attributeCode =  ((String) baseEntityAttr.get("attributeCode")).replaceAll("^\"|\"$", "");
			} catch (Exception e2) {
				log.error("AttributeCode not found ["+baseEntityAttr+"]");
			}
          String valueString = (String) baseEntityAttr.get("valueString");
          if (valueString != null) {
            valueString = valueString.replaceAll("^\"|\"$", "");
          }
          String baseEntityCode = null;
          
          try {
			baseEntityCode = 
			      ((String) baseEntityAttr.get("baseEntityCode")).replaceAll("^\"|\"$", "");
		          String weight = (String) baseEntityAttr.get("weight");
		          String privacyStr = (String) baseEntityAttr.get("privacy");
		          Boolean privacy = "TRUE".equalsIgnoreCase(privacyStr);
		          Attribute attribute = null;
		          BaseEntity be = null;
		          try {
		            attribute = service.findAttributeByCode(attributeCode);
		            if (attribute == null) {
		              log.error("BASE ENTITY CODE: " + baseEntityCode);
		            	log.error(attributeCode+" is not in the Attribute Table!!!");
		            } else {
		            be = service.findBaseEntityByCode(baseEntityCode);
		            Double weightField = null;
		            try {
		              weightField = Double.valueOf(weight);
		            } catch (java.lang.NumberFormatException ee) {
		              weightField = 0.0;
		            }
		            try {
		              EntityAttribute ea = be.addAttribute(attribute, weightField, valueString);
		              if (privacy || attribute.getDefaultPrivacyFlag()) {
		            	  	ea.setPrivacyFlag(true);
		              }
		            } catch (final BadDataException e) {
		              e.printStackTrace();
		            }
		            service.updateWithAttributes(be);
		            }
		          }
		           catch (final NoResultException e) {
		          }
		          
		} catch (Exception e1) {
			String beCode = "BAD BE CODE";
			if (baseEntityAttr != null) {
				beCode = (String) baseEntityAttr.get("baseEntityCode");
			}
			log.error("Error in getting baseEntityAttr  for AttributeCode "+attributeCode+ " and beCode="+beCode);
		}

        });
  }

  /**
   * Upsert EntityEntity
   * 
   * @param project
   */
  public void entityEntitys(Map<String, Object> project) {
    if (project.get("basebase") == null) {
      return;
    }
    ((HashMap<String, HashMap>) project.get("basebase")).entrySet().stream().forEach(data -> {
      Map<String, Object> entEnts = data.getValue();
      String linkCode = (String) entEnts.get("linkCode");
      String parentCode = (String) entEnts.get("parentCode");
      String targetCode = (String) entEnts.get("targetCode");
      String weightStr = (String) entEnts.get("weight");
      String valueString = (String) entEnts.get("valueString");
      final Double weight = Double.valueOf(weightStr);
      BaseEntity sbe = null;
      BaseEntity tbe = null;
      Attribute linkAttribute = service.findAttributeByCode(linkCode);
      try {
        sbe = service.findBaseEntityByCode(parentCode);
        tbe = service.findBaseEntityByCode(targetCode);
        if(isSynchronise) {
          try {
            EntityEntity ee = service.findEntityEntity(parentCode, targetCode, linkCode);
            ee.setWeight(weight);
            ee.setValueString(valueString);
            service.updateEntityEntity(ee);
          } catch (final NoResultException e) {
            EntityEntity ee = new EntityEntity(sbe, tbe, linkAttribute, weight);
            ee.setValueString(valueString);
            service.insertEntityEntity(ee);
          }
          return;
        }
        sbe.addTarget(tbe, linkAttribute, weight, valueString);
        //sbe.setRealm(REALM);
        service.updateWithAttributes(sbe);
      } catch (final NoResultException e) {
    	  log.warn("CODE NOT PRESENT IN LINKING: "+parentCode+" : "+targetCode+" : "+linkAttribute);
      } catch (final BadDataException e) {
        e.printStackTrace();
      } catch (final NullPointerException e) {
        e.printStackTrace();
      }
    });
  }

  /**
   * Upsert QuestionQuestion
   * 
   * @param project
   */
  @Transactional
  public void questionQuestions(Map<String, Object> project) {
    if (project.get("questionQuestions") == null) {
      return;
    }
    ((HashMap<String, HashMap>) project.get("questionQuestions")).entrySet().stream()
        .forEach(data -> {
          Map<String, Object> queQues = data.getValue();
          String parentCode = (String) queQues.get("parentCode");
          String targetCode = (String) queQues.get("targetCode");
          String weightStr = (String) queQues.get("weight");
          String mandatoryStr = (String) queQues.get("mandatory");
          String readonlyStr = (String) queQues.get("readonly");
          Boolean readonly = readonlyStr == null ? false: "TRUE".equalsIgnoreCase(readonlyStr);
          
          Double weight = 0.0;
          try {
            weight = Double.valueOf(weightStr);
        } catch (NumberFormatException e1) {
            weight = 0.0;
        }
          Boolean mandatory = "TRUE".equalsIgnoreCase(mandatoryStr);

          Question sbe = null;
          Question tbe = null;

          try {
            sbe = service.findQuestionByCode(parentCode);
            tbe = service.findQuestionByCode(targetCode);
            try {
                String oneshotStr = (String)  queQues.get("oneshot");
                Boolean oneshot = false;
                if (oneshotStr == null) {
                    // Set the oneshot to be that of the targetquestion
                    oneshot = tbe.getOneshot();
                } else {
                 oneshot = "TRUE".equalsIgnoreCase(oneshotStr);
                }
                //sbe.setRealm(REALM);
                QuestionQuestion qq = sbe.addChildQuestion(tbe.getCode(), weight, mandatory);
                qq.setOneshot(oneshot);
                qq.setReadonly(readonly);
                QuestionQuestion existing = null;
                try {
                    existing = service.findQuestionQuestionByCode(parentCode, targetCode);
                    if (existing == null) {
                        qq = service.upsert(qq);
                    } else {
                        service.upsert(qq);
                    }
                } catch (NoResultException e1) {
                    qq = service.upsert(qq);
                } catch (Exception e) {
                    existing.setMandatory(qq.getMandatory());
                    existing.setOneshot(qq.getOneshot());
                    existing.setWeight(qq.getWeight());
                    existing.setReadonly(qq.getReadonly());
                    qq = service.upsert(existing);
                } 
                
            } catch (NullPointerException e) {
                log.error("Cannot find QuestionQuestion targetCode:"+targetCode+":parentCode:"+parentCode);
        

          }
          } catch (final BadDataException e) {
            e.printStackTrace();
          }
        });
  }


  /**
   * Upsert LinkAttribute to database
   * 
   * @param project
   */
  public void attributeLinks(Map<String, Object> project,  Map<String, DataType> dataTypeMap) {
    ((HashMap<String, HashMap>) project.get("attributeLink")).entrySet().stream().forEach(data -> {
      Map<String, Object> attributeLink = data.getValue();
      
      String code = ((String) attributeLink.get("code")).replaceAll("^\"|\"$", "");;
     String dataType = null;
      AttributeLink linkAttribute = null;
     
     try {
		dataType = ((String) attributeLink.get("dataType")).replaceAll("^\"|\"$", "");;
		   String name = ((String) attributeLink.get("name")).replaceAll("^\"|\"$", "");;
		     DataType dataTypeRecord = dataTypeMap.get(dataType);
		     ((HashMap<String, HashMap>) project.get("dataType")).get(dataType);
		     String privacyStr = (String) attributeLink.get("privacy");
		     Boolean privacy = "TRUE".equalsIgnoreCase(privacyStr);

		     linkAttribute = new AttributeLink(code, name);
		     linkAttribute.setDefaultPrivacyFlag(privacy);
		     linkAttribute.setDataType(dataTypeRecord);
		     service.upsert(linkAttribute);
	} catch (Exception e) {
		  String name = ((String) attributeLink.get("name")).replaceAll("^\"|\"$", "");;
		     String privacyStr = (String) attributeLink.get("privacy");
		     Boolean privacy = "TRUE".equalsIgnoreCase(privacyStr);

		      linkAttribute = new AttributeLink(code, name);
		     linkAttribute.setDefaultPrivacyFlag(privacy);
	}
     service.upsert(linkAttribute);

      
     });
  }

  /**
   * Insert Questions to Database
   * 
   * @param project
   */
  public void questions(Map<String, Object> project) {
    ((HashMap<String, HashMap>) project.get("questions")).entrySet().stream().forEach(data -> {
      Map<String, Object> questions = data.getValue();
      String code = (String) questions.get("code");
      String name = (String) questions.get("name");
      String attrCode = (String) questions.get("attribute_code");
      String html = (String) questions.get("html");
      String oneshotStr = (String) questions.get("oneshot");
      String readonlyStr = (String) questions.get("readonly");
      String hiddenStr = (String) questions.get("hidden");
      String mandatoryStr = (String) questions.get("mandatory");
 
      Boolean oneshot =getBooleanFromString(oneshotStr);
      Boolean readonly = getBooleanFromString(readonlyStr);
      Boolean mandatory = getBooleanFromString(mandatoryStr);
      Attribute attr;
      attr = service.findAttributeByCode(attrCode);
      Question q = new Question(code, name, attr);
      q.setOneshot(oneshot);
      q.setHtml(html);
      q.setReadonly(readonly);
      q.setMandatory(mandatory);
      //q.setRealm(REALM);
      Question existing = service.findQuestionByCode(code);
      if (existing == null) {
        if(isSynchronise()) {
          Question val = service.findQuestionByCode(q.getCode(), "hidden");
          if(val != null) {
            val.setRealm("genny");
            service.updateRealm(val);
            return;
          }
        }
    	  	service.insert(q);
      } else {
          existing.setName(name);
          existing.setHtml(html);
          existing.setOneshot(oneshot);
          existing.setReadonly(readonly);
          existing.setMandatory(mandatory);
          //existing.setRealm(REALM);
          service.upsert(existing); 
      }
    });
  }

  /**
   * Insert Ask to database
   * 
   * @param project
   */
  public void asks(Map<String, Object> project) {
    ((HashMap<String, HashMap>) project.get("ask")).entrySet().stream().forEach(data -> {
      Map<String, Object> asks = data.getValue();
      String attributeCode = (String) asks.get("attributeCode");
      String sourceCode = (String) asks.get("sourceCode");
      String expired = (String) asks.get("expired");
      String refused = (String) asks.get("refused");
      String targetCode = (String) asks.get("targetCode");
      String qCode = (String) asks.get("question_code");
      String name = (String) asks.get("name");
      String expectedId = (String) asks.get("expectedId");
      String weightStr = (String) asks.get("weight");
      String mandatoryStr = (String) asks.get("mandatory");
      String readonlyStr = (String) asks.get("readonly");
      String hiddenStr = (String) asks.get("hidden");
      final Double weight = Double.valueOf(weightStr);
      if ("QUE_USER_SELECT_ROLE".equals(targetCode)) {
          System.out.println("dummy");
      }
      Boolean mandatory = "TRUE".equalsIgnoreCase(mandatoryStr);
      Boolean readonly = "TRUE".equalsIgnoreCase(readonlyStr);
      Boolean hidden = "TRUE".equalsIgnoreCase(hiddenStr);
      Question question = service.findQuestionByCode(qCode);
      final Ask ask = new Ask(question, sourceCode, targetCode, mandatory, weight);
      ask.setName(name);
      ask.setHidden(hidden);
      ask.setReadonly(readonly);
      //ask.setRealm(REALM);
      service.insert(ask);
    });
  }


  /**
   * Get the Project named on the last row inheriting or updating records from previous projects
   * names in the Hosting Sheet
   * 
   * @return
   */
  public Map<String, Object> getProject() {
    Map<String, Object> lastProject = null;
    List<Map<String, Object>> projects = getProjects();
    if (projects.size() <= 1) {
      System.out.println("is null");
      return projects.get(0);
    } else {
      for (int count = 0; count < projects.size(); count++) {
        int subsequentIndex = count + 1;
        if (subsequentIndex == projects.size()) {
          break;
        }

        if (lastProject == null) {
          lastProject = upsertProjectMapProps(projects.get(count), projects.get(subsequentIndex));
        } else {
          lastProject = upsertProjectMapProps(lastProject, projects.get(subsequentIndex));
        }
      }
    }
    return lastProject;
  }

  /**
   * Call functions named after the classes
   */
  public Map<String, Object> persistProject(boolean isSynchronise, String table, boolean isDelete) {
    System.out.println("Persisting Project in BatchLoading");
    BatchLoading.isSynchronise = isSynchronise;
    BatchLoading.table = table;
    if(isSynchronise) {
      System.out.println("Table to synchronise: " + table);
      Map<String, Object> finalProject = getProject();
      if(!isDelete) {
        switch(table) {
          case "validation":
            validations(finalProject);
            savedProjectData.put("validations", finalProject.get("validations"));
            break;
          case "attribute":
            Map<String, DataType> dataTypes = dataType(finalProject);
            attributes(finalProject, dataTypes);
            savedProjectData.put("attributes", finalProject.get("attributes"));
            break;
          case "baseentity": 
            baseEntitys(finalProject);
            savedProjectData.put("baseEntitys", finalProject.get("baseEntitys"));
            break;
          case "entityattribute":
            baseEntityAttributes(finalProject);
            savedProjectData.put("attibutesEntity", finalProject.get("attibutesEntity"));
            break;
          case "attributelink":
            Map<String, DataType> linkDataTypes = dataType(finalProject);
            attributeLinks(finalProject, linkDataTypes);
            savedProjectData.put("attributeLink", finalProject.get("attributeLink"));
            break;
          case "entityentity":
            entityEntitys(finalProject);
            savedProjectData.put("basebase", finalProject.get("basebase"));
            break;
          case "question":
            questions(finalProject);
            savedProjectData.put("questions", finalProject.get("questions"));
            break;
          case "questionquestion":
            questionQuestions(finalProject);
            savedProjectData.put("questionQuestions", finalProject.get("questionQuestions"));
            break;
          case "message":
            messageTemplates(finalProject);
            savedProjectData.put("messages", finalProject.get("messages"));
            break;
          default:
            System.out.println("Error in table name. Please check.");
        }
        System.out.println("########## SYNCHRONISED GOOGLE SHEET #############");
      }
      return finalProject;
    }
    Map<String, Object> lastProject = getProject();
    savedProjectData = lastProject;
    System.out.println("+++++++++ AbouDSDSDSDSDSDSDSDSDSDSSDSDSDt to load Questions +++++++++++++");
    validations(lastProject);
    Map<String, DataType> dataTypes = dataType(lastProject);
    attributes(lastProject, dataTypes);
    baseEntitys(lastProject);
    baseEntityAttributes(lastProject);
    attributeLinks(lastProject, dataTypes);
    entityEntitys(lastProject);
    System.out.println("+++++++++ About to load Questions +++++++++++++");
    questions(lastProject);
    System.out.println("+++++++++ About to load QuestionQuestions +++++++++++++");
    questionQuestions(lastProject);
    System.out.println("+++++++++ Finished loading QuestionQuestions +++++++++++++");
    asks(lastProject);
    System.out.println("+++++++++ About to load Message Templates +++++++++++++");
    messageTemplates(lastProject);
    System.out.println("########## LOADED ALL GOOGLE DOC DATA #############");
    return lastProject;
  }

  /**
   * List of Project Maps
   * 
   * @return
   */
  public List<Map<String, Object>> getModules() {
    List<Map> projectsConfig = sheets.projectsImport();
    return projectsConfig.stream().map(data -> {
      String sheetID = (String) data.get("sheetID");
      String name = (String) data.get("name");
      String module = (String) data.get("module");
      final List<Map<String, Object>> map = new ArrayList<>();
      System.out.printf("%-80s%s%n", "Loading Project \033[31;1m" + name
          + "\033[0m and module \033[31;1m" + module + "\033[0m please wait...", "\uD83D\uDE31\t");
      Map<String, Object> fields = project(sheetID);
      System.out.printf("%-80s%s%n", "Project \033[31;1m" + name + "\033[0m and module \033[31;1m"
          + module + "\033[0m uploaded ", "\uD83D\uDC4F  \uD83D\uDC4F  \uD83D\uDC4F");
      map.add(fields);
      return map;
    }).reduce((ac, acc) -> {
      ac.addAll(acc);
      return ac;
    }).get();
  }

  public List<Map<String, Object>> getProjects() {
    List<Map> projectsConfig = null;
    Integer countDown = 10;
    while (countDown > 0) {
      try {
        projectsConfig = sheets.hostingImport();
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
    return projectsConfig.stream().map(data -> {
      String sheetID = (String) data.get("sheetID");
      sheets.setSheetId(sheetID);
      List<Map<String, Object>> listModuleProject = getModules();
      return listModuleProject;
    }).reduce((ac, acc) -> {
      ac.addAll(acc);
      return ac;
    }).get();
  }
  
  public static String updateRealm() {
    System.out.println("Inside UpdateRealm");
    String realm = DEFAULT_REALM;
    try {
      List<Map> projectsConfig = getProjectConfig();
      java.util.Iterator itr = projectsConfig.iterator();
      while(itr.hasNext()) {
        Map projectMap = (Map) itr.next();
        if(projectMap != null) {
          String realmValue = (String) projectMap.get("name");
          realm = realmValue.toLowerCase();
          System.out.println("REALM VALUE: " + realm);
        } 
      }
    } catch (Exception ee) { 
        ee.printStackTrace();
    }
    
    return realm;
  }
  
  public static List<Map> getProjectConfig() {
    return new BatchLoading(service).sheets.hostingImport();
  }

  /**
   * Import records from google sheets
   * 
   * @param projectType
   * @param path
   * @return
   */
  public Map<String, Object> project(final String projectType) {
    final Map<String, Object> genny = new HashMap<>();
    sheets.setSheetId(projectType);
    Integer numOfTries = 3;
    while (numOfTries > 0) {
      try {
        if(isSynchronise) {
          switch(table) {
            case "validation":
              Map<String, Map> validations = sheets.newGetVal();
              genny.put("validations", validations);
              break;
            case "attribute":
              Map<String, Map> dataTypes = sheets.newGetDType();
              genny.put("dataType", dataTypes);
              Map<String, Map> attrs = sheets.newGetAttr();
              genny.put("attributes", attrs);
              break;
            case "baseentity": 
              Map<String, Map> bes = sheets.newGetBase();
              genny.put("baseEntitys", bes);
              break;
            case "entityattribute":
              Map<String, Map> attr2Bes = sheets.newGetEntAttr();
              genny.put("attibutesEntity", attr2Bes);
              break;
            case "attributelink":
              Map<String, Map> attrLink = sheets.newGetAttrLink();
              genny.put("attributeLink", attrLink);
              break;
            case "entityentity":
              Map<String, Map> bes2Bes = sheets.newGetEntEnt();
              genny.put("basebase", bes2Bes);
              break;
            case "question":
              Map<String, Map> gQuestions = sheets.newGetQtn();
              genny.put("questions", gQuestions);
              break;
            case "questionquestion":
              Map<String, Map> que2Que = sheets.newGetQueQue();
              genny.put("questionQuestions", que2Que);
              break;
            case "message":
              Map<String, Map> messages = sheets.getMessageTemplates();
              genny.put("messages", messages);
              break;
            default:
              System.out.println("Error in table name. Please check.");
          }
          return genny;
        }
        
      System.out.println("validatios");
        Map<String, Map> validations = sheets.newGetVal();
        genny.put("validations", validations);
      System.out.println("datatypes");
        Map<String, Map> dataTypes = sheets.newGetDType();
        genny.put("dataType", dataTypes);
      System.out.println("attrs");
        Map<String, Map> attrs = sheets.newGetAttr();
        genny.put("attributes", attrs);
      System.out.println("bes");
        Map<String, Map> bes = sheets.newGetBase();
        genny.put("baseEntitys", bes);
      System.out.println("eas");
        Map<String, Map> attr2Bes = sheets.newGetEntAttr();
        genny.put("attibutesEntity", attr2Bes);
      System.out.println("attr link");
        Map<String, Map> attrLink = sheets.newGetAttrLink();
        genny.put("attributeLink", attrLink);
      System.out.println("vee");
        Map<String, Map> bes2Bes = sheets.newGetEntEnt();
        genny.put("basebase", bes2Bes);
      System.out.println("qtns");
        Map<String, Map> gQuestions = sheets.newGetQtn();
        genny.put("questions", gQuestions);
      System.out.println("vQQs");
        Map<String, Map> que2Que = sheets.newGetQueQue();
        genny.put("questionQuestions", que2Que);
      System.out.println("asks");
        Map<String, Map> asks = sheets.newGetAsk();
        genny.put("ask", asks);
      System.out.println("templates");
        Map<String, Map> messages = sheets.getMessageTemplates();
        genny.put("messages", messages);
        break;
      } catch (Exception e) {
        log.error("Failed to download Google Docs Configuration ... , will retry , trys left="
            + numOfTries);
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e1) {
          log.error("sleep exception..");
        } // sleep for 10 secs
      }

      numOfTries--;
    }

    if (numOfTries <= 0) {
      log.error("Failed to download Google Docs Configuration ... given up ...");
    }

    return genny;
  }

  /**
   * Override or update fields with non-null fields from Subprojects to Superprojects
   * 
   * @param superProject
   * @param subProject
   * @return
   */
  static int count = 0;

  @SuppressWarnings({"unchecked", "unused"})
  public Map<String, Object> upsertProjectMapProps(Map<String, Object> superProject,
      Map<String, Object> subProject) {
    superProject.entrySet().stream().forEach(map -> {
      if (subProject.get(map.getKey()) == null && superProject.get(map.getKey()) != null) {
        subProject.put(map.getKey(), superProject.get(map.getKey()));
      }
    });
    subProject.entrySet().stream().forEach(map -> {
      if (superProject.get(map.getKey()) == null && subProject.get(map.getKey()) != null) {
        superProject.put(map.getKey(), subProject.get(map.getKey()));
      }
    });
    subProject.entrySet().stream().forEach(map -> {
      final Map<String, Object> objects = (Map<String, Object>) subProject.get(map.getKey());
      if (objects != null) {
        objects.entrySet().stream().forEach(obj -> {
          if (((Map<String, Object>) superProject.get(map.getKey()))
              .<HashMap<String, Object>>get(obj.getKey()) != null) {
            Map<String, Object> mapp = (Map<String, Object>) obj.getValue();
            Map<String, Object> mapp2 = ((Map<String, HashMap>) superProject.get(map.getKey()))
                .<HashMap<String, Object>>get(obj.getKey());
            mapp.entrySet().stream().forEach(data -> {
              if (data.getValue() != null) {
                mapp2.put(data.getKey(), data.getValue());
              }
            });
          } else {
            ((Map<String, Object>) superProject.get(map.getKey()))
                .<HashMap<String, Object>>put(obj.getKey(), obj.getValue());
          }
        });
      }
    });
    return superProject;
  }

  public void messageTemplates(Map<String, Object> project) {
        
    if (project.get("messages") == null) {
        System.out.println("project.get(messages) is null");
        return;
    }
      
    ((HashMap<String, HashMap>) project.get("messages")).entrySet().stream().forEach(data -> {
        
        System.out.println("messages, data ::"+data);
      Map<String, Object> template = data.getValue();
      String code = (String) template.get("code");
      String name = (String) template.get("name");
      String description = (String) template.get("description");
      String subject = (String) template.get("subject");
      String emailTemplateDocId = (String) template.get("email");
      String smsTemplate = (String) template.get("sms");
      String toastTemplate = (String) template.get("toast");

      final QBaseMSGMessageTemplate templateObj = new QBaseMSGMessageTemplate();
      templateObj.setCode(code);
      templateObj.setName(name);
      templateObj.setCreated(LocalDateTime.now());
      templateObj.setDescription(description);
      templateObj.setEmail_templateId(emailTemplateDocId);
      templateObj.setSms_template(smsTemplate);
      templateObj.setSubject(subject);
      templateObj.setToast_template(toastTemplate);
      //templateObj.setRealm(REALM);
      
      if (StringUtils.isBlank(name)) {
            log.error("Empty Name");
      } else {
    	  try {
			QBaseMSGMessageTemplate msg = service.findTemplateByCode(code);
			try {
				if(msg != null) {
					msg.setName(name);
					msg.setDescription(description);
					msg.setEmail_templateId(emailTemplateDocId);
					msg.setSms_template(smsTemplate);
					msg.setSubject(subject);
					msg.setToast_template(toastTemplate);
					Long id = service.update(msg);
					System.out.println("updated message id ::" + id);
				} else {
					Long id = service.insert(templateObj);
					System.out.println("message id ::" + id);
				}
				
			} catch (Exception e) {
				log.error("Cannot update QDataMSGMessage " + code);
			}
				} catch (NoResultException e1) {
				  try {
				    if(BatchLoading.isSynchronise()) {
				      QBaseMSGMessageTemplate val = service.findTemplateByCode(templateObj.getCode(), "hidden");
		                if(val != null) {
		                  val.setRealm("genny");
		                  service.updateRealm(val);
		                  return;
		                }
		              }
				    Long id = service.insert(templateObj);
                    System.out.println("message id ::" + id);
				  } catch (javax.validation.ConstraintViolationException ce)     {
	                log.error("Error in saving message due to constraint issue:" + templateObj + " :" + ce.getLocalizedMessage());
	                log.info("Trying to update realm from hidden to genny");
	                templateObj.setRealm("genny");
	                service.updateRealm(templateObj);
	            }
					
				} catch (Exception e) {
			log.error("Cannot add MessageTemplate");
	
		}
       }
    });
  }


  private Boolean getBooleanFromString(final String booleanString) {
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

  /*public static String getRealm() {
    return REALM;
  }*/


  
  public static boolean isSynchronise() {
    return isSynchronise;
  }

}
