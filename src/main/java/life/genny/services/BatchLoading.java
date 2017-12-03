package life.genny.services;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import javax.persistence.NoResultException;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.Logger;

import life.genny.qwanda.Ask;
import life.genny.qwanda.Question;
import life.genny.qwanda.QuestionQuestion;
import life.genny.qwanda.attribute.Attribute;
import life.genny.qwanda.attribute.AttributeLink;
import life.genny.qwanda.datatype.DataType;
import life.genny.qwanda.entity.BaseEntity;
import life.genny.qwanda.exception.BadDataException;
import life.genny.qwanda.validation.Validation;
import life.genny.qwanda.validation.ValidationList;
import life.genny.qwandautils.GennySheets;
import life.genny.services.BaseEntityService2;

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



  // @Inject
  private BaseEntityService2 service;

  // private BaseEntityService service;
  public BatchLoading(BaseEntityService2 service) {
    this.service = service;
  }

  // protected BaseEntityService service = null;
  // protected BaseEntityService service1 = null;
  // protected EntityManagerFactory emf;
  // protected EntityManager em;



  
  File getGoogleCredentials()
  {
	  File credentialsFile = null;
	  
	  // Check if a user home stored credential exists.
	  // This credential may allow the user to access credentials for other private google docs.
	  
	  // Otherwise the standard public one is used.
	  
	  InputStream initialStream = this.getClass().getResourceAsStream("/credentials/genny");
	  
      Path path = Paths.get(System.getProperty("user.home")+"/.genny/credentials/genny");
      //if directory exists?
      if (!Files.exists(path)) {
          try {
              Files.createDirectories(path);
          } catch (IOException e) {
              //fail to create directory
              e.printStackTrace();
          }
      }
	  
	  credentialsFile = new File(System.getProperty("user.home")+"/.genny/credentials/genny/StoredCredential");
	    
	    try {
			java.nio.file.Files.copy(
			  initialStream, 
			  credentialsFile.toPath(), 
			  StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    finally {
	    IOUtils.closeQuietly(initialStream);
	    }
	  return path.toFile();
  }
  
  String getGoogleSecret()
  {
	  File secretFile = null;
	  
	  // Check if a user home stored credential exists.
	  // This credential may allow the user to access credentials for other private google docs.
	  
	  // Otherwise the standard public one is used.
	  
	  InputStream initialStream = this.getClass().getResourceAsStream("/gennySecret");
	  
      Path path = Paths.get(System.getProperty("user.home")+"/.genny/secret");
      //if directory exists?
      if (!Files.exists(path)) {
          try {
              Files.createDirectories(path);
          } catch (IOException e) {
              //fail to create directory
              e.printStackTrace();
          }
      }
	  
	  secretFile = new File(System.getProperty("user.home")+"/.genny/secret/gennySecret");
	    
      if (!Files.exists(secretFile.toPath())) {

	    try {
			java.nio.file.Files.copy(
			  initialStream, 
			  secretFile.toPath(), 
			  StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    finally {
	    IOUtils.closeQuietly(initialStream);
	    }
      }
	    
	    // read file into String
      String ret = readLineByLineJava8(secretFile.getAbsolutePath());
      
	  return ret;
  }
  
  String getGoogleHostId()
  {
	  File hostIdFile = null;
	  
	  // Check if a user home stored credential exists.
	  // This credential may allow the user to access credentials for other private google docs.
	  
	  // Otherwise the standard public one is used.
	  
	  InputStream initialStream = this.getClass().getResourceAsStream("/gennyHostId");
	  
      Path path = Paths.get(System.getProperty("user.home")+"/.genny/secret");
      //if directory exists?
      if (!Files.exists(path)) {
          try {
              Files.createDirectories(path);
          } catch (IOException e) {
              //fail to create directory
              e.printStackTrace();
          }
      }
	  
	  hostIdFile = new File(System.getProperty("user.home")+"/.genny/secret/gennyHostId");
	    
      if (!Files.exists(hostIdFile.toPath())) {

	    try {
			java.nio.file.Files.copy(
			  initialStream, 
			  hostIdFile.toPath(), 
			  StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    finally {
	    IOUtils.closeQuietly(initialStream);
	    }
      }
	    
	    // read file into String
      String ret = readLineByLineJava8(hostIdFile.getAbsolutePath());
      ret = ret.trim();
	  return ret;
  }
  
  private static String readLineByLineJava8(String filePath)
  {
      StringBuilder contentBuilder = new StringBuilder();

      try (Stream<String> stream = Files.lines( Paths.get(filePath), StandardCharsets.UTF_8))
      {
          stream.forEach(s -> contentBuilder.append(s).append("\n"));
      }
      catch (IOException e)
      {
          e.printStackTrace();
      }

      return contentBuilder.toString();
  }
  /**
   * Upsert Validation to database
   * 
   * @param project
   */
  public void validations(Map<String, Object> project) {
     ValidatorFactory factory = javax.validation.Validation.buildDefaultValidatorFactory();
     Validator validator = factory.getValidator();
    ((HashMap<String, HashMap>) project.get("validations")).entrySet().stream().forEach(data -> {
      Map<String, Object> validations = data.getValue();
      String regex = ((String) validations.get("regex")).replaceAll("^\"|\"$", "");;
      String code = ((String) validations.get("code")).replaceAll("^\"|\"$", "");;
      String name = ((String) validations.get("name")).replaceAll("^\"|\"$", "");;
      Validation val = new Validation(code, name, regex);
      System.out.print("code " + code + ",name:" + name + ",val:" + val);

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
    ((HashMap<String, HashMap>) project.get("attributes")).entrySet().stream().forEach(data -> {
      try {
		Map<String, Object> attributes = data.getValue();
		if (data.getKey().equals("FBK_USERNAME")) {
			System.out.println("Validation cast to BaseEntity Exception caused by this one...");
		}
		  String code = ((String) attributes.get("code")).replaceAll("^\"|\"$", "");;
		  String dataType = ((String) attributes.get("dataType")).replaceAll("^\"|\"$", "");;
		  String name = ((String) attributes.get("name")).replaceAll("^\"|\"$", "");;
		  DataType dataTypeRecord = dataTypeMap.get(dataType);
		  ((HashMap<String, HashMap>) project.get("dataType")).get(dataType);
		  Attribute attr = new Attribute(code, name, dataTypeRecord);
		  service.upsert(attr);
	} catch (Exception e) {
		// TODO Auto-generated catch block
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
    final Map<String, DataType> dataTypeMap = new HashMap<String, DataType>();
    ((HashMap<String, HashMap>) project.get("dataType")).entrySet().stream().forEach(data -> {
      Map<String, Object> dataType = data.getValue();
      String validations = ((String) dataType.get("validations"));
      String code = ((String) dataType.get("code")).replaceAll("^\"|\"$", "");;
      String name = ((String) dataType.get("name")).replaceAll("^\"|\"$", "");;
      final ValidationList validationList = new ValidationList();
      validationList.setValidationList(new ArrayList<Validation>());
      if (validations != null) {
        final String[] validationListStr = validations.split(",");
        for (final String validationCode : validationListStr) {
          Validation validation = service.findValidationByCode(validationCode);
          validationList.getValidationList().add(validation);
        }
      }
      if (!dataTypeMap.containsKey(code)) {
        final DataType dataTypeRecord = new DataType(name, validationList);
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
    ((HashMap<String, HashMap>) project.get("baseEntitys")).entrySet().stream().forEach(data -> {
      Map<String, Object> baseEntitys = data.getValue();
      String code = ((String) baseEntitys.get("code")).replaceAll("^\"|\"$", "");;
      String name = ((String) baseEntitys.get("name")).replaceAll("^\"|\"$", "");;
      BaseEntity be = new BaseEntity(code, name);
      service.upsert(be);
    });
  }

  /**
   * Upsert BaseEntities with Attributes
   * 
   * @param project
   */
  public void baseEntityAttributes(Map<String, Object> project) {
    ((HashMap<String, HashMap>) project.get("attibutesEntity")).entrySet().stream()
        .forEach(data -> {
          Map<String, Object> baseEntityAttr = data.getValue();
          String attributeCode =
              ((String) baseEntityAttr.get("attributeCode")).replaceAll("^\"|\"$", "");;
          String valueString =
              ((String) baseEntityAttr.get("valueString")).replaceAll("^\"|\"$", "");;
          String baseEntityCode =
              ((String) baseEntityAttr.get("baseEntityCode")).replaceAll("^\"|\"$", "");;
          String weight = (String) baseEntityAttr.get("weight");
          Attribute attribute = null;
          BaseEntity be = null;
          try {
            attribute = service.findAttributeByCode(attributeCode);
            be = service.findBaseEntityByCode(baseEntityCode);
            Double weightField = null;
            try {
            		weightField = Double.valueOf(weight);
            } catch (java.lang.NumberFormatException ee) {
            	weightField = 0.0;
            }
            try {
              be.addAttribute(attribute, weightField, valueString);
            } catch (final BadDataException e) {
              e.printStackTrace();
            }
            service.update(be);
          } catch (final NoResultException e) {
          
        }
        });
  }

  /**
   * Upsert EntityEntity
   * 
   * @param project
   */
  public void entityEntitys(Map<String, Object> project) {
    ((HashMap<String, HashMap>) project.get("basebase")).entrySet().stream().forEach(data -> {
      Map<String, Object> entEnts = data.getValue();
      String linkCode = ((String) entEnts.get("linkCode"));
      String parentCode = ((String) entEnts.get("parentCode"));
      String targetCode = ((String) entEnts.get("targetCode"));
      String weightStr = ((String) entEnts.get("weight"));
      String valueString = ((String) entEnts.get("valueString"));
      final Double weight = Double.valueOf(weightStr);
      BaseEntity sbe = null;
      BaseEntity tbe = null;
      Attribute linkAttribute = service.findAttributeByCode(linkCode);
      try {
        sbe = service.findBaseEntityByCode(parentCode);
        tbe = service.findBaseEntityByCode(targetCode);
        sbe.addTarget(tbe, linkAttribute, weight, valueString);
        
        service.update(sbe);
      } catch (final NoResultException e) {
      } catch (final BadDataException e) {
        e.printStackTrace();
      }
    });
  }
  
  /**
   * Upsert QuestionQuestion
   * 
   * @param project
   */
  public void questionQuestions(Map<String, Object> project) {

    ((HashMap<String, HashMap>) project.get("questionQuestions")).entrySet().stream().forEach(data -> {
      Map<String, Object> queQues = data.getValue();
      String parentCode = ((String) queQues.get("parentCode"));
      String targetCode = ((String) queQues.get("targetCode"));
      String weightStr = ((String) queQues.get("weight"));
      String mandatoryStr = ((String) queQues.get("mandatory"));
      final Double weight = Double.valueOf(weightStr);
      Boolean mandatory = "TRUE".equalsIgnoreCase(mandatoryStr);
      Question sbe = null;
      Question tbe = null;

      try {
        sbe = service.findQuestionByCode(parentCode);
        tbe = service.findQuestionByCode(targetCode);
        QuestionQuestion qq = sbe.addChildQuestion(tbe.getCode(), weight, mandatory);
       
        sbe = service.upsert(sbe);
        
      } catch (final NoResultException e) {
    	  System.out.println("No Result! in QuestionQuestions Loading");
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
  public void attributeLinks(Map<String, Object> project) {
    ((HashMap<String, HashMap>) project.get("attributeLink")).entrySet().stream().forEach(data -> {
      Map<String, Object> attributeLink = data.getValue();
      String code = ((String) attributeLink.get("code")).replaceAll("^\"|\"$", "");;
      String name = ((String) attributeLink.get("name")).replaceAll("^\"|\"$", "");;
      final AttributeLink linkAttribute = new AttributeLink(code, name);
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
      Attribute attr;
      attr = service.findAttributeByCode(attrCode);
      final Question q = new Question(code, name, attr);
      service.insert(q);
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
      Question question = service.findQuestionByCode(qCode);
      final Ask ask = new Ask(question, sourceCode, targetCode);
      ask.setName(name);
      service.insert(ask);
    });
  }


  // public void main(String... args) {
  // emf = Persistence.createEntityManagerFactory("mysql");
  // em = emf.createEntityManager();
  // service = new BaseEntityService(em);
  // em.getTransaction().begin();
  // persistProject();
  // em.getTransaction().commit();
  // em.close();
  // }

  /**
   * Get the Project named on the last row inheriting or updating records from previous projects
   * names in the Hosting Sheet
   * 
   * @return
   */
  public Map<String, Object> getProject() {
    Map<String, Object> lastProject = null;
    if (getProjects().size() == 1) {
      return getProjects().get(0);
    } else {
      for (int count = 0; count < getProjects().size(); count++) {
        int subsequentIndex = count + 1;
        if (lastProject == null) {
          lastProject =
              upsertProjectMapProps(getProjects().get(count), getProjects().get(subsequentIndex));
        } else {
          lastProject = upsertProjectMapProps(lastProject, getProjects().get(subsequentIndex));
        }
      }
    }
    return lastProject;
  }

  /**
   * Call functions named after the classes
   */
  public void persistProject() {
	  System.out.println("Persisting Project in BatchLoading");
    Map<String, Object> lastProject = getProject();
    validations(lastProject);
    Map<String, DataType> dataTypes = dataType(lastProject);
    attributes(lastProject, dataTypes);
    baseEntitys(lastProject);
    baseEntityAttributes(lastProject);
    attributeLinks(lastProject);
    entityEntitys(lastProject);
    System.out.println("+++++++++ About to load Questions +++++++++++++");
    questions(lastProject);
    System.out.println("+++++++++ About to load QuestionQuestions +++++++++++++");
    questionQuestions(lastProject);
    System.out.println("+++++++++ Finished loading QuestionQuestions +++++++++++++");
    asks(lastProject);
  }

  /**
   * List of Project Maps
   * 
   * @return
   */
  public List<Map<String, Object>> getProjects() {
	 File credentialPath = getGoogleCredentials();
	 String secret = getGoogleSecret();
    GennySheets sheets = new GennySheets(secret, getGoogleHostId(), credentialPath);
    List<Map> projectsConfig = sheets.projectsImport(credentialPath);
    return projectsConfig.stream().map(data -> {
      String sheetID = (String) data.get("sheetID");
      final List<Map<String, Object>> map = new ArrayList<Map<String, Object>>();
      Map<String, Object> fields = project(sheetID);
      map.add(fields);
      return map;
    }).reduce((ac, acc) -> {
      ac.addAll(acc);
      return ac;
    }).get();
  }

  /**
   * Import records from google sheets
   * 
   * @param projectType
   * @param path
   * @return
   */
  public Map<String, Object> project(final String projectType) {
    final Map<String, Object> genny = new HashMap<String, Object>();
	 File credentialPath = getGoogleCredentials();

    GennySheets sheets = new GennySheets(getGoogleSecret(), projectType, credentialPath);

    Integer numOfTries = 3;

    while (numOfTries > 0) {
      try {
        Map<String, Map> validations = sheets.newGetVal();
        Map<String, Map> dataTypes = sheets.newGetDType();
        Map<String, Map> attrs = sheets.newGetAttr();
        Map<String, Map> bes = sheets.newGetBase();
        Map<String, Map> attr2Bes = sheets.newGetEntAttr();
        Map<String, Map> attrLink = sheets.newGetAttrLink();
        Map<String, Map> bes2Bes = sheets.newGetEntEnt();
        Map<String, Map> gQuestions = sheets.newGetQtn();
        Map<String, Map> que2Que = sheets.newGetQueQue();
        Map<String, Map> asks = sheets.newGetAsk();

        genny.put("validations", validations);
        genny.put("dataType", dataTypes);
        genny.put("attributes", attrs);
        genny.put("baseEntitys", bes);
        genny.put("attibutesEntity", attr2Bes);
        genny.put("attributeLink", attrLink);
        genny.put("basebase", bes2Bes);
        genny.put("questions", gQuestions);
        genny.put("questionQuestions",que2Que);
        genny.put("ask", asks);
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

    if (numOfTries <= 0)
      log.error("Failed to download Google Docs Configuration ... given up ...");

    return genny;
  }

  /**
   * Override or update fields with non-null fields from Subprojects to Superprojects
   * 
   * @param superProject
   * @param subProject
   * @return
   */
  @SuppressWarnings({"unchecked", "unused"})
  public Map<String, Object> upsertProjectMapProps(Map<String, Object> superProject,
      Map<String, Object> subProject) {
    subProject.entrySet().stream().forEach(map -> {
      final Map<String, Object> objects = (Map<String, Object>) subProject.get(map.getKey());
      objects.entrySet().stream().forEach(obj -> {
        if (((Map<String, Object>) superProject.get(map.getKey()))
            .<HashMap<String, Object>>get(obj.getKey()) != null) {
          Map<String, Object> mapp = ((Map<String, Object>) obj.getValue());
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
    });
    return superProject;
  }

}
