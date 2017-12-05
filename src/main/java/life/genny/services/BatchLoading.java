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

  private final String secret = System.getenv("GOOGLE_CLIENT_SECRET");
  private final String hostingSheetId = System.getenv("GOOGLE_HOSTING_SHEET_ID");
  File credentialPath =
      new File(System.getProperty("user.home"), ".genny/sheets.googleapis.com-java-quickstart");
  GennySheets sheets = new GennySheets(secret, hostingSheetId, credentialPath);

  /**
   * Upsert Validation to database
   * 
   * @param project
   */
  public void validations(Map<String, Object> project) {
    if (project.get("validations") == null)
      return;
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
    if (project.get("attributes") == null)
      return;
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
    if (project.get("dataType") == null)
      return null;
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
    if (project.get("baseEntitys") == null)
      return;
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
    if (project.get("attibutesEntity") == null)
      return;
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
    if (project.get("basebase") == null)
      return;
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
    if (project.get("questionQuestions") == null)
      return;
    ((HashMap<String, HashMap>) project.get("questionQuestions")).entrySet().stream()
        .forEach(data -> {
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

            qq = service.upsert(qq);

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
      String weightStr = ((String) asks.get("weight"));
      String mandatoryStr = ((String) asks.get("mandatory"));
      final Double weight = Double.valueOf(weightStr);
      Boolean mandatory = "TRUE".equalsIgnoreCase(mandatoryStr);

      Question question = service.findQuestionByCode(qCode);
      final Ask ask = new Ask(question, sourceCode, targetCode, mandatory, weight);
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
    List<Map<String, Object>> projects = getProjects();
    if (projects.size() <= 1) {
      System.out.println("is null");
      return projects.get(0);
    } else {
      for (int count = 0; count < projects.size(); count++) {
        int subsequentIndex = count + 1;

        if (subsequentIndex == projects.size())
          break;

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
    List<Map> projectsConfig = sheets.projectsImport();
    return projectsConfig.stream().map(data -> {
      String sheetID = (String) data.get("sheetID");
      String name = (String) data.get("name");
      String module = (String) data.get("module");
      final List<Map<String, Object>> map = new ArrayList<Map<String, Object>>();
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

  /**
   * Import records from google sheets
   * 
   * @param projectType
   * @param path
   * @return
   */
  public Map<String, Object> project(final String projectType) {
    final Map<String, Object> genny = new HashMap<String, Object>();
    sheets.setSheetId(projectType);
    Integer numOfTries = 3;
    while (numOfTries > 0) {
      try {
        Map<String, Map> validations = sheets.newGetVal();
        genny.put("validations", validations);
        Map<String, Map> dataTypes = sheets.newGetDType();
        genny.put("dataType", dataTypes);
        Map<String, Map> attrs = sheets.newGetAttr();
        genny.put("attributes", attrs);
        Map<String, Map> bes = sheets.newGetBase();
        genny.put("baseEntitys", bes);
        Map<String, Map> attr2Bes = sheets.newGetEntAttr();
        genny.put("attibutesEntity", attr2Bes);
        Map<String, Map> attrLink = sheets.newGetAttrLink();
        genny.put("attributeLink", attrLink);
        Map<String, Map> bes2Bes = sheets.newGetEntEnt();
        genny.put("basebase", bes2Bes);
        Map<String, Map> gQuestions = sheets.newGetQtn();
        genny.put("questions", gQuestions);
        Map<String, Map> que2Que = sheets.newGetQueQue();
        genny.put("questionQuestions", que2Que);
        Map<String, Map> asks = sheets.newGetAsk();
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
    superProject.entrySet().stream().forEach(map -> {
      System.out.println(map.getKey());
      final Map<String, Object> objects = (Map<String, Object>) superProject.get(map.getKey());
      if(superProject.get(map.getKey())==null) {
        superProject.put(map.getKey(), subProject.get(map.getKey()));
      }
    });
    subProject.entrySet().stream().forEach(map -> {
      System.out.println(map.getKey());
      final Map<String, Object> objects = (Map<String, Object>) subProject.get(map.getKey());
      if(subProject.get(map.getKey())==null) {
        subProject.put(map.getKey(), superProject.get(map.getKey()));
      }
    });
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
