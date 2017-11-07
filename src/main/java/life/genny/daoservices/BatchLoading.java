package life.genny.daoservices;

import static java.lang.System.out;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.NoResultException;
import javax.persistence.Persistence;
import life.genny.qwanda.Answer;
import life.genny.qwanda.Ask;
import life.genny.qwanda.Question;
import life.genny.qwanda.attribute.Attribute;
import life.genny.qwanda.attribute.AttributeLink;
import life.genny.qwanda.datatype.DataType;
import life.genny.qwanda.entity.BaseEntity;
import life.genny.qwanda.exception.BadDataException;
import life.genny.qwanda.validation.Validation;
import life.genny.qwanda.validation.ValidationList;

/**
 * @author helios
 *
 */
public class BatchLoading {

  protected static QwandaUpserts service = null;
  protected static BaseEntityService service1 = null;
  protected static EntityManagerFactory emf;
  protected static EntityManager em;

  static File credentialPath = new File(System.getProperty("user.home"), ".credentials/genny");

  private final static String secret = System.getenv("GOOGLE_CLIENT_SECRET");
  private final static String sheetId = "1Ie5rCqyQy-7SmK_YAufK9RPw4C7awEf0KLuCfsV622M";
  static File gennyPath = new File(System.getProperty("user.home"), ".credentials/genny");

  /**
   * Upsert Validation to database
   * 
   * @param project
   */
  public static void validations(Map<String, Object> project) {
    ((HashMap<String, HashMap>) project.get("validations")).entrySet().stream().forEach(data -> {
      Map<String, Object> validations = data.getValue();
      String regex = (String) validations.get("regex");
      String code = (String) validations.get("code");
      String name = (String) validations.get("name");
      Validation val = new Validation(code, name, regex);
      service.upsert(val);
    });
  }

  /**
   * Upsert Attribute to database
   * 
   * @param project
   * @param dataTypeMap
   */
  public static void attributes(Map<String, Object> project, Map<String, DataType> dataTypeMap) {
    ((HashMap<String, HashMap>) project.get("attributes")).entrySet().stream().forEach(data -> {
      Map<String, Object> attributes = data.getValue();
      String code = (String) attributes.get("code");
      String dataType = (String) attributes.get("dataType");
      String name = (String) attributes.get("name");
      DataType dataTypeRecord = dataTypeMap.get(dataType);
      ((HashMap<String, HashMap>) project.get("dataType")).get(dataType);
      Attribute attr = new Attribute(code, name, dataTypeRecord);
      service.upsert(attr);
    });
  }

  /**
   * Initialized Map of DataTypes
   * 
   * @param project
   * @return
   */
  public static Map<String, DataType> dataType(Map<String, Object> project) {
    final Map<String, DataType> dataTypeMap = new HashMap<String, DataType>();
    ((HashMap<String, HashMap>) project.get("dataType")).entrySet().stream().forEach(data -> {
      Map<String, Object> dataType = data.getValue();
      String validations = (String) dataType.get("validations");
      String code = (String) dataType.get("code");
      String name = (String) dataType.get("name");
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
  public static void baseEntitys(Map<String, Object> project) {
    ((HashMap<String, HashMap>) project.get("baseEntitys")).entrySet().stream().forEach(data -> {
      Map<String, Object> baseEntitys = data.getValue();
      String code = (String) baseEntitys.get("code");
      String name = (String) baseEntitys.get("name");
      BaseEntity be = new BaseEntity(code, name);
      service.upsert(be);
    });
  }

  /**
   * Upsert BaseEntities with Attributes
   * 
   * @param project
   */
  public static void baseEntityAttributes(Map<String, Object> project) {
    ((HashMap<String, HashMap>) project.get("attibutesEntity")).entrySet().stream()
        .forEach(data -> {
          Map<String, Object> baseEntityAttr = data.getValue();
          String attributeCode = (String) baseEntityAttr.get("attributeCode");
          String valueString = (String) baseEntityAttr.get("valueString");
          String baseEntityCode = (String) baseEntityAttr.get("baseEntityCode");
          String weight = (String) baseEntityAttr.get("weight");
          Attribute attribute = null;
          BaseEntity be = null;
          try {
            attribute = service.findAttributeByCode(attributeCode);
            be = service.findBaseEntityByCode(baseEntityCode);
            final Double weightField = Double.valueOf(weight);
            try {
              be.addAttribute(attribute, weightField, valueString);
            } catch (final BadDataException e) {
              e.printStackTrace();
            }
            service.upsert(be);
          } catch (final NoResultException e) {
          }
        });
  }

  /**
   * Upsert EntityEntity
   * 
   * @param project
   */
  public static void entityEntitys(Map<String, Object> project) {
    ((HashMap<String, HashMap>) project.get("basebase")).entrySet().stream().forEach(data -> {
      Map<String, Object> entEnts = data.getValue();
      String linkCode = (String) entEnts.get("linkCode");
      String parentCode = (String) entEnts.get("parentCode");
      String targetCode = (String) entEnts.get("targetCode");
      String weightStr = (String) entEnts.get("weight");
      final Double weight = Double.valueOf(weightStr);
      BaseEntity sbe = null;
      BaseEntity tbe = null;
      Attribute linkAttribute = service.findAttributeByCode(linkCode);
      try {
        sbe = service.findBaseEntityByCode(parentCode);
        tbe = service.findBaseEntityByCode(targetCode);
        sbe.addTarget(tbe, linkAttribute, weight);
        service.upsert(sbe);
      } catch (final NoResultException e) {
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
  public static void attributeLinks(Map<String, Object> project) {
    ((HashMap<String, HashMap>) project.get("attributeLink")).entrySet().stream().forEach(data -> {
      Map<String, Object> attributeLink = data.getValue();
      String code = (String) attributeLink.get("code");
      String name = (String) attributeLink.get("name");
      final AttributeLink linkAttribute = new AttributeLink(code, name);
      service.upsert(linkAttribute);
    });
  }

  /**
   * Insert Questions to Database
   * 
   * @param project
   */
  public static void questions(Map<String, Object> project) {
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
  public static void asks(Map<String, Object> project) {
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


  public static void main(String... args) {
    emf = Persistence.createEntityManagerFactory("mysql");
    em = emf.createEntityManager();
    service = new QwandaUpserts(em);
    em.getTransaction().begin();
    persistProject();
    em.getTransaction().commit();
    em.close();
  }

  /**
   * Get the Project named on the last row inheriting or updating records from previous projects
   * names in the Hosting Sheet
   * 
   * @return
   */
  public static Map<String, Object> getProject() {
    Map<String, Object> lastProject = null;
    if (getProjects().size() > 1) {
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
   * Call static functions named after the classes
   */
  public static void persistProject() {
    Map<String, Object> lastProject = getProject();
    validations(lastProject);
    Map<String, DataType> dataTypes = dataType(lastProject);
    attributes(lastProject, dataTypes);
    baseEntitys(lastProject);
    baseEntityAttributes(lastProject);
    attributeLinks(lastProject);
    entityEntitys(lastProject);
    questions(lastProject);
    asks(lastProject);
  }

  /**
   * List of Project Maps
   * 
   * @return
   */
  public static List<Map<String, Object>> getProjects() {    
    GennySheets sheets = new GennySheets(secret, sheetId, gennyPath);
    List<Map> projectsConfig = sheets.projectsImport(gennyPath);
    return projectsConfig.stream().map(data -> {
      String sheetID = (String) data.get("sheetID");
      final List<Map<String, Object>> map = new ArrayList<Map<String, Object>>();
      Map<String, Object> fields = project(sheetID, gennyPath);
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
  public static Map<String, Object> project(final String projectType, final File path) {
    GennySheets sheets = new GennySheets(secret, projectType, path);
    Map<String, Map> validations = sheets.newGetVal();
    Map<String, Map> dataTypes = sheets.newGetDType();
    Map<String, Map> attrs = sheets.newGetAttr();
    Map<String, Map> bes = sheets.newGetBase();
    Map<String, Map> attr2Bes = sheets.newGetEntAttr();
    Map<String, Map> attrLink = sheets.newGetAttrLink();
    Map<String, Map> bes2Bes = sheets.newGetEntEnt();
    Map<String, Map> gQuestions = sheets.newGetQtn();
    Map<String, Map> asks = sheets.newGetAsk();
    final Map<String, Object> genny = new HashMap<String, Object>();
    genny.put("validations", validations);
    genny.put("dataType", dataTypes);
    genny.put("attributes", attrs);
    genny.put("baseEntitys", bes);
    genny.put("attibutesEntity", attr2Bes);
    genny.put("attributeLink", attrLink);
    genny.put("basebase", bes2Bes);
    genny.put("questions", gQuestions);
    genny.put("ask", asks);
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
  public static Map<String, Object> upsertProjectMapProps(Map<String, Object> superProject,
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
