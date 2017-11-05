package life.genny.daoservices;

import static java.lang.System.out;
import java.io.File;
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

public class MainTest {

  // protected static BaseEntityService service1 = null;
  protected static BatchLoading service = null;
  protected static BaseEntityService service1 = null;
  protected static EntityManagerFactory emf;
  protected static EntityManager em;

  static File credentialPath = new File(System.getProperty("user.home"), ".credentials/genny");

  private final static String secret = System.getenv("GOOGLE_CLIENT_SECRET");
  private final static String gennyID = System.getenv("GOOGLE_SHEETID");
  private final static String channel40 = "1O1tDIJksSv6o9EGq1xoPnb5pnuEkRMwlGcQZfMkNFQM";
  private final static String all = "1Ie5rCqyQy-7SmK_YAufK9RPw4C7awEf0KLuCfsV622M";
  static File gennyPath = new File(System.getProperty("user.home"), ".credentials/genny");
  static File channelPath = new File(System.getProperty("user.home"), ".credentials/channel");

  private final String g = System.getenv("GOOGLE_CLIENT_SECRET");
  private final String go = System.getenv("GOOGLE_SHEETID");


  public static void main(String... args) {
    final GennySheets gennySheets =
        new GennySheets(secret, gennyID, new File(System.getProperty("user.home"),
            ".credentials/sheets.googleapis.com-java-quickstart"));
    emf = Persistence.createEntityManagerFactory("mysql");
    em = emf.createEntityManager();
    service = new BatchLoading(em);
    service1 = new BaseEntityService();
    em.getTransaction().begin();
    Map<String, Object> genny = project(gennyID, new File(System.getProperty("user.home"),
        ".credentials/sheets.googleapis.com-java-quickstart"));
    Map<String, Object> channel = project(channel40, new File(System.getProperty("user.home"),
        ".credentials/sheets.googleapis.com-java-quickstart"));
    Map<String, Object> lastProject = modifyObj(genny, channel);
    
//    ((Map)lastProject.get("baseEntitys")).entrySet().stream().forEach(out::println);
    
    lastProject.entrySet().stream().forEach(out::println);
    
    ((HashMap<String, HashMap>)lastProject.get("validations")).entrySet().stream().forEach(data->{
      Map<String, Object> validations = data.getValue();
      String regex = (String) validations.get("regex");
      String code = (String) validations.get("code");
      String name = (String) validations.get("name");
      Validation val = new Validation(code, name, regex);
      service.upsert(val);
    });
    
    final Map<String, DataType> dataTypeMap = new HashMap<String, DataType>(); 
    ((HashMap<String, HashMap>)lastProject.get("dataType")).entrySet().stream().forEach(data->{
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
    
    ((HashMap<String, HashMap>)lastProject.get("attributes")).entrySet().stream().forEach(data->{
      Map<String, Object> attributes = data.getValue();
      String code = (String) attributes.get("code");
      String dataType = (String) attributes.get("dataType");
      String name = (String) attributes.get("name");
      DataType dataTypeRecord = dataTypeMap.get(dataType);
      ((HashMap<String, HashMap>)lastProject.get("dataType")).get(dataType); 
      Attribute attr = new Attribute(code, name, dataTypeRecord );
      service.upsert(attr);
    });
   
    ((HashMap<String, HashMap>)lastProject.get("baseEntitys")).entrySet().stream().forEach(data->{
      Map<String, Object> baseEntitys = data.getValue();
      String code = (String) baseEntitys.get("code");
      String name = (String) baseEntitys.get("name");
      BaseEntity be = new BaseEntity(code, name);
      service.upsert(be);
    });
    
    ((HashMap<String, HashMap>)lastProject.get("attibutesEntity")).entrySet().stream().forEach(data->{
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
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        service.upsert(be);
      } catch (final NoResultException e) {}
    });
    
    ((HashMap<String, HashMap>)lastProject.get("attributeLink")).entrySet().stream().forEach(data->{
      Map<String, Object> attributeLink = data.getValue();
      String code = (String) attributeLink.get("code");
      String name = (String) attributeLink.get("name");
      final AttributeLink linkAttribute = new AttributeLink(code, name);
      service.upsert(linkAttribute);
    });
    
    ((HashMap<String, HashMap>)lastProject.get("basebase")).entrySet().stream().forEach(data->{
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
    
    ((HashMap<String, HashMap>)lastProject.get("questions")).entrySet().stream().forEach(data->{
      Map<String, Object> questions = data.getValue();
      String code = (String) questions.get("code");
      String name = (String) questions.get("name");
      String attrCode = (String) questions.get("attribute_code");
      Attribute attr;
      attr = service.findAttributeByCode(attrCode);
      final Question q = new Question(code, name, attr);
      service.insert(q);
    });
    
    ((HashMap<String, HashMap>)lastProject.get("ask")).entrySet().stream().forEach(data->{
      Map<String, Object> asks = data.getValue();
      String attributeCode = (String) asks.get("attributeCode");
      String sourceCode = (String) asks.get("sourceCode");
      String expired = (String) asks.get("expired");
      String refused = (String) asks.get("refused");
      String targetCode = (String) asks.get("targetCode");
      String qCode = (String) asks.get("question_code");
      String name = (String) asks.get("name");
      String expectedId = (String) asks.get("expectedId");
      Question q;
      q = service.findQuestionByCode(qCode);
      final Ask a = new Ask(q, sourceCode, targetCode);
      a.setName(name);
      service.insert(a);
    });
    
 // Map<String, Validation> validations = (Map<String, Validation>)
    // lastProject.get("validations");
    // validations.entrySet().stream().map(data -> data.getValue()).forEach(validation -> {
    // service.upsert(validation);
    // });
    
    // ((Map)lastProject.get("baseEntitys")).entrySet().stream().forEach(out::println);

    // Map<String, Validation> validations = (Map<String, Validation>)
    // lastProject.get("validations");
    // validations.entrySet().stream().map(data -> data.getValue()).forEach(validation -> {
    // service.upsert(validation);
    // });
    //
    // Map<String, Attribute> attributes = (Map<String, Attribute>) lastProject.get("attributes");
    // attributes.entrySet().stream().map(data -> data.getValue()).forEach(attr -> {
    // service.upsert(attr);
    // });
    //
    // Map<String, BaseEntity> baseEntitys = (Map<String, BaseEntity>)
    // lastProject.get("baseEntitys");
    // baseEntitys.entrySet().stream().map(data -> data.getValue()).forEach(be -> {
    // service.upsert(be);
    // });
    // Map<String, BaseEntity> attibutesEntity =
    // (Map<String, BaseEntity>) lastProject.get("attibutesEntity");
    // attibutesEntity.entrySet().stream().map(data -> data.getValue()).forEach(ba -> {
    // BaseEntity baseEntity = service.findBaseEntityByCode(ba.getCode());
    // ba.getBaseEntityAttributes().stream().forEach(data -> {
    // Attribute att = service.findAttributeByCode(data.getAttribute().getCode());
    // try {
    // baseEntity.addAttribute(att, data.getWeight(), data.getValueString());
    // } catch (BadDataException e) {
    // e.printStackTrace();
    // }
    // });
    // service.em.merge(baseEntity);
    // });
    //
    // Map<String, AttributeLink> attributeLink =
    // (Map<String, AttributeLink>) lastProject.get("attributeLink");
    // attributeLink.entrySet().stream().map(data -> data.getValue()).forEach(al -> {
    // service.upsert(al);
    // });
    //
    // Map<String, Object> basebase = (Map<String, Object>) lastProject.get("basebase");
    // basebase.entrySet().stream().map(data -> data.getValue()).forEach(bb -> {
    // Map<String, String> map = (Map<String, String>) bb;
    // out.println(map);
    // BaseEntity sbe = service.findBaseEntityByCode(map.get("parentCode"));
    // BaseEntity tbe = service.findBaseEntityByCode(map.get("targetCode"));
    // Attribute linkAttribute2 = service.findAttributeByCode(map.get("linkCode"));
    // final Double weight = Double.valueOf(map.get("weight"));
    // try {
    // sbe.addTarget(tbe, linkAttribute2, weight);
    // } catch (BadDataException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }
    // service.em.merge(sbe);
    // });
    //
    // Map<String, Question> questions = (Map<String, Question>) lastProject.get("questions");
    // questions.entrySet().stream().map(data -> data.getValue()).forEach(q -> {
    // out.println("dfsdf"+q.getAttribute().getCode());
    // Attribute attr = service.findAttributeByCode(q.getAttribute().getCode());
    // System.out.println("_______"+attr);
    // Question que = new Question(q.getCode(),q.getName(), attr);
    // service.insert(que);
    // });
    System.out.println("**************************III************************************");
    em.getTransaction().commit();

    em.close();
    // emf.close();
  }

  public static void create() {
    Map<String, Object> genny = project(gennyID, new File(System.getProperty("user.home"),
        ".credentials/sheets.googleapis.com-java-quickstart"));
    Map<String, Object> channel = project(channel40, new File(System.getProperty("user.home"),
        ".credentials/sheets.googleapis.com-java-quickstart"));
    Map<String, Object> lastProject = modifyObj(genny, new HashMap<String, Object>());
    Map<String, Validation> validations = (Map<String, Validation>) lastProject.get("validations");
    validations.entrySet().stream().map(data -> data.getValue()).forEach(validation -> {
      // service.upsert(validation);
    });
  }

  // @SuppressWarnings({"unchecked", "unused"})
  // public static Map<String, Object> modifyObj(Map<String, Object> superProject,
  // Map<String, Object> subProject) {
  //
  // subProject.entrySet().stream().forEach(map -> {
  // final Map<String, Object> objects = (Map<String, Object>) subProject.get(map.getKey());
  // objects.entrySet().stream().forEach(obj -> {
  // try {
  // if (((Map<String, Object>) superProject.get(map.getKey()))
  // .<HashMap<String, Object>>get(obj.getKey()) != null) {
  // BeanNotNullFields.getInstance()
  // .copyProperties(((Map<String, Object>) superProject.get(map.getKey()))
  // .<HashMap<String, Object>>get(obj.getKey()), obj.getValue());
  // } else {
  // ((Map<String, Object>) superProject.get(map.getKey()))
  // .<HashMap<String, Object>>put(obj.getKey(), obj.getValue());
  // }
  // } catch (IllegalAccessException | InvocationTargetException e) {
  // e.printStackTrace();
  // }
  // });
  // });
  // return superProject;
  // }



  // public static Map<String, Object> project(final String projectType, final File path) {
  // GennySheets sheets = new GennySheets(secret, projectType, path);
  // Map<String, Validation> gValidations = sheets.validationData();
  // Map<String, DataType> gDataTypes = sheets.dataTypesData(gValidations);
  // Map<String, Attribute> gAttrs = sheets.attributesData(gDataTypes);
  // Map<String, BaseEntity> gBes = sheets.baseEntityData();
  // Map<String, BaseEntity> bes = sheets.baseEntity();
  // Map<String, BaseEntity> gAttr2Bes = sheets.attr2BaseEntitys(gAttrs, bes);
  // Map<String, AttributeLink> gAttrLink = sheets.attrLink();
  // Map<String, Object> gBes2Bes = sheets.be2BeTarget(gAttrLink, bes);
  // Map<String, Question> gQuestions = sheets.questionsData(sheets.attributesData(gDataTypes));
  // // final Map<String, Ask> gAsks = sheets.asksData(gQuestions, gBes);
  // final Map<String, Object> genny = new HashMap<String, Object>();
  // genny.put("validations", gValidations);
  // genny.put("dataType", gDataTypes);
  // genny.put("attributes", gAttrs);
  // genny.put("baseEntitys", gBes);
  // genny.put("attibutesEntity", gAttr2Bes);
  // genny.put("attributeLink", gAttrLink);
  // genny.put("basebase", gBes2Bes);
  // genny.put("questions", gQuestions);
  // // genny.put("ask", gAsks);
  // return genny;
  // }

  public static Map<String, Object> project(final String projectType, final File path) {
    GennySheets sheets = new GennySheets(secret, projectType, path);
    Map<String, Map> gValidations = sheets.newGetVal();
    Map<String, Map> gDataTypes = sheets.newGetDType();
    Map<String, Map> gAttrs = sheets.newGetAttr();
    Map<String, Map> gBes = sheets.newGetBase();
    Map<String, Map> gAttr2Bes = sheets.newGetEntAttr();
    Map<String, Map> gAttrLink = sheets.newGetAttrLink();
    Map<String, Map> gBes2Bes = sheets.newGetEntEnt();
    Map<String, Map> gQuestions = sheets.newGetQtn();
    Map<String, Map> gAsks = sheets.newGetAsk();
    final Map<String, Object> genny = new HashMap<String, Object>();
    genny.put("validations", gValidations);
    genny.put("dataType", gDataTypes);
    genny.put("attributes", gAttrs);
    genny.put("baseEntitys", gBes);
    genny.put("attibutesEntity", gAttr2Bes);
    genny.put("attributeLink", gAttrLink);
    genny.put("basebase", gBes2Bes);
    genny.put("questions", gQuestions);
    genny.put("ask", gAsks);
    return genny;
  }

  @SuppressWarnings({"unchecked", "unused"})
  public static Map<String, Object> modifyObj(Map<String, Object> superProject,
      Map<String, Object> subProject) {
    subProject.entrySet().stream().forEach(map -> {
      final Map<String, Object> objects = (Map<String, Object>) subProject.get(map.getKey());
      objects.entrySet().stream().forEach(obj -> {
        try {
          if (((Map<String, Object>) superProject.get(map.getKey()))
              .<HashMap<String, Object>>get(obj.getKey()) != null) {
            // out.println(obj.getValue());
            Map<String, Object> mapp = ((Map<String, Object>) obj.getValue());
            Map<String, Object> mapp2 = ((Map<String, HashMap>) superProject.get(map.getKey()))
                .<HashMap<String, Object>>get(obj.getKey());
            BeanNotNullFields.getInstance().copyProperties(mapp2, mapp);

            mapp.entrySet().stream().forEach(data -> {
              if (data.getValue() != null) {
                mapp2.put(data.getKey(), data.getValue());
              }
            });
          } else {
            ((Map<String, Object>) superProject.get(map.getKey()))
                .<HashMap<String, Object>>put(obj.getKey(), obj.getValue());
          }
        } catch (IllegalAccessException | InvocationTargetException e) {
          e.printStackTrace();
        }
      });
    });
    return superProject;
  }

}
