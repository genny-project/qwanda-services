package life.genny.services;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
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

  private BaseEntityService2 service;

  public static int id = 1;


  public BatchLoading(BaseEntityService2 service) {
    this.service = service;
  }

  private final String secret =
      "{\"installed\":{\"client_id\":\"260075856207-9d7a02ekmujr2bh7i53dro28n132iqhe.apps.googleusercontent.com\",\"project_id\":\"genny-sheets-181905\",\"auth_uri\":\"https://accounts.google.com/o/oauth2/auth\",\"token_uri\":\"https://accounts.google.com/o/oauth2/token\",\"auth_provider_x509_cert_url\":\"https://www.googleapis.com/oauth2/v1/certs\",\"client_secret\":\"vgXEFRgQvh3_t_e5Hj-eb6IX\",\"redirect_uris\":[\"urn:ietf:wg:oauth:2.0:oob\",\"http://localhost\"]}}";
  private final String hostingSheetId = "1OQ3IUdKTCCN-qgMahaNfc3KFOc_iN8l2BAVx7-KdA0A";
  // private final String secret = System.getenv("GOOGLE_CLIENT_SECRET");
  // private final String hostingSheetId = System.getenv("GOOGLE_HOSTING_SHEET_ID");
  File credentialPath =
      new File(System.getProperty("user.home"), ".genny/sheets.googleapis.com-java-quickstart");
  public GennySheets sheets = new GennySheets(secret, hostingSheetId, credentialPath);

  public static Map<String, Object> savedProjectData;

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
      String regex = null;

      regex = ((String) validations.get("regex"));
      if (regex != null) {
        regex = regex.replaceAll("^\"|\"$", "");
      }
      String code = ((String) validations.get("code")).replaceAll("^\"|\"$", "");;
      if ("VLD_AU_DRIVER_LICENCE_NO".equalsIgnoreCase(code)) {
        System.out.println("detected VLD_AU_DRIVER_LICENCE_NO");
      }
      String name = ((String) validations.get("name")).replaceAll("^\"|\"$", "");;
      String recursiveStr = ((String) validations.get("recursive"));
      String multiAllowedStr = ((String) validations.get("multi_allowed"));
      String groupCodesStr = ((String) validations.get("group_codes"));
      Boolean recursive = getBooleanFromString(recursiveStr);
      Boolean multiAllowed = getBooleanFromString(multiAllowedStr);

      Validation val = null;

      if (code.startsWith(Validation.getDefaultCodePrefix() + "SELECT_")) {
        val = new Validation(code, name, groupCodesStr, recursive, multiAllowed);
      } else {
        val = new Validation(code, name, regex);

      }
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
    if (project.get("attributes") == null)
      return;
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
        String privacyStr = ((String) attributes.get("privacy"));
        if (privacyStr != null) {
          privacyStr = privacyStr.toUpperCase();
        }
        Boolean privacy = "TRUE".equalsIgnoreCase(privacyStr);
        if (privacy) {
          System.out.println("Attribute " + code + " has default privacy");
        }
        String descriptionStr = ((String) attributes.get("description"));
        String helpStr = ((String) attributes.get("help"));
        String placeholderStr = ((String) attributes.get("placeholder"));
        String defaultValueStr = ((String) attributes.get("defaultValue"));
        Attribute attr = new Attribute(code, name, dataTypeRecord);
        attr.setDefaultPrivacyFlag(privacy);
        attr.setDescription(descriptionStr);
        attr.setHelp(helpStr);
        attr.setPlaceholder(placeholderStr);
        attr.setDefaultValue(defaultValueStr);
        Set<ConstraintViolation<Attribute>> constraints = validator.validate(attr);
        for (ConstraintViolation<Attribute> constraint : constraints) {
          System.out.println(constraint.getPropertyPath() + " " + constraint.getMessage());
        }
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
      String inputmask = ((String) dataType.get("inputmask"));
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
    if (project.get("baseEntitys") == null)
      return;
    ValidatorFactory factory = javax.validation.Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();

    ((HashMap<String, HashMap>) project.get("baseEntitys")).entrySet().stream().forEach(data -> {
      Map<String, Object> baseEntitys = data.getValue();
      String code = ((String) baseEntitys.get("code")).replaceAll("^\"|\"$", "");;
      String name = ((String) baseEntitys.get("name")).replaceAll("^\"|\"$", "");;
      BaseEntity be = new BaseEntity(code, name);

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
    if (project.get("attibutesEntity") == null)
      return;
    ((HashMap<String, HashMap>) project.get("attibutesEntity")).entrySet().stream()
        .forEach(data -> {
          Map<String, Object> baseEntityAttr = data.getValue();
          String attributeCode = null;
          try {
            attributeCode =
                ((String) baseEntityAttr.get("attributeCode")).replaceAll("^\"|\"$", "");;
          } catch (Exception e2) {
            log.error("AttributeCode not found [" + baseEntityAttr + "]");
          }
          String valueString = ((String) baseEntityAttr.get("valueString"));
          if (valueString != null) {
            valueString = valueString.replaceAll("^\"|\"$", "");;
          }
          String baseEntityCode = null;

          try {
            baseEntityCode =
                ((String) baseEntityAttr.get("baseEntityCode")).replaceAll("^\"|\"$", "");;
            String weight = (String) baseEntityAttr.get("weight");
            String privacyStr = (String) baseEntityAttr.get("privacy");
            Boolean privacy = "TRUE".equalsIgnoreCase(privacyStr);
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
                EntityAttribute ea = be.addAttribute(attribute, weightField, valueString);
                if (privacy || attribute.getDefaultPrivacyFlag()) {
                  ea.setPrivacyFlag(true);
                }
              } catch (final BadDataException e) {
                e.printStackTrace();
              }
              service.updateWithAttributes(be);
            } catch (final NoResultException e) {
            }
          } catch (Exception e1) {
            String beCode = "BAD BE CODE";
            if (baseEntityAttr != null) {
              beCode = (String) baseEntityAttr.get("baseEntityCode");
            }
            log.error("Error in getting baseEntityAttr  for AttributeCode " + attributeCode
                + " and beCode=" + beCode);
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
        service.updateWithAttributes(sbe);
      } catch (final NoResultException e) {
        log.warn(
            "CODE NOT PRESENT IN LINKING: " + parentCode + ":" + targetCode + ":" + linkAttribute);
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
    if (project.get("questionQuestions") == null)
      return;
    ((HashMap<String, HashMap>) project.get("questionQuestions")).entrySet().stream()
        .forEach(data -> {
          Map<String, Object> queQues = data.getValue();
          String parentCode = ((String) queQues.get("parentCode"));
          String targetCode = ((String) queQues.get("targetCode"));
          String weightStr = ((String) queQues.get("weight"));
          String mandatoryStr = ((String) queQues.get("mandatory"));


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
              String oneshotStr = (String) queQues.get("oneshot");
              Boolean oneshot = false;
              if (oneshotStr == null) {
                // Set the oneshot to be that of the targetquestion
                oneshot = tbe.getOneshot();
              } else {
                oneshot = ("TRUE".equalsIgnoreCase(oneshotStr));
              }

              QuestionQuestion qq = sbe.addChildQuestion(tbe.getCode(), weight, mandatory);
              qq.setOneshot(oneshot);
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
                qq = service.upsert(existing);
              }

            } catch (NullPointerException e) {
              log.error("Cannot find QuestionQuestion targetCode:" + targetCode + ":parentCode:"
                  + parentCode);


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
  public void attributeLinks(Map<String, Object> project, Map<String, DataType> dataTypeMap) {
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
        String privacyStr = ((String) attributeLink.get("privacy"));
        Boolean privacy = "TRUE".equalsIgnoreCase(privacyStr);

        linkAttribute = new AttributeLink(code, name);
        linkAttribute.setDefaultPrivacyFlag(privacy);
        linkAttribute.setDataType(dataTypeRecord);
        service.upsert(linkAttribute);
      } catch (Exception e) {
        String name = ((String) attributeLink.get("name")).replaceAll("^\"|\"$", "");;
        String privacyStr = ((String) attributeLink.get("privacy"));
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
      Boolean oneshot = oneshotStr == null ? false : ("TRUE".equalsIgnoreCase(oneshotStr));
      Attribute attr;
      attr = service.findAttributeByCode(attrCode);
      Question q = new Question(code, name, attr);
      q.setOneshot(oneshot);
      q.setHtml(html);
      Question existing = service.findQuestionByCode(code);
      if (existing == null) {
        service.insert(q);
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
      String mandatoryStr = ((String) asks.get("mandatory"));
      final Double weight = Double.valueOf(weightStr);
      if ("QUE_USER_SELECT_ROLE".equals(targetCode)) {
        System.out.println("dummy");
      }
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


  public static void filterRawsSpreadSheet(String deployCode, HashMap<String, Object> records) {
    // records.entrySet().stream().allMatch(predicate)
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
        System.out.println("23434 =" + projects.size());
        if (subsequentIndex == projects.size())
          break;
        if (lastProject == null) {
          // System.out.println("234SDFSD34");
          lastProject = upsertProjectMapProps(projects.get(count), projects.get(subsequentIndex));
          System.out.println("23434");
        } else {
          // System.out.println("23wDSFSDFDSFSDFs4");
          lastProject = upsertProjectMapProps(lastProject, projects.get(subsequentIndex));
          System.out.println("23ws4");
        }
      }
    }
    return lastProject;
  }

  // public static void main(String...strings) {
  // Stream<Integer> streamIterated = Stream.iterate(40, n -> n + 2).limit(20);
  // IntStream intStream = IntStream.range(1, 3);
  // LongStream longStream = LongStream.rangeClosed(1, 3);
  // IntStream streamOfChars = "abc".chars();
  // Stream<String> streamOfString =
  // Pattern.compile(", ").splitAsStream("a, b, c");
  // streamOfString.forEach(System.out::println);
  //
  // // Stream<String> stream =
  // // Stream.of("a", "b", "c").filter(element -> element.contains("b"));
  // // Optional<String> anyElement = stream.findAny();
  //
  // List<String> elements =
  // Stream.of("a", "b", "c").filter(element -> element.contains("b"))
  // .collect(Collectors.toList());
  // Optional<String> anyElement = elements.stream().findAny();
  // Optional<String> firstElement = elements.stream().findFirst();
  //
  // Stream<String> onceModifiedStream =
  // Stream.of("abcd", "bbcd", "cbcd").skip(1);
  // }

  // public static void main(String... strings) {
  // BatchLoading bl = new BatchLoading(null);
  // Map<String, Object> tables = bl.getProject();
  //
  // System.out.println();
  // System.out.println();
  // System.out.println();
  // System.out.println();
  //
  // /* Extract all tables from the spreadsheets */
  // Map<String, Object> project = tables.entrySet().stream().map(table -> {
  // // System.out.println(tables);
  // //System.out.println("------------table--------------" + table.getKey());
  // Map<String, Object> records = (HashMap) table.getValue();
  // /* Extracts all rows from the table */
  // Map<String, Object> recordsFiltered = records.entrySet().stream().map(record -> {
  // //System.out.println("------------record--------------" + record.getKey());
  // /* Extracts data from the rows */
  // Map<String, Object> fields = (HashMap) record.getValue();
  // Map<String, Object> recordFiltered = fields.entrySet().stream().filter(field ->{
  // String key = field.getKey();
  // String val = (String)field.getValue();
  // return (key.equals("deploy_code") && val.equals("") && !val.equals(DEPLOY_CODE));
  // }).map(pair ->{
  // Map<String, Object> mapPair = new HashMap <String, Object>();
  // mapPair.put(pair.getKey(), pair.getValue());
  // return mapPair;
  // }).reduce((first, second) ->{
  // first.putAll(second);
  // return first;
  // }).get();
  // Map<String, Object> superPair = new HashMap <String, Object>();
  // superPair.put(record.getKey(), recordFiltered);
  // return superPair;
  // }).reduce((first, second) ->{
  // first.putAll(second);
  // return first;
  // }).get();;
  // Map<String,Object> newTables = new HashMap<String, Object>();
  // newTables.put(table.getKey(), recordsFiltered);
  // return newTables;
  // }).reduce((first, second) ->{
  // first.putAll(second);
  // return first;
  // }).get();
  //
  // System.out.println(project);
  // System.out.println();
  // System.out.println((project.equals(tables)));
  // System.out.println();
  // System.out.println((tables));
  //
  // // field.getKey().equals("deploy_code") && !field.getValue().equals("")
  // // && !field.getValue().equals(DEPLOY_CODE);
  //
  // // data.stream().map(pair -> {
  // // HashMap<String, Object> subPair = (HashMap) pair.get("baseEntitys");
  // // subPair.keySet().forEach(act -> {
  // // HashMap<String, Object> subData = (HashMap) subPair.get(act);
  // // if (subData.get("deploy_code") == "")
  // // System.out.println("false");
  // // else
  // // subPair.remove(act);
  // // });
  // // //System.out.println(subPair+"-----");
  // // return subPair;
  // // }).reduce((one,two) ->{
  // // System.out.println(one+"---");
  // // System.out.println(two+"+++");
  // // return null;
  // // }).get();//;;
  // // }).reduce((one,two) ->{
  // // return new HashMap<String,Object>().put(kk, value)
  // // });
  //
  // // data.stream().forEach(System.out::println);
  // }

  final static String DEPLOY_CODE = null;// System.getenv("DEPLOY_CODE") ;

  public static Optional<String> getEnvDeployCode() {
    return Optional.ofNullable(DEPLOY_CODE);
  }

  public static Stream<String> getDeployCode() {
    return Stream
        .of(getEnvDeployCode().map(code -> code.split("\\s*(,|\\s)\\s*")).orElse(new String[] {}));
  }

  // Match any String from a sequence
  public static boolean matchStringFromSequence(String seq1, String seq2) {
    String splitRex = "\\s*(,|\\s)\\s*";
    String[] arrStr = seq1.split(splitRex);

    String patternRex =
        "^$|" + getDeployCode().peek(System.out::println).map(data -> ".*(\\b" + data + "\\b).*")
            .reduce((first, second) -> first + "|" + second).orElse("");

    Pattern p = Pattern.compile(patternRex);// . represents single
    Matcher m = p.matcher(seq2.trim());
    return m.matches();
  }

  static Function<Entry<String, Object>, HashMap<String, Object>> recordsFil = table -> {
    Object recordsFiltered = parseToHashMap(table.getValue()).entrySet().stream()
        .filter(data -> true).map(rec -> new HashMap<String, Object>() {
          {
            put(rec.getKey().toString(), rec.getValue());
          }
        })
        .reduce((first, second) -> {
          first.putAll(second);
          return first;
        })
        .orElse(new HashMap<String, Object>());
    return new HashMap<String, Object>() {
      {
        put(table.getKey(), recordsFiltered);
      }
    };
  };

  public static void main(String... strings) {
    BatchLoading bl = new BatchLoading(null);
    Map<String, Object> tables = bl.getProject();
    Map<String, Object> tabl =
        tables.entrySet().stream().map(recordsFil::apply).reduce((firstMap, secondMap) -> {
          firstMap.putAll(secondMap);
          return firstMap;
        }).get();

    System.out.println(tables.equals(tabl));
    tabl.entrySet().stream().forEach(System.out::println);
    // String str = "Byron, Andres, Aguirre".trim().toLowerCase();
    //
    // String[] arrStr = getDeployCode().get().split("\\s*(,|\\s)\\s*");
    //
    // System.out.println(arrStr.length);
    //
    // String st = "^$|" + Arrays.asList(arrStr).stream().map(data -> ".*(\\b" + data + "\\b).*")
    // .reduce((first, second) -> first + "|" + second).get();
    //
    // Pattern p = Pattern.compile(st);// . represents single
    // System.out.println("Stream from string: " + st);
    // // character
    // Matcher m = p.matcher("byron ".trim());
    // boolean b = m.matches();
    // System.out.println(b);
  }

  /**
   * @param strings
   */
  // final static String DEPLOY_CODE = System.getenv("DEPLOY_CODE");

  // 25
  public static void filterProject() {
    BatchLoading bl = new BatchLoading(null);
    Map<String, Object> tables = bl.getProject();
    Map<String, Object> tablesFiltered = tables.entrySet().stream().map(table -> {
      HashMap<String, Object> records = (HashMap<String, Object>) table.getValue();
      HashMap<String, Object> recordsFiltered =
          (HashMap<String, Object>) records.entrySet().stream().filter(record -> {
            HashMap<String, Object> fields = (HashMap<String, Object>) record.getValue();
            return fields.entrySet().stream().allMatch(field -> {
              String key = field.getKey();
              String val = (String) field.getValue();
              if (key.equals("deploy_code"))
                return (val.equals("") || val.equals(DEPLOY_CODE)) ? true : false;
              else
                return true;
            });
          }).map(recordFiltered -> {
            return new HashMap<String, Object>() {
              {
                put(recordFiltered.getKey(), recordFiltered.getValue());
              }
            };
          }).reduce((first, second) -> {
            first.putAll(second);
            return first;
          }).get();
      return new HashMap<String, Object>() {
        {
          put(table.getKey(), recordsFiltered);
        }
      };
    }).reduce((firstMap, secondMap) ->

    {
      firstMap.putAll(secondMap);
      return firstMap;
    }).get();
  }

  static BiPredicate<? super Entry<String, Object>, String> isRecordAllowed =
      (field, columnKey) -> {
        String key = field.getKey();
        String val = (String) field.getValue();
        if (key.equals(columnKey))
          return (val.equals("") || val.equals(DEPLOY_CODE)) ? true : false;
        else
          return true;
      };

  // public void function1(){}
  // map.entrySet().Stream().map(fuction2)
  //
  //// function2 = table -> {
  // HashMap<String, Object> records = (HashMap<String, Object>) table.getValue();
  // Stream<HashMap<String, Object>> recordsFiltered =
  // records.entrySet().stream().filter(record -> {
  // HashMap<String, Object> fields = (HashMap<String, Object>) record.getValue();
  // boolean isAllsatisfied = fields.entrySet().stream()
  // .allMatch(field -> isRecordAllowed.test(field, columnKey));
  // return isAllsatisfied;
  // }).map(recordFiltered -> {
  // return new HashMap<String, Object>() {
  // {
  // put(recordFiltered.getKey(), recordFiltered.getValue());
  // }
  // };
  // });
  // HashMap<String, Object> recordsReduced = recordsFiltered.reduce((first, second) -> {
  // first.putAll(second);
  // return first;
  // }).get();
  // return new HashMap<String, Object>() {
  // {
  // put(table.getKey(), recordsReduced);
  // }
  // };
  // }
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //


  Function<HashMap<String, Object>, Stream> getTable = table -> {
    HashMap<String, Object> records = (HashMap<String, Object>) table;
    Stream<HashMap<String, Object>> recordsFiltered = records.entrySet().stream().filter(record -> {
      HashMap<String, Object> fields = (HashMap<String, Object>) record.getValue();
      boolean isAllsatisfied =
          fields.entrySet().stream().allMatch(field -> isRecordAllowed.test(field, null));
      return isAllsatisfied;
    }).map(recordFiltered -> {
      return new HashMap<String, Object>() {
        {
          put(recordFiltered.getKey(), recordFiltered.getValue());
        }
      };
    });
    return null;
  };

  public static void extractFields(Stream<Object> map) {
    Stream<Object> m = parseToHashMap(map).entrySet().stream().map(data -> data.getValue());
  }

  static Function<Object, Stream<Object>> reducing = table -> {

    return Stream.of(table);
  };

  // public static void extractFields(HashMap<String, Object> map) {
  // BatchLoading bl = new BatchLoading(null);
  // bl.getProject().entrySet().parallelStream().map(mapper);
  // }
  public static HashMap<String, Object> parseToHashMap(Object obj) {
    return (HashMap<String, Object>) obj;
  }

  // table -> {
  // HashMap<String, Object> records = parseToHashMap(table.getValue());
  // Stream<HashMap<String, Object>> recordsFiltered =
  // records.entrySet().stream().filter(record -> {
  // HashMap<String, Object> fields = parseToHashMap(record.getValue());
  // boolean isAllsatisfied = fields.entrySet().stream()
  // .allMatch(field -> isRecordAllowed.test(field, columnKey));
  // return isAllsatisfied;
  // }).map(recordFiltered -> {
  // return new HashMap<String, Object>() {
  // {
  // put(recordFiltered.getKey(), recordFiltered.getValue());
  // }
  // };
  // });
  // HashMap<String, Object> recordsReduced = recordsFiltered.reduce((first, second) -> {
  // first.putAll(second);
  // return first;
  // }).get();
  // return new HashMap<String, Object>() {
  // {
  // put(table.getKey(), recordsReduced);
  // }
  // };
  // }
  // public static void filterProject(String columnKey) {
  // BatchLoading bl = new BatchLoading(null);
  // Map<String, Object> tables = bl.getProject();
  // ltered = tables.values().stream().map(reducing::apply).reduce();
  // tablesFiltered.reduce((firstMap, secondMap) -> {
  // firstMap.putAll(secondMap);
  // return firstMap;
  // }).get();
  // }
  /**
   * Call functions named after the classes
   */
  public void persistProject() {
    System.out.println("Persisting Project in BatchLoading");
    Map<String, Object> lastProject = getProject();
    savedProjectData = lastProject;
    System.out.println("+++++++++ About to load Questions +++++++++++++");
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
  static int count = 0;

  @SuppressWarnings({"unchecked", "unused"})
  public Map<String, Object> upsertProjectMapProps(Map<String, Object> superProject,
      Map<String, Object> subProject) {
    // superProject.entrySet().stream().forEach(map -> {
    // final Map<String, Object> objects = (Map<String, Object>) superProject.get(map.getKey());
    // if (superProject.get(map.getKey()) == null) {
    // System.out.println("\n\n\n"+map.getKey()+"\n\n\n");
    // superProject.put(map.getKey(), subProject.get(map.getKey()));
    // }
    // });
    // subProject.entrySet().stream().forEach(map -> {
    // final Map<String, Object> objects = (Map<String, Object>) subProject.get(map.getKey());
    // if (subProject.get(map.getKey()) == null) {
    // subProject.put(map.getKey(), superProject.get(map.getKey()));
    // }
    // });
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
      if (objects != null)
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

  public void messageTemplates(Map<String, Object> project) {

    if (project.get("messages") == null) {
      System.out.println("project.get(messages) is null");
      return;
    }

    ((HashMap<String, HashMap>) project.get("messages")).entrySet().stream().forEach(data -> {

      System.out.println("messages, data ::" + data);
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

      if (StringUtils.isBlank(name)) {
        log.error("Empty Name");
      } else {
        try {
          QBaseMSGMessageTemplate msg = service.findTemplateByCode(code);
          try {
            if (msg != null) {
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
          Long id = service.insert(templateObj);
          System.out.println("message id ::" + id);
        } catch (Exception e) {
          log.error("Cannot add MessageTemplate");

        }
        // try {
        // QBaseMSGMessageTemplate msg = service.findTemplateByCode(code);
        // try {
        // service.update(templateObj);
        // } catch (Exception e) {
        // log.error("Cannot update QDataMSGMessage "+code);
        // }
        // } catch (Exception e) {
        // Long id = service.insert(templateObj);
        // System.out.println("id::" + id + " Code:" + code + " :" + subject);
        // }
      }
    });
  }


  private Boolean getBooleanFromString(final String booleanString) {
    if (booleanString == null) {
      return false;
    }

    if (("TRUE".equalsIgnoreCase(booleanString.toUpperCase()))
        || ("YES".equalsIgnoreCase(booleanString.toUpperCase()))
        || ("T".equalsIgnoreCase(booleanString.toUpperCase()))
        || ("Y".equalsIgnoreCase(booleanString.toUpperCase()))
        || ("1".equalsIgnoreCase(booleanString))) {
      return true;
    }
    return false;

  }


}
