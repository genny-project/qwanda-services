package life.genny;


import java.io.File;
import java.io.FileNotFoundException;
import java.lang.invoke.MethodHandles;
import java.sql.SQLException;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import life.genny.services.BaseEntityService2;
import life.genny.services.BatchLoading;

public class JPAHibernateTest {

  private static final Logger log = org.apache.logging.log4j.LogManager
      .getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());

  protected static EntityManagerFactory emf;
  protected static EntityManager em;

  private final static String secret = System.getenv("GOOGLE_CLIENT_SECRET");
  private final static String gennySheetId = System.getenv("GOOGLE_SHEETID");
  static File credentialPath = new File(System.getProperty("user.home"), ".credentials/genny");
  static File channelPath = new File(System.getProperty("user.home"), ".credentials/channel");

  protected static BaseEntityService2 service = null;
  BatchLoading bl;

  @BeforeClass
  public static void init() throws FileNotFoundException, SQLException {
    log.info("Setting up EntityManagerFactory");
    try {
      emf = Persistence.createEntityManagerFactory("h2-pu2");
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    if (emf == null) {
      log.error("EMF is null");
    } else {
      log.info("Setting up EntityManager");      
      em = emf.createEntityManager();
    }
  }

//   @Before
  public void initializeDatabase() {
    // final Session session = em.unwrap(Session.class);
    // session.doWork(new Work() {
    // @Override
    // public void execute(final Connection connection) throws SQLException {
    // try {
    // final File script = new File(getClass().getResource("/data.sql").getFile());
    // RunScript.execute(connection, new FileReader(script));
    // } catch (final FileNotFoundException e) {
    // throw new RuntimeException("could not initialize with script");
    // }
    // }
    // });
  }

  @Test
  public void import_from_google_docs() {
    em.getTransaction().begin();
    bl = new BatchLoading(em);
    bl.persistProject();
    em.getTransaction().commit();
  }

  // public static void import_from_google_docs2()
  // {
  // log.info("Setting up Import");
  // service = new BaseEntityService2(em);
  //
  // if (service == null) {
  // log.error("BaseEntityService is null");
  // }
  //
  // em.getTransaction().begin();
  //
  // System.out
  // .println("**************************** IMPORTING *************************************");
  //
  // final GennySheets gennySheets = new GennySheets(secret, gennySheetId, credentialPath);
  //
  //
  // System.out.println(
  // "**************************** IMPORTING VALIDATIONS *************************************");
  //
  // // Validations
  // final Map<String, Validation> validationMap = new HashMap<String, Validation>();
  //
  // List<Validation> validations = null;
  // try {
  // validations = gennySheets.getBeans(Validation.class);
  // } catch (final IOException e2) {
  // // TODO Auto-generated catch block
  // e2.printStackTrace();
  // }
  // validations.stream().forEach(object -> {
  // service.insert(object);
  // validationMap.put(object.getCode(), object);
  // });
  //
  // System.out.println(
  // "**************************** IMPORTING DATATYPES *************************************");
  //
  // // DataTypes
  // final Map<String, DataType> dataTypeMap = new HashMap<String, DataType>();
  // List<Map> obj = null;
  // try {
  // obj = gennySheets.row2DoubleTuples(DataType.class.getSimpleName());
  // } catch (final IOException e1) {
  // // TODO Auto-generated catch block
  // e1.printStackTrace();
  // }
  // obj.stream().forEach(object -> {
  // final String code = (String) object.get("code");
  // final String name = (String) object.get("name");
  // final String validationss = (String) object.get("validations");
  // final ValidationList validationList = new ValidationList();
  // validationList.setValidationList(new ArrayList<Validation>());
  // if (validationss != null) {
  // final String[] validationListStr = validationss.split(",");
  // for (final String validationCode : validationListStr) {
  // validationList.getValidationList().add(validationMap.get(validationCode));
  // }
  // }
  // if (!dataTypeMap.containsKey(code)) {
  // final DataType dataType = new DataType(name, validationList);
  // dataTypeMap.put(code, dataType);
  // }
  // });
  //
  // System.out.println(
  // "**************************** IMPORTING ATTRIBUTES *************************************");
  //
  // List<Map> attrs = null;
  // try {
  // attrs = gennySheets.row2DoubleTuples(Attribute.class.getSimpleName());
  // } catch (final IOException e2) {
  // // TODO Auto-generated catch block
  // e2.printStackTrace();
  // }
  // attrs.stream().forEach(data -> {
  // Attribute attribute = null;
  // final String datatype = (String) data.get("datatype");
  // try {
  // System.out.println("ATTRIBUTE:::Code:" + data.get("code") + ":name" + data.get("name")
  // + ":datatype:" + data.get("datatype"));
  // attribute = service.findAttributeByCode((String) data.get("code"));
  //
  // } catch (final NoResultException e) {
  // attribute = new Attribute((String) data.get("code"), (String) data.get("name"),
  // dataTypeMap.get(datatype));
  // // System.out.println("ATTRIBUTE:::Code:" + data.get("code") + ":name" + data.get("name")
  // // + ":datatype:" + data.get("datatype") + "##################" + attribute);
  // service.insert(attribute);
  // } catch (final OptimisticLockException ee) {
  // attribute = new Attribute((String) data.get("code"), (String) data.get("name"),
  // dataTypeMap.get(datatype));
  // // System.out.println("ATTRIBUTE:::Code:" + data.get("code") + ":name" + data.get("name")
  // // + ":datatype:" + data.get("datatype") + "##################" + attribute);
  // // service.insert(attribute);
  // }
  // });
  // System.out.println(
  // "**************************** IMPORTING BASEENTITYS *************************************");
  //
  // List<BaseEntity> bes = null;
  // try {
  // bes = gennySheets.getBeans(BaseEntity.class);
  // bes.stream().forEach(object -> {
  // service.insert(object);
  // });
  // } catch (final IOException e2) {
  // // TODO Auto-generated catch block
  // e2.printStackTrace();
  // }
  //
  //
  // if (true) {
  // System.out.println(
  // "**************************** IMPORTING BASEENTITY - ATTRIBUTE LINKS
  // *************************************");
  //
  // // Now link Attributes to Baseentitys
  // List<Map> obj2 = null;
  // try {
  // obj2 = gennySheets.row2DoubleTuples(EntityAttribute.class.getSimpleName());
  // } catch (final IOException e1) {
  // // TODO Auto-generated catch block
  // e1.printStackTrace();
  // }
  // System.out.println("Number of Links = " + obj2.size());
  //
  // obj2.stream().forEach(object -> {
  // final String beCode = (String) object.get("baseEntityCode");
  // final String attributeCode = (String) object.get("attributeCode");
  // final String weightStr = (String) object.get("weight");
  // final String valueString = (String) object.get("valueString");
  // System.out.println("BECode:" + beCode + ":attCode" + attributeCode + ":weight:" + weightStr
  // + ": valueString:" + valueString);
  // Attribute attribute = null;
  // BaseEntity be = null;
  // try {
  // attribute = service.findAttributeByCode(attributeCode);
  // be = service.findBaseEntityByCode(beCode);
  // System.out.println("In EA : found " + be.getCode() + "," + attribute.getCode());
  // final Double weight = Double.valueOf(weightStr);
  // try {
  // be.addAttribute(attribute, weight, valueString);
  //
  // } catch (final BadDataException e) {
  // // TODO Auto-generated catch block
  // e.printStackTrace();
  // }
  //
  // service.update(be);
  // } catch (final NoResultException e) {
  //
  // }
  // });
  // System.out.println(
  // "**************************** IMPORTING BASEENTITY - BASEENTITY LINKS
  // *************************************");
  //
  // // Now link be to be
  // final AttributeLink linkAttribute2 = new AttributeLink("LNK_CORE", "Parent");
  // service.insert(linkAttribute2);
  //
  // List<Map> obj3 = null;
  // try {
  // obj3 = gennySheets.row2DoubleTuples(EntityEntity.class.getSimpleName());
  // } catch (final IOException e1) {
  // // TODO Auto-generated catch block
  // e1.printStackTrace();
  // }
  // obj3.stream().forEach(object -> {
  // final String parentCode = (String) object.get("parentCode");
  // final String targetCode = (String) object.get("targetCode");
  // object.get("linkCode");
  // final String weightStr = (String) object.get("weight");
  // object.get("valueString");
  // BaseEntity sbe = null;
  // BaseEntity tbe = null;
  // try {
  // sbe = service.findBaseEntityByCode(parentCode);
  // tbe = service.findBaseEntityByCode(targetCode);
  // final Double weight = Double.valueOf(weightStr);
  // sbe.addTarget(tbe, linkAttribute2, weight);
  // service.update(tbe);
  // } catch (final NoResultException e) {
  //
  // } catch (final BadDataException e) {
  // // TODO Auto-generated catch block
  // e.printStackTrace();
  // }
  // });
  // }
  //
  // em.getTransaction().commit();
  // }



  // @After
  public void aftertest() {
    // log.info("AFTER: Clearing data");
    // org.h2.store.fs.FileUtils.deleteRecursive("mem:test", true);
    // // em.clear();
    // DeleteDbFiles.execute("mem", "test", true);
    // em.createNativeQuery("TRUNCATE SCHEMA PUBLIC RESTART IDENTITY AND COMMIT NO CHECK")
    // .executeUpdate();
  }

  @AfterClass
  public static void tearDown() {
    log.info("Starting Tear down");
    if (em != null) {
      org.h2.store.fs.FileUtils.deleteRecursive("mem:test", true);
      em.clear();
      em.close();
    } else {
      log.error("EntityManager not created, so not torn down...");
    }
    if (emf != null) {
      emf.close();
    } else {
      log.error("EntityManagerFactory was null");
    }
  }


}
