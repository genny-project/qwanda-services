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

//  private final static String secret = System.getenv("GOOGLE_CLIENT_SECRET");
//  private final static String gennySheetId = System.getenv("GOOGLE_SHEETID");
//  static File credentialPath = new File(System.getProperty("user.home"), ".credentials/genny");
//  static File channelPath = new File(System.getProperty("user.home"), ".credentials/channel");

  protected static BaseEntityService2 service = null;
  static BatchLoading bl;

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
      service = new BaseEntityService2(em);
      import_from_google_docs();
    }
  }


  public static void import_from_google_docs() {
    em.getTransaction().begin();
    bl = new BatchLoading(service);
    bl.persistProject(false,null);
    em.getTransaction().commit();
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


/**
 * @return the em
 */
public static EntityManager getEm() {
	return em;
}


/**
 * @param em the em to set
 */
public static void setEm(EntityManager em) {
	JPAHibernateTest.em = em;
}

  

}
