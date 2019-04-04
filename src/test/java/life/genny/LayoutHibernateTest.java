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

public class LayoutHibernateTest {

  private static final Logger log = org.apache.logging.log4j.LogManager
      .getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());

  protected static EntityManagerFactory emf;
  protected static EntityManager em;


  protected static BaseEntityService2 service = null;


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

    }
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
	LayoutHibernateTest.em = em;
}

  

}
