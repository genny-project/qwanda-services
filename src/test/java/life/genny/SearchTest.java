package life.genny;

import java.io.FileNotFoundException;
import java.lang.invoke.MethodHandles;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import life.genny.qwanda.attribute.AttributeDate;
import life.genny.qwanda.attribute.AttributeInteger;
import life.genny.qwanda.attribute.AttributeText;
import life.genny.qwanda.entity.BaseEntity;
import life.genny.qwanda.exception.BadDataException;
import life.genny.services.BaseEntityService2;
import life.genny.services.BatchLoading;

public class SearchTest {
	private static final Logger log = org.apache.logging.log4j.LogManager
			.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());

	protected static EntityManagerFactory emf;
	protected static EntityManager em;

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

	@Test
	public void searchBETest() {

		BaseEntity searchBE = new BaseEntity("SER_TEST_SEARCH", "Search test");
		try {
			// searchBE.setValue(new AttributeText("SCH_STAKEHOLDER_CODE",
			// "Stakeholder"),"PER_USER1");
			searchBE.setValue(new AttributeInteger("SCH_PAGE_START", "PageStart"), 0);
			searchBE.setValue(new AttributeInteger("SCH_PAGE_SIZE", "PageSize"), 10);
			
			// Set some Filter attributes
			// searchBE.setValue(new AttributeText("QRY_PRI_FIRST_NAME", "First
			// Name"),"Bob");

			searchBE.setValue(new AttributeDate("PRI_DOB", "DOB"), LocalDate.of(2018, 2, 20));

			searchBE.setValue(new AttributeText("SRT_PRI_DOB", "DOB"), "ASC", 0.8);
			searchBE.setValue(new AttributeText("SRT_PRI_FIRSTNAME", "FIRSTNAME"), "DESC", 1.0); // higher priority
																									// sorting

			searchBE.setValue(new AttributeText("PRI_FIRSTNAME", "First name"), null, 1.0); // return this
																										// column with
																										// this header
//			searchBE.setValue(new AttributeText("PRI_DOB", "DOB"), "Birthday", 2.0); // return this column with this
//																						// header
//			searchBE.setValue(new AttributeText("PRI_LASTNAME", "LastName"), "Last Name", 1.5); // return this column
//																								// with this header

		} catch (BadDataException e) {
			log.error("Bad Data Exception");
		}

		List<BaseEntity> results = service.findBySearchBE(searchBE);
		log.info(results);
	}
}
