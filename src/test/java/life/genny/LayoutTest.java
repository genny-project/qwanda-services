package life.genny;


import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import javax.persistence.Query;
import javax.ws.rs.core.MultivaluedMap;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.InvalidRemoteException;
import org.eclipse.jgit.api.errors.TransportException;
import org.eclipse.jgit.errors.AmbiguousObjectException;
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.eclipse.jgit.errors.RevisionSyntaxException;
import org.javamoney.moneta.Money;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.junit.Test;
import org.keycloak.admin.client.resource.UserResource;
import org.keycloak.representations.idm.UserRepresentation;
import org.mortbay.log.Log;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import life.genny.qwanda.Answer;
import life.genny.qwanda.AnswerLink;
import life.genny.qwanda.Ask;
import life.genny.qwanda.CoreEntity;
import life.genny.qwanda.DateTimeDeserializer;
import life.genny.qwanda.Link;
import life.genny.qwanda.MoneyDeserializer;
import life.genny.qwanda.Question;
import life.genny.qwanda.attribute.Attribute;
import life.genny.qwanda.attribute.AttributeDate;
import life.genny.qwanda.attribute.AttributeDateTime;
import life.genny.qwanda.attribute.AttributeInteger;
import life.genny.qwanda.attribute.AttributeLink;
import life.genny.qwanda.attribute.AttributeText;
import life.genny.qwanda.attribute.EntityAttribute;
import life.genny.qwanda.entity.BaseEntity;
import life.genny.qwanda.entity.EntityEntity;
import life.genny.qwanda.entity.Person;
import life.genny.qwanda.entity.SearchEntity;
import life.genny.qwanda.exception.BadDataException;
import life.genny.qwanda.message.QDataAskMessage;
import life.genny.qwanda.message.QDataBaseEntityMessage;
import life.genny.qwanda.message.QDataSubLayoutMessage;
import life.genny.qwanda.message.QSearchEntityMessage;
import life.genny.qwandautils.GitUtils;
import life.genny.qwandautils.JsonUtils;
import life.genny.qwandautils.KeycloakService;
import life.genny.qwandautils.MergeUtil;
import life.genny.qwandautils.QwandaUtils;
import life.genny.services.BatchLoading;

public class LayoutTest extends LayoutHibernateTest {

  private static final Logger log = org.apache.logging.log4j.LogManager
      .getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());

	Attribute layoutDataAttribute = new AttributeText("PRI_LAYOUT_DATA","Layout Data");
	Attribute layoutURLAttribute = new AttributeText("PRI_LAYOUT_URL","Layout URL");
	Attribute layoutURIAttribute = new AttributeText("PRI_LAYOUT_URI","Layout URI");
	Attribute layoutNameAttribute = new AttributeText("PRI_LAYOUT_NAME","Layout Name");
	Attribute layoutModifiedDateAttribute = new AttributeDateTime("PRI_LAYOUT_MODIFIED_DATE","Layout  Modified DateTime");
	Attribute layoutVersionAttribute = new AttributeText("PRI_VERSION","Layout Version");
	Attribute layoutBranchAttribute = new AttributeText("PRI_BRANCH","Layout Branch");
	Attribute layoutGitRealmAttribute = new AttributeText("PRI_GITREALM","Layout Git Realm");;
	
	AttributeLink linkCoreAttribute = new AttributeLink("LNK_CORE","Link Core");

  
 //@Test
  public void layoutTest() throws RevisionSyntaxException, InvalidRemoteException, TransportException, AmbiguousObjectException, IncorrectObjectTypeException, BadDataException, GitAPIException, IOException {
	 getEm().getTransaction().begin();
	 


   log.info("Creating Layout Test");
   log.info("Created "+layoutDataAttribute);
   log.info("Created "+layoutURLAttribute);
   
   service.upsert(layoutDataAttribute);
   service.upsert(layoutURLAttribute);
   service.upsert(layoutURIAttribute);
   service.upsert(layoutNameAttribute);
   service.upsert(layoutModifiedDateAttribute);
   service.upsert(layoutVersionAttribute);
   service.upsert(layoutBranchAttribute);
   service.upsert(layoutGitRealmAttribute);
   service.upsert(linkCoreAttribute);
   
   BaseEntity grpLayouts = new BaseEntity("GRP_LAYOUTS","Layouts");
   service.upsert(grpLayouts);

   
   // Create some Layouts (as if they have come from git
   
   String gitUrl = "https://github.com/genny-project/layouts.git";
   String branch = "master";
   String realm = "internmatch";
   String gitrealm = "genny/sublayouts";
   
	List<BaseEntity> gennyLayouts = GitUtils.getLayoutBaseEntitys(gitUrl, branch, realm,gitrealm,true); // get common layouts		
	
	List<BaseEntity> bes4 = service.getAll();
	service.saveLayouts(gennyLayouts, "genny/sublayouts", "V1", branch);

	 getEm().getTransaction().commit();
	 
//	List<BaseEntity> bes = service.getAll();
//
//	Query q2 = getEm().createQuery(" from EntityAttribute ");
//	List<EntityAttribute> eas = q2.getResultList();
//
//	
//	Query q = getEm().createQuery("select distinct ea.pk.baseEntity from EntityAttribute ea  where  ea.pk.baseEntity.realm in ('genny')   and ea.pk.baseEntity.code LIKE 'LAY_%'  order by  ea.pk.baseEntity.created DESC");
//	List<BaseEntity> bes2 = q.getResultList();
//			
			
	QDataSubLayoutMessage v1genny = service.fetchSubLayoutsFromDb(realm, gitrealm, branch);

	log.info("v1genny = "+v1genny+" with "+v1genny.getItems().length+" items");
	
   



  }

 
  
  
}
