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
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.persistence.Query;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.logging.log4j.Logger;
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
import life.genny.qwanda.attribute.AttributeLink;
import life.genny.qwanda.attribute.AttributeText;
import life.genny.qwanda.attribute.EntityAttribute;
import life.genny.qwanda.entity.BaseEntity;
import life.genny.qwanda.entity.EntityEntity;
import life.genny.qwanda.entity.Person;
import life.genny.qwanda.exception.BadDataException;
import life.genny.qwanda.message.QBaseMSGMessageTemplate;
import life.genny.qwanda.message.QDataAnswerMessage;
import life.genny.qwanda.message.QDataAskMessage;
import life.genny.qwanda.message.QDataBaseEntityMessage;
import life.genny.qwandautils.KeycloakService;
import life.genny.qwandautils.MergeUtil;
import life.genny.qwandautils.QwandaUtils;

public class JPAHibernateCRUDTest extends JPAHibernateTest {

  private static final Logger log = org.apache.logging.log4j.LogManager
      .getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());

  @Test
  public void saveAnswerTest() {
	 getEm().getTransaction().begin();

    final Gson gson = new GsonBuilder().registerTypeAdapter(Money.class, new MoneyDeserializer())
        .registerTypeAdapter(LocalDateTime.class, new JsonDeserializer<LocalDateTime>() {
          @Override
          public LocalDateTime deserialize(final JsonElement json, final Type type,
              final JsonDeserializationContext jsonDeserializationContext)
              throws JsonParseException {
            return ZonedDateTime.parse(json.getAsJsonPrimitive().getAsString()).toLocalDateTime();
          }

          public JsonElement serialize(final LocalDateTime date, final Type typeOfSrc,
              final JsonSerializationContext context) {
            return new JsonPrimitive(date.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)); // "yyyy-mm-dd"
          }
        }).create();


    String json = "{ " + "\"created\": \"2014-11-01T12:34:56+10:00\"," + "\"value\": \"Bob\","
        + "\"expired\": false," + "\"refused\": false," + "\"weight\": 1," + "\"version\": 1,"
        + "\"targetCode\": \"PER_USER1\"," + "\"sourceCode\": \"PER_USER1\","
        + "\"attributeCode\": \"PRI_FIRSTNAME\"" + "}";

    final Answer answer = gson.fromJson(json, Answer.class);
    log.info("Answer loaded :" + answer);
    final Long answerId = service.insert(answer);

    log.info("answerId=" + answerId);

    json = "{ " + "\"created\": \"2014-11-01T12:34:57+10:00\"," + "\"value\": \"Console\","
        + "\"expired\": false," + "\"refused\": false," + "\"weight\": 1," + "\"version\": 1,"
        + "\"targetCode\": \"PER_USER1\"," + "\"sourceCode\": \"PER_USER1\","
        + "\"attributeCode\": \"PRI_LASTNAME\"" + "}";

    // final Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss").create();


    final Answer answer2 = gson.fromJson(json, Answer.class);
    log.info("Answer2 loaded :" + answer2);
    final Long answerId2 = service.insert(answer2);

    log.info("answerId2=" + answerId2);

    getEm().getTransaction().commit();

    final List<AnswerLink> answers = service.findAnswerLinks();
    log.info(answers);

  }

  @Test
  public void fetchAttribute() {
    final Attribute at = service.findAttributeByCode("PRI_FIRSTNAME");
    log.info(at);
  }

  @Test
  public void fetchBE() {
    final BaseEntity be = service.findBaseEntityByCode("PER_USER1");
    log.info(be);
  }

  @Test
  public void countBE() {
    final Long count =
        (Long) em.createQuery("SELECT count(be) FROM BaseEntity be where  be.code=:sourceCode")
            .setParameter("sourceCode", "PER_USER1").getSingleResult();
    assertThat(count, is(1L));
  }

  // @Test
  public void sqlCountTest() {
    final String sql =
        "SELECT count(distinct be) FROM BaseEntity be,EntityEntity ee where ee.pk.targetCode=be.code and ee.pk.attribute.code=:linkAttributeCode and ee.pk.source.code=:sourceCode";

    final Long count = (Long) em.createQuery(sql).setParameter("sourceCode", "GRP_USERS")
        .setParameter("linkAttributeCode", "LNK_CORE").getSingleResult();
    log.info("Number of users = " + count);

    assertThat(count, is(3L));
  }

  // @Test
  public void sqlBETest() {
    final String sql =
        "SELECT be FROM BaseEntity be,EntityEntity ee where ee.pk.targetCode=be.code and ee.pk.attribute.code=:linkAttributeCode and ee.pk.source.code=:sourceCode";


    final List<BaseEntity> eeResults = em.createQuery(sql).setParameter("sourceCode", "GRP_USERS")
        .setParameter("linkAttributeCode", "LNK_CORE").setFirstResult(0).setMaxResults(1000)
        .getResultList();

    log.info("Number of users = " + eeResults.size());

    assertThat(eeResults.size(), is(3));
  }



  //@Test
  public void sqlBEandAttributesTest() {

    final String sql =
        "SELECT distinct be FROM BaseEntity be,EntityEntity ee JOIN be.baseEntityAttributes bea where ee.pk.targetCode=be.code and ee.pk.attribute.code=:linkAttributeCode and ee.pk.source.code=:sourceCode";

    if (em == null) {
      log.error("EntityManager is NULL!");
    }
    final List<BaseEntity> eeResults = em.createQuery(sql).setParameter("sourceCode", "GRP_USERS")
        .setParameter("linkAttributeCode", "LNK_CORE").setFirstResult(0).setMaxResults(1000)
        .getResultList();

    log.info("Number of users = " + eeResults.size());

    assertThat(eeResults.size(), is(3));
    assertThat(eeResults.get(0).getBaseEntityAttributes().size(), greaterThan(5));
  }
  
  //@Test
  public void testRemoveAttributeValue()
  {
	  getEm().getTransaction().begin();

		Answer answer = new Answer("PER_USER1","PER_USER","PRI_TEST","TEST_USERNAME");
		service.insert(answer);

	  List<EntityAttribute> eas = service.findAttributesByBaseEntityCode("PER_USER1");
	  boolean ok = false;
	  for (EntityAttribute ea : eas) {
		  if (ea.getAttributeCode().equalsIgnoreCase("PRI_TEST")) {
			  ok = true;
			  break;
		  }
	  }
	  assertTrue(ok);
	  
	  service.removeEntityAttribute("PER_USER1","PRI_TEST");
	    getEm().getTransaction().commit();
	  
	  eas = service.findAttributesByBaseEntityCode("PER_USER1");
	  ok = false;
	  for (EntityAttribute ea : eas) {
		  if (ea.getAttributeCode().equalsIgnoreCase("PRI_TEST")) {
			  ok = true;
			  break;
		  }
	  }
	  assertTrue(!ok);	  
	  
  }

  // @Test
  public void test_Query() {
    final String sourceCode = "GRP_USERS";
    final String linkCode = "LNK_CORE";

    Integer pageStart = 0;
    Integer pageSize = 10; // default
    Integer level = 1;

    final MultivaluedMap params = new MultivaluedMapImpl();
    params.add("pageStart", "0");
    params.add("pageSize", "2");

    params.add("PRI_USERNAME", "user1");

    final String pageStartStr = (String) params.getFirst("pageStart");
    final String pageSizeStr = (String) params.getFirst("pageSize");
    final String levelStr = (String) params.getFirst("level");
    if (pageStartStr != null && pageSizeStr != null) {
      pageStart = Integer.decode(pageStartStr);
      pageSize = Integer.decode(pageSizeStr);
    }
    if (levelStr != null) {
      level = Integer.decode(levelStr);
    }
    final List<BaseEntity> targets = service.findChildrenByAttributeLink(sourceCode, linkCode,
        false, pageStart, pageSize, level, params);

    BaseEntity[] beArr = new BaseEntity[targets.size()];
    assertThat(beArr.length, is(2));
    // assertThat(beArr[0].getBaseEntityAttributes().size(), is(7));
    beArr = targets.toArray(beArr);
    new QDataBaseEntityMessage(beArr, sourceCode, linkCode);
  }


  // @Test
  public void getBesWithAttributesPaged() {
    Integer pageStart = 0;
    Integer pageSize = 10; // default
    final MultivaluedMap<String, String> params = new MultivaluedMapImpl<String, String>();
    params.add("pageStart", "0");
    params.add("pageSize", "2");

    params.add("PRI_USERNAME", "user1");
    params.add("PRI_USERNAME", "user2");

    final String pageStartStr = params.getFirst("pageStart");
    final String pageSizeStr = params.getFirst("pageSize");
    final String levelStr = params.getFirst("level");
    if (pageStartStr != null && pageSizeStr != null) {
      pageStart = Integer.decode(pageStartStr);
      pageSize = Integer.decode(pageSizeStr);
    }
    if (levelStr != null) {
      Integer.decode(levelStr);
    }


    final List<BaseEntity> eeResults;
    new HashMap<String, BaseEntity>();


    Log.info("**************** BE SEARCH WITH ATTRIBUTE VALUE WITH ATTRIBUTES!! pageStart = "
        + pageStart + " pageSize=" + pageSize + " ****************");

    // ugly and insecure
    final Integer pairCount = params.size();
    if (pairCount.equals(0)) {
      eeResults =
          em.createQuery("SELECT distinct be FROM BaseEntity be JOIN be.baseEntityAttributes bee")
              .setFirstResult(pageStart).setMaxResults(pageSize).getResultList();
    } else {
      String queryStr =
          "SELECT distinct be FROM BaseEntity be JOIN be.baseEntityAttributes bee where  ";
      int attributeCodeIndex = 0;
      int valueIndex = 0;
      final List<String> attributeCodeList = new ArrayList<String>();
      final List<String> valueList = new ArrayList<String>();

      for (final Map.Entry<String, List<String>> entry : params.entrySet()) {
        if (entry.getKey().equals("pageStart") || entry.getKey().equals("pageSize")) { // ugly
          continue;
        }
        final List<String> qvalueList = entry.getValue();
        if (!qvalueList.isEmpty()) {
          // create the value or
          String valueQuery = "(";
          for (final String value : qvalueList) {
            valueQuery += "bee.valueString=:valueString" + valueIndex + " or ";
            valueList.add(valueIndex, value);
            valueIndex++;
          }
          // remove last or
          valueQuery = valueQuery.substring(0, valueQuery.length() - 4);
          valueQuery += ")";
          attributeCodeList.add(attributeCodeIndex, entry.getKey());
          if (attributeCodeIndex > 0) {
            queryStr += " and ";
          }
          queryStr +=
              " bee.attributeCode=:attributeCode" + attributeCodeIndex + " and " + valueQuery;
          System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
        }
        attributeCodeIndex++;

      }
      System.out.println("Query=" + queryStr);
      final Query query = em.createQuery(queryStr);
      int index = 0;
      for (final String attributeParm : attributeCodeList) {
        query.setParameter("attributeCode" + index, attributeParm);
        System.out.println("attributeCode" + index + "=:" + attributeParm);
        index++;
      }
      index = 0;
      for (final String valueParm : valueList) {
        query.setParameter("valueString" + index, valueParm);
        System.out.println("valueString" + index + "=:" + valueParm);
        index++;
      }
      query.setFirstResult(pageStart).setMaxResults(pageSize).getResultList();
      eeResults = query.getResultList();
    }
    if (eeResults.isEmpty()) {
      System.out.println("EEE IS EMPTY");
    } else {
      System.out.println("EEE Count" + eeResults.size());
      System.out.println("EEE" + eeResults);
    }
    for (final BaseEntity be : eeResults) {
      System.out.println(be.getCode() + " + attributes");
      be.getBaseEntityAttributes().stream().forEach(p -> System.out.println(p.getAttributeCode()));
    }

    // TODO: improve

    // final List<BaseEntity> results = beMap.values().stream().collect(Collectors.toList());

  }

  // @Test
  public void getChildrenWithAttributesPaged() {
    System.out.println("\n\n******************* KIDS WITH ATTRIBUTE!**************");
    final MultivaluedMap<String, String> qparams = new MultivaluedMapImpl<String, String>();
    qparams.add("pageStart", "0");
    qparams.add("pageSize", "10");
    final String sourceCode = "GRP_USERS";
    final String linkCode = "LNK_CORE";


    qparams.add("PRI_USERNAME", "user1");
    // params.add("PRI_USERNAME", "user2");

    Integer pageStart = 0;
    Integer pageSize = 10; // default
    final Boolean includeAttributes = true;
    final MultivaluedMap<String, String> params = new MultivaluedMapImpl<String, String>();
    params.putAll(qparams);

    final String pageStartStr = params.getFirst("pageStart");
    final String pageSizeStr = params.getFirst("pageSize");
    final String levelStr = params.getFirst("level");
    if (pageStartStr != null) {
      pageStart = Integer.decode(pageStartStr);
      params.remove("pageStart");
    }
    if (pageSizeStr != null) {
      pageSize = Integer.decode(pageSizeStr);
      params.remove("pageSize");
    }
    if (levelStr != null) {
      Integer.decode(levelStr);
      params.remove("level");
    }


    final List<BaseEntity> eeResults;
    new HashMap<String, BaseEntity>();

    if (includeAttributes) {


      // ugly and insecure
      final Integer pairCount = params.size();
      if (pairCount == 0) {
        eeResults = em.createQuery(
            "SELECT distinct be FROM BaseEntity be,EntityEntity ee JOIN be.baseEntityAttributes bee where ee.pk.targetCode=be.code and ee.pk.attribute.code=:linkAttributeCode and ee.pk.source.code=:sourceCode")
            .setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode)
            .setFirstResult(pageStart).setMaxResults(pageSize).getResultList();


      } else {
        System.out.println("PAIR COUNT IS NOT ZERO " + pairCount);
        String eaStrings = "";
        String eaStringsQ = "";
        if (pairCount > 0) {
          eaStringsQ = "(";
          for (int i = 0; i < (pairCount); i++) {
            eaStrings += ",EntityAttribute ea" + i;
            eaStringsQ += "ea" + i + ".baseEntityCode=be.code or ";
          }
          eaStringsQ = eaStringsQ.substring(0, eaStringsQ.length() - 4);
          eaStringsQ += ") and ";
        }


        String queryStr = "SELECT distinct be FROM BaseEntity be,EntityEntity ee" + eaStrings
            + "  JOIN be.baseEntityAttributes bee where " + eaStringsQ
            + "  ee.pk.targetCode=be.code and ee.pk.attribute.code=:linkAttributeCode and ee.pk.source.code=:sourceCode and ";
        int attributeCodeIndex = 0;
        int valueIndex = 0;
        final List<String> attributeCodeList = new ArrayList<String>();
        final List<String> valueList = new ArrayList<String>();

        for (final Map.Entry<String, List<String>> entry : params.entrySet()) {
          if (entry.getKey().equals("pageStart") || entry.getKey().equals("pageSize")) { // ugly
            continue;
          }
          final List<String> qvalueList = entry.getValue();
          if (!qvalueList.isEmpty()) {
            // create the value or
            String valueQuery = "(";
            for (final String value : qvalueList) {
              valueQuery +=
                  "ea" + attributeCodeIndex + ".valueString=:valueString" + valueIndex + " or ";
              valueList.add(valueIndex, value);
              valueIndex++;
            }
            // remove last or
            valueQuery = valueQuery.substring(0, valueQuery.length() - 4);
            valueQuery += ")";
            attributeCodeList.add(attributeCodeIndex, entry.getKey());
            if (attributeCodeIndex > 0) {
              queryStr += " and ";
            }
            queryStr += " ea" + attributeCodeIndex + ".attributeCode=:attributeCode"
                + attributeCodeIndex + " and " + valueQuery;
            System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
          }
          attributeCodeIndex++;

        }
        System.out.println("KIDS + ATTRIBUTE Query=" + queryStr);
        final Query query = em.createQuery(queryStr);
        int index = 0;
        for (final String attributeParm : attributeCodeList) {
          query.setParameter("attributeCode" + index, attributeParm);
          System.out.println("attributeCode" + index + "=:" + attributeParm);
          index++;
        }
        index = 0;
        for (final String valueParm : valueList) {
          query.setParameter("valueString" + index, valueParm);
          System.out.println("valueString" + index + "=:" + valueParm);
          index++;
        }
        query.setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode);

        query.setFirstResult(pageStart).setMaxResults(pageSize);
        eeResults = query.getResultList();


      }
    } else {
      Log.info("**************** ENTITY ENTITY WITH NO ATTRIBUTES ****************");


      // ugly and insecure
      final Integer pairCount = params.size();
      if (pairCount == 0) {
        eeResults = em.createQuery(
            "SELECT distinct be FROM BaseEntity be,EntityEntity ee  where ee.pk.targetCode=be.code and ee.pk.attribute.code=:linkAttributeCode and ee.pk.source.code=:sourceCode")
            .setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode)
            .setFirstResult(pageStart).setMaxResults(pageSize).getResultList();


      } else {
        System.out.println("PAIR COUNT  " + pairCount);
        String eaStrings = "";
        String eaStringsQ = "";
        if (pairCount > 0) {
          eaStringsQ = "(";
          for (int i = 0; i < (pairCount); i++) {
            eaStrings += ",EntityAttribute ea" + i;
            eaStringsQ += "ea" + i + ".baseEntityCode=be.code or ";
          }
          eaStringsQ = eaStringsQ.substring(0, eaStringsQ.length() - 4);
          eaStringsQ += ") and ";
        }

        String queryStr = "SELECT distinct be FROM BaseEntity be,EntityEntity ee" + eaStrings
            + "  where " + eaStringsQ
            + " ee.pk.targetCode=be.code and ee.pk.attribute.code=:linkAttributeCode and ee.pk.source.code=:sourceCode and ";
        int attributeCodeIndex = 0;
        int valueIndex = 0;
        final List<String> attributeCodeList = new ArrayList<String>();
        final List<String> valueList = new ArrayList<String>();

        for (final Map.Entry<String, List<String>> entry : params.entrySet()) {
          if (entry.getKey().equals("pageStart") || entry.getKey().equals("pageSize")) { // ugly
            continue;
          }
          final List<String> qvalueList = entry.getValue();
          if (!qvalueList.isEmpty()) {
            // create the value or
            String valueQuery = "(";
            for (final String value : qvalueList) {
              valueQuery +=
                  "ea" + attributeCodeIndex + ".valueString=:valueString" + valueIndex + " or ";
              valueList.add(valueIndex, value);
              valueIndex++;
            }
            // remove last or

            valueQuery = valueQuery.substring(0, valueQuery.length() - 4);

            valueQuery += ")";
            attributeCodeList.add(attributeCodeIndex, entry.getKey());
            if (attributeCodeIndex > 0) {
              queryStr += " and ";
            }
            queryStr += " ea" + attributeCodeIndex + ".attributeCode=:attributeCode"
                + attributeCodeIndex + " and " + valueQuery;
            System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
          }
          attributeCodeIndex++;

        }
        System.out.println("KIDS + ATTRIBUTE Query=" + queryStr);
        final Query query = em.createQuery(queryStr);
        int index = 0;
        for (final String attributeParm : attributeCodeList) {
          query.setParameter("attributeCode" + index, attributeParm);
          System.out.println("attributeCode" + index + "=:" + attributeParm);
          index++;
        }
        index = 0;
        for (final String valueParm : valueList) {
          query.setParameter("valueString" + index, valueParm);
          System.out.println("valueString" + index + "=:" + valueParm);
          index++;
        }
        query.setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode);

        query.setFirstResult(pageStart).setMaxResults(pageSize);
        eeResults = query.getResultList();

      }
      for (final BaseEntity be : eeResults) {
        be.setBaseEntityAttributes(null); // ugly
      }

    }
    System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++");
    // TODO: improve

    // final List<BaseEntity> results = beMap.values().stream().collect(Collectors.toList());

  }

  // @Test
  public void sqlBEFilterTest() {
    // final String sql =
    // "SELECT distinct be FROM BaseEntity be,EntityEntity ee,EntityAttribute ea0,EntityAttribute
    // ea1 JOIN be.baseEntityAttributes bee where (ea0.baseEntityCode=be.code or
    // ea1.baseEntityCode=be.code) and ee.pk.targetCode=be.code and
    // ee.pk.attribute.code=:linkAttributeCode and ee.pk.source.code=:sourceCode and
    // ea0.attributeCode=:attributeCode0 and (ea0.valueString='user1') and
    // ea1.attributeCode=:attributeCode1 and (ea1.valueString='user2')";
    final String sql =
        // "SELECT distinct be FROM BaseEntity be,EntityEntity ee,EntityAttribute ea0 JOIN
        // be.baseEntityAttributes bee where (ea0.baseEntityCode=be.code ) and
        // ee.pk.targetCode=be.code and ee.pk.attribute.code=:linkAttributeCode and
        // ee.pk.source.code=:sourceCode and ea0.attributeCode=:attributeCode0 and
        // (ea0.valueString=:valueString0)";
        "SELECT distinct be FROM BaseEntity be,EntityEntity ee,EntityAttribute ea0  JOIN be.baseEntityAttributes bee where (ea0.baseEntityCode=be.code) and ee.pk.targetCode=be.code and ee.pk.attribute.code=:linkAttributeCode and ee.pk.source.code=:sourceCode and  ea0.attributeCode=:attributeCode0 and (ea0.valueString=:valueString0)";
    try {
      final List<BaseEntity> eeResults = em.createQuery(sql).setParameter("sourceCode", "GRP_USERS")
          .setParameter("linkAttributeCode", "LNK_CORE")
          .setParameter("attributeCode0", "PRI_USERNAME").setParameter("valueString0", "user1")
          .setFirstResult(0).setMaxResults(1000).getResultList();

      log.info("Number of users = " + eeResults.size());

      assertThat(eeResults.size(), is(1));
    } catch (final Exception e) {
      // TODO Auto-generated catch block
      log.info("NO RESULT");
    }
  }


  //// @Test
  public void getKeycloakUsersTest() {
    KeycloakService ks;
    final Map<String, Map<String, Object>> usersMap = new HashMap<String, Map<String, Object>>();

    try {
      ks = new KeycloakService("https://keycloakUrl", "genny", "user1", "password1", "genny");
      final List<LinkedHashMap> users = ks.fetchKeycloakUsers();
      for (final Object user : users) {
        final LinkedHashMap map = (LinkedHashMap) user;
        final Map<String, Object> userMap = new HashMap<String, Object>();
        for (final Object key : map.keySet()) {
          // System.out.println(key + ":" + map.get(key));
          userMap.put((String) key, map.get(key));

        }
        usersMap.put((String) userMap.get("username"), userMap);
        System.out.println();
      }

      System.out.println("finished");
    } catch (final IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    for (final String username : usersMap.keySet()) {
      final MultivaluedMap params = new MultivaluedMapImpl();
      params.add("PRI_USERNAME", username);
      final Map<String, Object> userMap = usersMap.get(username);

      final List<BaseEntity> users = service.findBaseEntitysByAttributeValues(params, true, 0, 1);
      if (users.isEmpty()) {
        final String code = "PER_" + username;
        final String firstName = (String) userMap.get("firstName");
        final String lastName = (String) userMap.get("lastName");
        final String name = firstName + " " + lastName;
        final String email = (String) userMap.get("email");
        final String id = (String) userMap.get("id");
        final Long unixSeconds = (Long) userMap.get("createdTimestamp");
        final Date date = new Date(unixSeconds); // *1000 is to convert seconds to milliseconds
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z"); // the format of
                                                                                    // your date
        sdf.setTimeZone(TimeZone.getTimeZone("GMT+10")); // give a timezone reference for formating
        sdf.format(date);
        final Attribute firstNameAtt = service.findAttributeByCode("PRI_FIRSTNAME");
        final Attribute lastNameAtt = service.findAttributeByCode("PRI_LASTNAME");
        final Attribute nameAtt = service.findAttributeByCode("PRI_NAME");
        final Attribute emailAtt = service.findAttributeByCode("PRI_EMAIL");
        final Attribute uuidAtt = service.findAttributeByCode("PRI_UUID");
        final Attribute usernameAtt = service.findAttributeByCode("PRI_USERNAME");

        try {
          final BaseEntity user = new BaseEntity(code, name);

          user.addAttribute(firstNameAtt, 0.0, firstName);
          user.addAttribute(lastNameAtt, 0.0, lastName);
          user.addAttribute(nameAtt, 0.0, name);
          user.addAttribute(emailAtt, 0.0, email);
          user.addAttribute(uuidAtt, 0.0, id);
          user.addAttribute(usernameAtt, 0.0, username);
          service.insert(user);

          System.out.println("BE:" + user);
        } catch (final BadDataException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }

      } else {
        users.get(0);
      }

    }

  }

  public void updateUser(final String realm, final String keycloakId, final String fn,
      final String ln) throws Exception {
    KeycloakService ks;

    ks = new KeycloakService("https://keycloakUrl", "genny", "user1", "password1", "genny");

    final UserResource userResource = ks.getKeycloak().realm(realm).users().get(keycloakId);
    final UserRepresentation user = userResource.toRepresentation();
    user.setFirstName(fn);
    user.setLastName(ln);
    userResource.update(user);

  }

//  @Test
  public void mergeUtilTest() {

    // String template = "Template.ftl";
    String teststr =
        "Welcome {{USER.PRI_FIRSTNAME}} {{USER.PRI_LASTNAME}} !! Your contact number is {{USER.PRI_MOBILE}} and email ID is {{USER.PRI_EMAIL}} !!";

    BaseEntity projectBaseEnt = service.findBaseEntityByCode("PRJ_GENNY");
    BaseEntity userBaseEnt = service.findBaseEntityByCode("PER_USER2");
    BaseEntity dashboardBaseEnt = service.findBaseEntityByCode("GRP_DASHBOARD");

    Map<String, BaseEntity> templateEntityMap = new HashMap<>();
    templateEntityMap.put("PROJECT", projectBaseEnt);
    templateEntityMap.put("USER", userBaseEnt);
    templateEntityMap.put("JOB", dashboardBaseEnt);

    String mergedString = MergeUtil.merge(teststr, templateEntityMap);
    System.out.println("merged string in template ::" + mergedString);
    
    String testStr1 = "Welcome {{OWNER.PRI_FIRSTNAME}} {{OWNER.PRI_LASTNAME}} ! Your load of type {{LOAD.PRI_LOAD_TYPE}} has been picked from {{LOAD.PRI_FULL_PICKUP_ADDRESS}} by {{DRIVER.PRI_FIRSTNAME}}. It will be delivered at {{LOAD.PRI_FULL_DROPOFF_ADDRESS}}";

    Map<String, BaseEntity> templateEntityMap1 = new HashMap<>();
    BaseEntity loadEnt = service.findBaseEntityByCode("LOD_LOAD5");
    BaseEntity ownerEnt = service.findBaseEntityByCode("PER_USER2");
    BaseEntity driverEnt = service.findBaseEntityByCode("PER_USER1");
    templateEntityMap1.put("LOAD", loadEnt);
    templateEntityMap1.put("OWNER", ownerEnt);
    templateEntityMap1.put("DRIVER", driverEnt);
    
    String mergedString1 = MergeUtil.merge(testStr1, templateEntityMap1);
    System.out.println("merged string in template ::" + mergedString1);
    
  }

  @Test
  public void addLinkTest() {
    getEm().getTransaction().begin();
    
    final Gson gson = new GsonBuilder().registerTypeAdapter(Money.class, new MoneyDeserializer())
        .registerTypeAdapter(LocalDateTime.class, new JsonDeserializer<LocalDateTime>() {
          @Override
          public LocalDateTime deserialize(final JsonElement json, final Type type,
              final JsonDeserializationContext jsonDeserializationContext)
              throws JsonParseException {
            return ZonedDateTime.parse(json.getAsJsonPrimitive().getAsString()).toLocalDateTime();
          }

          public JsonElement serialize(final LocalDateTime date, final Type typeOfSrc,
              final JsonSerializationContext context) {
            return new JsonPrimitive(date.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)); // "yyyy-mm-dd"
          }
        }).create();


    String json = "{ " + "\"created\": \"2014-11-01T12:34:56+10:00\"," + "\"value\": \"Bob\","
        + "\"expired\": false," + "\"refused\": false," + "\"weight\": 1," + "\"version\": 1,"
        + "\"targetCode\": \"PER_USER1\"," + "\"sourceCode\": \"PER_USER1\","
        + "\"attributeCode\": \"PRI_FIRSTNAME\"" + "}";

    final Answer answer = gson.fromJson(json, Answer.class);
    log.info("Answer loaded :" + answer);
    final Long answerId = service.insert(answer);

    log.info("answerId=" + answerId);

    BaseEntity user1 = service.findBaseEntityByCode("PER_USER1");
    BaseEntity testGroup = service.findBaseEntityByCode("GRP_TEST");
    BaseEntity testGroup2 = service.findBaseEntityByCode("GRP_TEST2");
    
    try {
		EntityEntity ee = service.addLink(testGroup.getCode(), user1.getCode(), "LNK_TEST", new Double(3.14), 1.2);
	
		assertEquals(ee.getPk().getAttribute().getCode(),"LNK_TEST");
		
		// fetch link
		EntityEntity newEntity = service.findEntityEntity(testGroup.getCode(), user1.getCode(), "LNK_TEST");
		assertEquals(newEntity.getPk().getAttribute().getCode(),"LNK_TEST");
		assertEquals(newEntity.getPk().getSource().getCode(),testGroup.getCode());
		assertEquals(newEntity.getPk().getTargetCode(),user1.getCode());
		
	    final MultivaluedMap<String, String> params = new MultivaluedMapImpl<String, String>();
	//    params.add("pageStart", "0");
	//    params.add("pageSize", "2");


		List<BaseEntity> baseEntitys = service.findChildrenByAttributeLink(testGroup.getCode(), "LNK_TEST", false, 0, 10, 2, params);
		List<BaseEntity> baseEntitys2 = service.findChildrenByAttributeLink(testGroup2.getCode(), "LNK_TEST", false, 0, 10, 2, params);
		// Check baseEntitys has testGroup and no testGroup2
		assertEquals(baseEntitys.contains(user1),true);
		assertEquals(baseEntitys2.contains(user1),false);
		
		
		// Move link!
		
		service.moveLink(testGroup.getCode(), user1.getCode(), "LNK_TEST", testGroup2.getCode());
		List<BaseEntity> baseEntitysA = service.findChildrenByAttributeLink(testGroup.getCode(), "LNK_TEST", false, 0, 10, 2, params);
		List<BaseEntity> baseEntitys2A = service.findChildrenByAttributeLink(testGroup2.getCode(), "LNK_TEST", false, 0, 10, 2, params);
		// Check baseEntitys has testGroup and no testGroup2
		assertEquals(baseEntitysA.contains(user1),false);
		assertEquals(baseEntitys2A.contains(user1),true);
		
		// now fetch all the links for a target
		List<Link> links = service.findLinks(user1.getCode(), "LNK_TEST");
		Integer linkCount = links.size();
		assertEquals(linkCount==1,true);
		assertEquals(links.get(0).getSourceCode().equals(testGroup2.getCode()),true); // check it moved
	} catch (IllegalArgumentException | BadDataException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    getEm().getTransaction().commit();
  
  }
  
  @Test
  public <T extends CoreEntity> void testGen() {
    System.out.println("\n\n\n\n******222*******\n\n\n\n\n\n");
    getEm().getTransaction().begin();
    T object = (T) new BaseEntity("PER_1","codi");
    System.out.println("ooo b j e c t "+service.upsert(object));
    System.out.println("ooo b j e c t "+service.findBaseEntityByCode("PER_1"));
    getEm().getTransaction().commit();
  }
  
  
  @Test
  public void recursiveAsks() {

	  
	      try {
			AttributeText attributeFirstname2 =
				        new AttributeText(AttributeText.getDefaultCodePrefix() + "FIRSTNAME_TEST2", "Firstname");
			  AttributeText  attributeLastname2 =
				        new AttributeText(AttributeText.getDefaultCodePrefix() + "LASTNAME_TEST2", "Surname");
			  AttributeDate  attributeBirthdate2 =
				        new AttributeDate(AttributeText.getDefaultCodePrefix() + "BIRTHDAY2", "Date of Birth");
			  AttributeText  attributeMiddlename2 =
			        new AttributeText(AttributeText.getDefaultCodePrefix() + "MIDDLENAME_TEST2", "Middle Name");
			  
			  AttributeText attributeStreetAddress12 =
				        new AttributeText(AttributeText.getDefaultCodePrefix() + "STREET_ADDRESS1_TEST2", "Street Address 1");
			  AttributeText attributeStreetAddress22 =
				        new AttributeText(AttributeText.getDefaultCodePrefix() + "STREET_ADDRESS2_TEST2", "Street Address 2");
			  AttributeText attributeCity2 =
				        new AttributeText(AttributeText.getDefaultCodePrefix() + "CITY_TEST2", "City");
			  AttributeText attributeState2 =
				        new AttributeText(AttributeText.getDefaultCodePrefix() + "STATE_TEST2", "State");
			  AttributeText attributePostcode2 =
				        new AttributeText(AttributeText.getDefaultCodePrefix() + "POSTCODE_TEST2", "Postcode");
			  AttributeText attributeCountry2 =
				        new AttributeText(AttributeText.getDefaultCodePrefix() + "COUNTRY_TEST2", "Country");
			  

				   Person person2 = new Person("Clark Kent");

				    person2.addAttribute(attributeFirstname2, 1.0);
				    person2.addAttribute(attributeLastname2, 0.8);
				    person2.addAttribute(attributeBirthdate2, 0.6, LocalDate.of(1989, 1, 7));
				    person2.addAttribute(attributeMiddlename2, 0.9);
				    
				    person2.addAttribute(attributeStreetAddress12, 1.0);
				    person2.addAttribute(attributeStreetAddress22, 1.0);
				    person2.addAttribute(attributeCity2, 1.0);
				    person2.addAttribute(attributeState2, 1.0);
				    person2.addAttribute(attributePostcode2, 1.0);
				    person2.addAttribute(attributeCountry2, 1.0);
				    
				    getEm().getTransaction().begin();
			  for (final EntityAttribute ea : person2.getBaseEntityAttributes()) {
			    service.insert(ea.getAttribute());
			  }
			  service.upsert(person2);
			  getEm().getTransaction().commit();
			  
			  System.out.println(person2);

			      
			  Question questionFirstname2 = new Question(Question.getDefaultCodePrefix() + "FIRSTNAME2", "Firstname:",
			      attributeFirstname2);
			  Question questionMiddlename2 = new Question(Question.getDefaultCodePrefix() + "MIDDLENAME2", "Middlename:",
			          attributeMiddlename2);
			  Question questionLastname2 = new Question(Question.getDefaultCodePrefix() + "LASTNAME2", "Lastname:",
			      attributeLastname2);
			  Question questionBirthdate2 = new Question(Question.getDefaultCodePrefix() + "BIRTHDATE2",
			      "Birthdate:", attributeBirthdate2);
			  
			  Question questionStreetAddress12 = new Question(Question.getDefaultCodePrefix() + "STREET_ADDRESS_12", "Street Address 1:",
			          attributeStreetAddress12);
			  Question questionStreetAddress22 = new Question(Question.getDefaultCodePrefix() + "STREET_ADDRESS_22", "Street Address 2:",
			          attributeStreetAddress22);
			  Question questionCity2 = new Question(Question.getDefaultCodePrefix() + "CITY2", "City:",
			          attributeCity2);
			  Question questionState2 = new Question(Question.getDefaultCodePrefix() + "STATE2", "State:",
			          attributeState2);
			  Question questionPostcode2 = new Question(Question.getDefaultCodePrefix() + "POSTCODE2", "Postcode:",
			          attributePostcode2);
			  Question questionCountry2 = new Question(Question.getDefaultCodePrefix() + "COUNTRY2", "Country:",
			          attributeCountry2);
			  
			  getEm().getTransaction().begin();
			  service.upsert(questionFirstname2);
			  service.upsert(questionMiddlename2);
			  service.upsert(questionLastname2);
			  service.upsert(questionBirthdate2);
			  service.upsert(questionStreetAddress12);
			  service.upsert(questionStreetAddress22);
			  service.upsert(questionCity2);
			  service.upsert(questionState2);	
			  service.upsert(questionPostcode2);
			  service.upsert(questionCountry2);
			  
 
			 Question questionName = new Question(Question.getDefaultCodePrefix() + "NAME", "Name:");
			 questionName.addChildQuestion(questionFirstname2.getCode(), 10.0, true);
			 questionName.addChildQuestion(questionMiddlename2.getCode(), 20.0, false);
			 questionName.addChildQuestion(questionLastname2.getCode(), 30.0, true);
			 service.upsert(questionName);
			 
			 
			 Question questionAddress = new Question(Question.getDefaultCodePrefix() + "ADDRESS", "Address:");
			 questionAddress.addChildQuestion(questionStreetAddress12.getCode(), 10.0, true);
			 questionAddress.addChildQuestion(questionStreetAddress22.getCode(), 20.0, false);
			 questionAddress.addChildQuestion(questionCity2.getCode(), 30.0, true);
			 questionAddress.addChildQuestion(questionState2.getCode(), 40.0, true);
			 questionAddress.addChildQuestion(questionPostcode2.getCode(), 50.0, true);
			 questionAddress.addChildQuestion(questionCountry2.getCode(), 60.0, true);

			 service.upsert(questionAddress);
			 
			 getEm().getTransaction().commit();
			 
			 System.out.println("Question Name:"+questionName);
			 System.out.println("Question Address:"+questionAddress);
			 
			 // Now find recursive Asks
			 
			 List<Ask> asks = service.findAsksWithQuestions();
			 
			 
		} catch (BadDataException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	     


  }
  
@Test
public void questionGroupTest()
{
   
	getEm().getTransaction().begin();
	Question addressGroupQuestion = service.findQuestionByCode("QUE_ADDRESS_GRP");
	
	System.out.println("Question Address group = "+addressGroupQuestion);
	
	List<Ask> asks = service.createAsksByQuestionCode2(addressGroupQuestion.getCode(), "PER_USER1","PER_USER1");
	getEm().getTransaction().commit();
	
	System.out.println("Asks:"+asks);
	
	
}

	@Test
	public void messageTemplateTest() {

		QBaseMSGMessageTemplate template = service.findTemplateByCode("MSG_CH40_MOVE_GRP_IN_TRANSIT");
		System.out.println("template description ::"+template.getDescription());

	}
	
	
	@Test
	public void findAsks2Test()
	{
		System.out.println("FIND ASKS 2 TEST ");
		Question rootQuestion = service.findQuestionByCode("QUE_NEW_USER_PROFILE_GRP");
		 BaseEntity source = service.findBaseEntityByCode("PER_USER1");
		getEm().getTransaction().begin();
		List<Ask> asks = service.findAsks2(rootQuestion, source,  source,
				false) ;
		getEm().getTransaction().commit();
		
		System.out.println("Asks:"+asks);
		System.out.println("Number of asks=" + asks.size());
		System.out.println("Number of asks=" + asks);
		final QDataAskMessage askMsgs = new QDataAskMessage(asks.toArray(new Ask[0]));
		System.out.println("askMsgs=" + askMsgs);

		GsonBuilder gsonBuilder = new GsonBuilder().registerTypeAdapter(Money.class, new MoneyDeserializer());
		Gson gson = null;

		gsonBuilder.registerTypeAdapter(LocalDateTime.class, new DateTimeDeserializer());
		gson = gsonBuilder.excludeFieldsWithoutExposeAnnotation().create();
		System.out.println("Performing JSON conversion ...");
		int i=0;
		for (Ask ask : askMsgs.getItems()) {
			for (Ask ask2 : ask.getChildAsks()) {
			System.out.println(ask2);
//			for (Ask ask3 : ask2.getChildAsks()) 
			try {
				if (i>-1) {
				String j = gson.toJson(ask2);
				System.out.println("Json="+j);
				}
				i++;
			} catch (Exception e) {
				System.out.println(ask2.getQuestionCode()+" crapped itri ");
			}
			}
		}
//		String json2 = gson.toJson(askMsgs.getItems()[0].getChildAsks()[1].getChildAsks());
//		String json3 = gson.toJson(askMsgs.getItems()[1].getChildAsks()[1].getChildAsks());
//		askMsgs.getItems()[0].getChildAsks()[3] = null;
//		askMsgs.getItems()[0].getChildAsks()[2] = null;
//		askMsgs.getItems()[0].getChildAsks()[1] = null;
		String json = gson.toJson(askMsgs);
		System.out.println("json:"+json);
		
		
	}
	
	@Test
	public void findAsks2SelectionTest()
	{
		System.out.println("FIND ASKS 2 TEST MEMBERHUB SELECTION");
		Question rootQuestion = service.findQuestionByCode("QUE_MEMBERHUB_GRP");
		 BaseEntity source = service.findBaseEntityByCode("PER_USER1");
		getEm().getTransaction().begin();
		List<Ask> asks = service.findAsks2(rootQuestion, source,  source,
				false) ;
		getEm().getTransaction().commit();
		
		System.out.println("Asks:"+asks);
		System.out.println("Number of asks=" + asks.size());
		System.out.println("Number of asks=" + asks);
		final QDataAskMessage askMsgs = new QDataAskMessage(asks.toArray(new Ask[0]));
		System.out.println("askMsgs=" + askMsgs);

		GsonBuilder gsonBuilder = new GsonBuilder();
		Gson gson = null;

		gsonBuilder.registerTypeAdapter(LocalDateTime.class, new DateTimeDeserializer());
		gson = gsonBuilder.excludeFieldsWithoutExposeAnnotation().create();
		System.out.println("Performing JSON conversion ...");
		int i=0;
		for (Ask ask : askMsgs.getItems()) {
			for (Ask ask2 : ask.getChildAsks()) {
			System.out.println(ask2);
//			for (Ask ask3 : ask2.getChildAsks()) 
			try {
				if (i>-1) {
				String j = gson.toJson(ask2);
				System.out.println("Json="+j);
				}
				i++;
			} catch (Exception e) {
				System.out.println(ask2.getQuestionCode()+" crapped itri ");
			}
			}
		}
//		String json2 = gson.toJson(askMsgs.getItems()[0].getChildAsks()[1].getChildAsks());
//		String json3 = gson.toJson(askMsgs.getItems()[1].getChildAsks()[1].getChildAsks());
//		askMsgs.getItems()[0].getChildAsks()[3] = null;
//		askMsgs.getItems()[0].getChildAsks()[2] = null;
//		askMsgs.getItems()[0].getChildAsks()[1] = null;
		String json = gson.toJson(askMsgs);
		System.out.println("json:"+json);
		
		
	}
	
	@Test
	public void asks2Test()
	{
		// http://10.0.0.197:8280/qwanda/baseentitys/PER_USER1/asks2/QUE_NEW_USER_PROFILE_GRP/PER_USER1
		// http://localhost:8280/qwanda/baseentitys/PER_USER1/asks2/QUE_OFFER_DETAILS_GRP/OFR_OFFER1
		List<Ask> asks = service.createAsksByQuestionCode2("QUE_NEW_USER_PROFILE_GRP", "PER_USER1", "PER_USER1");
		System.out.println("Number of asks=" + asks.size());
		System.out.println("Number of asks=" + asks);
		QDataAskMessage askMsgs = new QDataAskMessage(asks.toArray(new Ask[0]));
		GsonBuilder gsonBuilder = new GsonBuilder();
		Gson gson = null;

		gsonBuilder.registerTypeAdapter(LocalDateTime.class, new DateTimeDeserializer());
		gson = gsonBuilder.excludeFieldsWithoutExposeAnnotation().create();
		String json = gson.toJson(askMsgs);
		System.out.println("askMsgs=" + json);

		asks = service.createAsksByQuestionCode2("QUE_NEW_USER_PROFILE_GRP", "PER_USER1", "OFR_OFFER1");
		System.out.println("Number of asks=" + asks.size());
		System.out.println("Number of asks=" + asks);
		askMsgs = new QDataAskMessage(asks.toArray(new Ask[0]));

		json = gson.toJson(askMsgs);
		System.out.println("askMsgs 2=" + json);
	}
	
	@Test
	public void stakeholderQuery()
	{
		Integer pageStart = 0;
		Integer pageSize = 10; // default
		Integer level = 1;

		MultivaluedMap<String, String> params = new MultivaluedMapImpl<String, String>();
		MultivaluedMap<String, String> qparams = new MultivaluedMapImpl<String, String>();
		qparams.putAll(params);


		final List<BaseEntity> targets = service.findChildrenByAttributeLink("GRP_NEW_ITEMS", "LNK_CORE", true, pageStart,
				pageSize, level, qparams, "PER_USER1");

		for (final BaseEntity be : targets) {
			log.info("\n" + be.getCode() + " + attributes");
			be.getBaseEntityAttributes().stream().forEach(p -> System.out.println(p.getAttributeCode()));
		}

	}
}
