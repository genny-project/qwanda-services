package life.genny.services;

import static java.lang.System.out;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import javax.persistence.EntityExistsException;
import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import javax.transaction.Transactional;
import javax.validation.constraints.NotNull;
import javax.ws.rs.core.MultivaluedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.hibernate.exception.ConstraintViolationException;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.keycloak.KeycloakSecurityContext;
import org.mortbay.log.Log;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import life.genny.qwanda.Answer;
import life.genny.qwanda.AnswerLink;
import life.genny.qwanda.Ask;
import life.genny.qwanda.CoreEntity;
import life.genny.qwanda.GPS;
import life.genny.qwanda.Link;
import life.genny.qwanda.Question;
import life.genny.qwanda.QuestionQuestion;
import life.genny.qwanda.attribute.Attribute;
import life.genny.qwanda.attribute.AttributeLink;
import life.genny.qwanda.attribute.AttributeText;
import life.genny.qwanda.attribute.EntityAttribute;
import life.genny.qwanda.datatype.DataType;
import life.genny.qwanda.entity.BaseEntity;
import life.genny.qwanda.entity.Company;
import life.genny.qwanda.entity.EntityEntity;
import life.genny.qwanda.entity.Group;
import life.genny.qwanda.entity.Person;
import life.genny.qwanda.entity.Product;
import life.genny.qwanda.exception.BadDataException;
import life.genny.qwanda.message.QBaseMSGMessageTemplate;
import life.genny.qwanda.message.QEventAttributeValueChangeMessage;
import life.genny.qwanda.rule.Rule;
import life.genny.qwanda.validation.Validation;

/**
 * This Service bean demonstrate various JPA manipulations of {@link BaseEntity}
 *
 * @author Adam Crow
 */

public class BaseEntityService2 {

  /**
   * Stores logger object.
   */
  protected static final Logger log = org.apache.logging.log4j.LogManager
      .getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());

  private static final String DEFAULT_REALM = "genny";

  EntityManager em;

  protected EntityManager getEntityManager() {
    return em;
  }

  protected BaseEntityService2() {

  }

  public BaseEntityService2(final EntityManager em) {
    this.em = em;
  }

  private Attribute createAttributeText(final String attributeName) {
    Attribute attribute = null;
    try {
      attribute = findAttributeByCode(AttributeText.getDefaultCodePrefix() + attributeName);
    } catch (final NoResultException e) {

      attribute = new AttributeText(AttributeText.getDefaultCodePrefix() + attributeName,
          StringUtils.capitalize(attributeName));

      getEntityManager().persist(attribute);
    }
    return attribute;
  }

  /**
   * @return all the {@link BaseEntity} in the db
   */
  public List<BaseEntity> getAll() {
    final Query query = getEntityManager().createQuery("SELECT e FROM BaseEntity e");
    return query.getResultList();

  }

  /**
   * Remove {@link BaseEntity} one by one and throws an exception at a given point to simulate a
   * real error and test Transaction bahaviour
   *
   * @throws IllegalStateException when removing {@link BaseEntity} at given index
   */
  public void remove(final BaseEntity entity) {
    resetMsgLists();
    final BaseEntity baseEntity = findBaseEntityById(entity.getId());
    getEntityManager().remove(baseEntity);
  }

  /**
   * Remove {@link BaseEntity} one by one and throws an exception at a given point to simulate a
   * real error and test Transaction bahaviour
   *
   * @throws IllegalStateException when removing {@link BaseEntity} at given index
   */
  public void removeBaseEntity(final String code) {
    final BaseEntity baseEntity = findBaseEntityByCode(code);
    getEntityManager().remove(baseEntity);
  }

  /**
   * Remove {@link Attribute} one by one and throws an exception at a given point to simulate a real
   * error and test Transaction bahaviour
   *
   * @throws IllegalStateException when removing {@link Attribute} at given index
   */
  public void removeAttribute(final String code) {
    final Attribute attribute = findAttributeByCode(code);
    getEntityManager().remove(attribute);
  }

  public void resetMsgLists() {
    commitMsg.clear();
    rollbackMsg.clear();
  }

  /**
   * Add a message to the commit messages list
   *
   * @param msg to add
   */
  public void addCommitMsg(final String msg) {
    commitMsg.add(msg);
  }

  /**
   * Add a message to the roll back messages list
   *
   * @param msg to add
   */
  public void addRollbackMsg(final String msg) {
    rollbackMsg.add(msg);
  }

  /**
   * @return commit messages
   */
  public List<String> getCommitMsg() {
    return commitMsg;
  }

  /**
   * @return rollback messages
   */
  public List<String> getRollbackMsg() {
    return rollbackMsg;
  }

  // Override as required
  public void sendQEventAttributeValueChangeMessage(final QEventAttributeValueChangeMessage event) {
    log.info("Send Attribute Change:" + event);
  }

  protected String getCurrentToken() {
    return "DUMMY_TOKEN";
  }

  private List<String> commitMsg = new ArrayList<>();

  private List<String> rollbackMsg = new ArrayList<>();

  public static String set(final Object item) {

    final ObjectMapper mapper = new ObjectMapper();
    // mapper.registerModule(new JavaTimeModule());

    String json = null;

    try {
      json = mapper.writeValueAsString(item);
    } catch (final JsonProcessingException e) {

    }
    return json;
  }

  /**
   * Inserts
   */

  public Long insert(final Ask ask) {
    // Fetch the associated BaseEntitys and Question

    // always check if question exists through check for unique code
    try {
      Question question = null;
      // check that these bes exist
      BaseEntity beSource = findBaseEntityByCode(ask.getSourceCode());
      BaseEntity beTarget = findBaseEntityByCode(ask.getTargetCode());
      Attribute attribute = findAttributeByCode(ask.getAttributeCode());
      Ask newAsk = null;
      if (ask.getQuestionCode() != null) {
        question = findQuestionByCode(ask.getQuestionCode());
        newAsk = new Ask(question, beSource.getCode(), beTarget.getCode());
      } else {
        newAsk = new Ask(attribute.getCode(), beSource.getCode(), beTarget.getCode(),
            attribute.getName());
      }
      Log.info("Creating new Ask " + beSource.getCode() + ":" + beTarget.getCode() + ":"
          + attribute.getCode() + ":" + (question == null ? "No Question" : question.getCode()));

      getEntityManager().persist(newAsk);
    } catch (final ConstraintViolationException e) {
      // so update otherwise // TODO merge?
      Ask existing = findAskById(ask.getId());
      existing = getEntityManager().merge(existing);
      return existing.getId();
    } catch (final PersistenceException e) {
      // so update otherwise // TODO merge?
      Ask existing = findAskById(ask.getId());
      existing = getEntityManager().merge(existing);
      return existing.getId();
    } catch (final IllegalStateException e) {
      // so update otherwise // TODO merge?
      Ask existing = findAskById(ask.getId());
      existing = getEntityManager().merge(existing);
      return existing.getId();
    }
    return ask.getId();
  }

  public Long insert(final GPS entity) {
    try {
      getEntityManager().persist(entity);

    } catch (final EntityExistsException e) {
      // so update otherwise // TODO merge?
      GPS existing = findGPSById(entity.getId());
      existing = getEntityManager().merge(existing);
      return existing.getId();

    }
    return entity.getId();
  }

  public Long insert(final Question question) {
    // always check if question exists through check for unique code
    try {
      getEntityManager().persist(question);
      System.out.println("Loaded " + question.getCode());
    } catch (final ConstraintViolationException e) {
      Question existing = findQuestionByCode(question.getCode());
      existing = getEntityManager().merge(existing);
      return existing.getId();
    } catch (final PersistenceException e) {
      Question existing = findQuestionByCode(question.getCode());
      existing = getEntityManager().merge(existing);
      return existing.getId();
    } catch (final IllegalStateException e) {
      Question existing = findQuestionByCode(question.getCode());
      existing = getEntityManager().merge(existing);
      return existing.getId();
    }
    return question.getId();
  }

  public Long insert(final Rule rule) {
    // always check if rule exists through check for unique code
    try {
      getEntityManager().persist(rule);

    } catch (final EntityExistsException e) {
      // so update otherwise // TODO merge?
      Rule existing = findRuleById(rule.getId());
      existing = getEntityManager().merge(existing);
      return existing.getId();

    }
    return rule.getId();
  }

  public Long insert(final Validation validation) {
    // always check if rule exists through check for unique code
    try {
      System.out.println("______________________________________");
      getEntityManager().persist(validation);
      System.out.println("Loaded Validation " + validation.getCode());
    } catch (final ConstraintViolationException e) {
      System.out.println("\n\n\n\n\n\n222222222222\n\n\n\n\n\n\n\n\n");
      final Validation existing = findValidationByCode(validation.getCode());
      System.out.println("\n\n\n\n\n\n" + existing + "\n\n\n\n\n\n\n\n\n");
      return existing.getId();
    } catch (final PersistenceException e) {
      System.out
          .println("\n\n\n\n\n\n22222***" + validation.getCode() + "****2222222\n\n\n\n\n\n\n\n\n");
      final Validation existing = findValidationByCode(validation.getCode());
      System.out.println("\n\n\n\n\n\n" + existing.getRegex() + "\n\n\n\n\n\n\n\n\n");
      return existing.getId();
    } catch (final IllegalStateException e) {
      System.out.println("\n\n\n\n\n\n222222222222\n\n\n\n\n\n\n\n\n");
      final Validation existing = findValidationByCode(validation.getCode());
      System.out.println("\n\n\n\n\n\n" + existing + "\n\n\n\n\n\n\n\n\n");
      return existing.getId();
    }
    return validation.getId();
  }

  public AnswerLink insert(AnswerLink answerLink) {
    // always check if rule exists through check for unique code

    AnswerLink existing = null;

    try {
      existing = findAnswerLinkByCodes(answerLink.getTargetCode(), answerLink.getSourceCode(),
          answerLink.getAttributeCode());
    } catch (Exception e) {
      // TODO Auto-generated catch block
      existing = null;
    }
    if ((existing == null)) {

      try {
        getEntityManager().persist(answerLink);
      } catch (Exception e) {
        // TODO Auto-generated catch block
        // e.printStackTrace();
        log.error("Eror in persisting answerlink");
      }

      QEventAttributeValueChangeMessage msg = new QEventAttributeValueChangeMessage(
          answerLink.getSourceCode(), answerLink.getTargetCode(), answerLink.getAttributeCode(),
          null, answerLink.getValue(), getCurrentToken());

      sendQEventAttributeValueChangeMessage(msg);
    }

    else {
      // so update otherwise // TODO merge?
      Object oldValue = existing.getValue();
      existing.setValue(answerLink.getValue());
      existing.setExpired(answerLink.getExpired());
      existing.setRefused(answerLink.getRefused());
      existing.setValueBoolean(answerLink.getValueBoolean());
      existing.setValueDateTime(answerLink.getValueDateTime());
      existing.setValueDate(answerLink.getValueDate());
      existing.setValueDouble(answerLink.getValueDouble());
      existing.setValueLong(answerLink.getValueLong());
      existing.setWeight(answerLink.getWeight());

      existing = getEntityManager().merge(existing);
      String token = getCurrentToken();

      QEventAttributeValueChangeMessage msg = new QEventAttributeValueChangeMessage(
          answerLink.getSourceCode(), answerLink.getTargetCode(), answerLink.getAttributeCode(),
          oldValue.toString(), answerLink.getValue(), token);

      sendQEventAttributeValueChangeMessage(msg);

      return existing;

    }
    return answerLink;
  }

  public Long insert(BaseEntity entity) {

    // get security
    // if (securityService.isAuthorised()) {
    // String realm = securityService.getRealm();
    // System.out.println("Realm = " + realm);
    // entity.setRealm(realm); // always override
    // }
    // entity.setRealm("genny");
    // always check if baseentity exists through check for unique code
    try {
      getEntityManager().persist(entity);
    } catch (final ConstraintViolationException e) {
      // so update otherwise // TODO merge?
      getEntityManager().merge(entity);
      return entity.getId();
    } catch (final PersistenceException e) {
      // so update otherwise // TODO merge?
      getEntityManager().merge(entity);
      return entity.getId();
    } catch (final IllegalStateException e) {
      // so update otherwise // TODO merge?
      getEntityManager().merge(entity);
      return entity.getId();
    }
    // }
    return entity.getId();
  }

@Transactional
  public Long insert(Answer answer) {
    if (answer.getAttributeCode().equalsIgnoreCase("PRI_GENDER")) {
      System.out.println("GENDER CHANGE");
    }
    log.info("insert(Answer):" + answer.getSourceCode() + ":" + answer.getTargetCode() + ":"
        + answer.getAttributeCode() + ":"
        + StringUtils.abbreviateMiddle(answer.getValue(), "...", 30));
    // always check if answer exists through check for unique code
    BaseEntity beTarget = null;
    BaseEntity beSource = null;
    Attribute attribute = null;
    Ask ask = null;

    if (answer.getValue() == null) {
      return -1L;
    }
    try {

      try {
        // check that the codes exist
        beTarget = findBaseEntityByCode(answer.getTargetCode());
        beSource = findBaseEntityByCode(answer.getSourceCode());
        attribute = findAttributeByCode(answer.getAttributeCode());

        if (answer.getAskId() != null) {
          ask = findAskById(answer.getAskId());
          if (!((answer.getSourceCode().equals(ask.getSourceCode()))
              && (answer.getAttributeCode().equals(ask.getAttributeCode()))
              && (answer.getTargetCode().equals(ask.getTargetCode())))) {
            log.error("Answer codes do not match Ask codes! " + answer);
            // return -1L; // need to throw error
          }
        }

        answer.setAttribute(attribute);

        getEntityManager().persist(answer);
        
        // Check if answer represents a link only
        if (attribute.getDataType().getClassName().startsWith("DTT_LINK_")) {
        	   // add a link
        	addLink(answer.getValue(), answer.getTargetCode(),
        			attribute.getDataType().getTypeName(), "ANSWER" , answer.getWeight());
        } else {

        // update answerlink

        AnswerLink answerLink = null;
        try {
          Optional<EntityAttribute> optExisting =
              beTarget.findEntityAttribute(answer.getAttributeCode());
          Object old = optExisting.isPresent() ? optExisting.get().getValue() : null;
          answerLink = beTarget.addAnswer(beSource, answer, answer.getWeight()); // TODo replace with soucr
          update(beTarget);
          boolean sendAttributeChangeEvent = false;
          if (!optExisting.isPresent()) {
            sendAttributeChangeEvent = true;
          }
          if (optExisting.isPresent()) {
            Object newOne = answerLink.getValue();
            if (newOne != null) {
              if (old.hashCode() != (newOne.hashCode())) {
                sendAttributeChangeEvent = true;
              }
            }
          }
          if (sendAttributeChangeEvent) {
            String oldValue = null;
            if (old != null) {
              oldValue = old.toString();
            }
            if (answerLink == null) {
              System.out.println("answerLink is Null");
            }
            if (getCurrentToken() == null) {
              System.out.println("getCurrentToken is Null");
            }
            if (answerLink.getValue() == null) {
              System.out.println("answerLink.getValue() is Null");
            }
            if (answerLink.getTargetCode() == null) {
              System.out.println("answerLink.getTargetCode() is Null");
            }
            if (answerLink.getSourceCode() == null) {
              System.out.println("answerLink.getSourceCode() is Null");
            }
            // Hack: avoid stack overflow
            Answer pojo = new Answer(answer.getSourceCode(), answer.getTargetCode(),
                answer.getAttributeCode(), answer.getValue());
            pojo.setWeight(answer.getWeight());
            pojo.setInferred(answer.getInferred());
            pojo.setExpired(answer.getExpired());
            pojo.setRefused(answer.getRefused());

            QEventAttributeValueChangeMessage msg =
                new QEventAttributeValueChangeMessage(pojo, (oldValue), getCurrentToken());

            sendQEventAttributeValueChangeMessage(msg);
          }

        } catch (final Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        }

      } catch (final EntityExistsException e) {
        System.out.println("Answer Insert EntityExistsException");
        // so update otherwise // TODO merge?
        Answer existing = findAnswerById(answer.getId());
        existing.setRefused(answer.getRefused());
        existing.setExpired(answer.getExpired());
        existing.setWeight(answer.getWeight());
        existing.setValue(answer.getValue());
        existing = getEntityManager().merge(existing);
        return existing.getId();

      }
    } catch (Exception transactionException) {
      log.error("Transaction Exception in saving Answer" + answer);
    }

    return answer.getId();
  }

@Transactional
  public Long insert(final Attribute attribute) {
    // always check if baseentity exists through check for unique code
    try {
      getEntityManager().persist(attribute);
    } catch (final ConstraintViolationException e) {
      // so update otherwise // TODO merge?
      Attribute existing = findAttributeByCode(attribute.getCode());
      existing = getEntityManager().merge(existing);
      return existing.getId();
    } catch (final PersistenceException e) {
      // so update otherwise // TODO merge?
      Attribute existing = findAttributeByCode(attribute.getCode());
      existing = getEntityManager().merge(existing);
      return existing.getId();
    } catch (final IllegalStateException e) {
      // so update otherwise // TODO merge?
      Attribute existing = findAttributeByCode(attribute.getCode());
      existing = getEntityManager().merge(existing);
      return existing.getId();
    }
    return attribute.getId();
  }

  public EntityEntity insertEntityEntity(final EntityEntity ee) {

    try {
      // getEntityManager().getTransaction().begin();
      getEntityManager().persist(ee);
      // getEntityManager().getTransaction().commit();
    } catch (Exception e) {
      // rollback
    }
    return ee;
  }

  /************************************************
   * Finish inserts
   ******************************************************************/

  /**
   * updates
   */

  public Long update(BaseEntity entity) {
    // always check if baseentity exists through check for unique code
    try {
      entity = getEntityManager().merge(entity);
    } catch (final IllegalArgumentException e) {
      // so persist otherwise
      getEntityManager().persist(entity);
    }
    return entity.getId();
  }

  public Long update(Attribute attribute) {
    // always check if attribute exists through check for unique code
    try {

      attribute = getEntityManager().merge(attribute);
    } catch (final IllegalArgumentException e) {
      // so persist otherwise
      getEntityManager().persist(attribute);
    }
    return attribute.getId();
  }

  public Long update(Ask ask) {
    // always check if ask exists through check for unique code
    try {
      ask = getEntityManager().merge(ask);
    } catch (final IllegalArgumentException e) {
      // so persist otherwise
      getEntityManager().persist(ask);
    }
    return ask.getId();
  }

  public Long update(Validation val) {
    // always check if ask exists through check for unique code
    try {
      val = getEntityManager().merge(val);
    } catch (final IllegalArgumentException e) {
      // so persist otherwise
      getEntityManager().persist(val);
    }
    return val.getId();
  }

  public AnswerLink update(AnswerLink answerLink) {
    // always check if answerLink exists through check for unique code
    try {
      answerLink = getEntityManager().merge(answerLink);
    } catch (final IllegalArgumentException e) {
      // so persist otherwise
      getEntityManager().persist(answerLink);
    }
    return answerLink;
  }

  /************************************************
   * Finish Updates
   ******************************************************************/

  /**
   * Upserts
   */

  public <T extends CoreEntity> T upsert(T object) {

    try {
      getEntityManager().persist(object);
      System.out.println("UPSERTING:" + object);
      return object;
    } catch (Exception e) {
      object = getEntityManager().merge(object);
      return object;
    }
  }

  public QuestionQuestion upsert(QuestionQuestion qq) {
    try {
      QuestionQuestion val =
          findQuestionQuestionByCode(qq.getPk().getSource().getCode(), qq.getPk().getTargetCode());
      // BeanNotNullFields copyFields = new BeanNotNullFields();
      // copyFields.copyProperties(val, qq);
      System.out.println("------- QUESTION 123 ------------" + val);
      System.out.println("------- QUESTION 456 ------------" + qq);

      val = getEntityManager().merge(qq);
      // BeanUtils.copyProperties(validation, val);
      return val;
    } catch (NoResultException e) {
      System.out.println("------- QUESTION 00 ------------");
      getEntityManager().persist(qq);
      QuestionQuestion id = qq;
      return id;
    }
  }

  public Validation upsert(Validation validation) {
    try {
      String code = validation.getCode();
      Validation val = findValidationByCode(code);
      if (val != null) {
        BeanNotNullFields copyFields = new BeanNotNullFields();
        copyFields.copyProperties(val, validation);
        val = getEntityManager().merge(val);
      } else {
        throw new NoResultException();
      }
      // BeanUtils.copyProperties(validation, val);
      return val;
    } catch (NoResultException | IllegalAccessException | InvocationTargetException e) {
      getEntityManager().persist(validation);
      Validation id = validation;
      return id;
    }
  }

  public Attribute upsert(Attribute attr) {
    try {
      String code = attr.getCode();
      Attribute val = findAttributeByCode(code);
      BeanNotNullFields copyFields = new BeanNotNullFields();
      copyFields.copyProperties(val, attr);
      val = getEntityManager().merge(val);
      // BeanUtils.copyProperties(attr, val);
      return val;
    } catch (NoResultException | IllegalAccessException | InvocationTargetException e) {
      getEntityManager().persist(attr);
      Long id = attr.getId();
      return attr;
    }
  }

  public BaseEntity upsert(BaseEntity be) {
    try {
      String code = be.getCode();
      BaseEntity val = findBaseEntityByCode(code);

      BeanNotNullFields copyFields = new BeanNotNullFields();
      copyFields.copyProperties(val, be);
      // System.out.println("***********" + val);
      val = getEntityManager().merge(val);
      // System.out.println("*******&&&&&&&&&&&&****");
      return be;
    } catch (NoResultException | IllegalAccessException | InvocationTargetException e) {
      Long id = insert(be);
      return be;
    }
  }

  public Long upsert(final BaseEntity be, Set<EntityAttribute> ba) {
    try {
      // be.setBaseEntityAttributes(null);
      out.println("****3*****" + be.getBaseEntityAttributes().stream().map(data -> data.pk)
          .reduce((d1, d2) -> d1).get());
      String code = be.getCode();
      final BaseEntity val = findBaseEntityByCode(code);
      BeanNotNullFields copyFields = new BeanNotNullFields();
      // copyFields.copyProperties(val, be);
      getEntityManager().merge(val);
      return val.getId();
    } catch (NoResultException e) {
      Long id = insert(be);
      return id;
    }

  }

  public Ask findAskById(final Long id) {
    return getEntityManager().find(Ask.class, id);
  }

  public GPS findGPSById(final Long id) {
    return getEntityManager().find(GPS.class, id);
  }

  public Question findQuestionById(final Long id) {
    return getEntityManager().find(Question.class, id);
  }

  public Answer findAnswerById(final Long id) {
    return getEntityManager().find(Answer.class, id);
  }

  public life.genny.qwanda.Context findContextById(final Long id) {
    return getEntityManager().find(life.genny.qwanda.Context.class, id);
  }

  public BaseEntity findBaseEntityById(final Long id) {
    return getEntityManager().find(BaseEntity.class, id);
  }

  public Attribute findAttributeById(final Long id) {
    return getEntityManager().find(Attribute.class, id);
  }

  public Rule findRuleById(final Long id) {
    return getEntityManager().find(Rule.class, id);
  }

  public Validation findValidationById(final Long id) {
    return getEntityManager().find(Validation.class, id);
  }

  public DataType findDataTypeById(final Long id) {
    return getEntityManager().find(DataType.class, id);
  }

  public BaseEntity findBaseEntityByCode(@NotNull final String baseEntityCode)
      throws NoResultException {

    return findBaseEntityByCode(baseEntityCode, false);

  }

  public BaseEntity findBaseEntityByCode(@NotNull final String baseEntityCode,
      boolean includeEntityAttributes) throws NoResultException {

    BaseEntity result = null;
    final String userRealmStr = getRealm();

    if (includeEntityAttributes) {
      result = (BaseEntity) getEntityManager().createQuery(
          "SELECT be FROM BaseEntity be JOIN be.baseEntityAttributes ea where be.code=:baseEntityCode and be.realm=:realmStr")
          .setParameter("baseEntityCode", baseEntityCode.toUpperCase())
          .setParameter("realmStr", userRealmStr).getSingleResult();
    } else {
      result = (BaseEntity) getEntityManager()
          .createQuery(
              "SELECT be FROM BaseEntity be where be.code=:baseEntityCode  and be.realm=:realmStr")
          .setParameter("baseEntityCode", baseEntityCode.toUpperCase())
          .setParameter("realmStr", userRealmStr).getSingleResult();

    }

    // // Ugly, add field filtering through header field list
    //
    // final List<EntityAttribute> attributes = getEntityManager()
    // .createQuery(
    // "SELECT ea FROM EntityAttribute ea where
    // ea.pk.baseEntity.code=:baseEntityCode")
    // .setParameter("baseEntityCode", baseEntityCode).getResultList();
    // result.setBaseEntityAttributes(new HashSet<EntityAttribute>(attributes));
    return result;

  }

  public Rule findRuleByCode(@NotNull final String ruleCode) throws NoResultException {

    final Rule result =
        (Rule) getEntityManager().createQuery("SELECT a FROM Rule a where a.code=:ruleCode")
            .setParameter("ruleCode", ruleCode.toUpperCase()).getSingleResult();

    return result;
  }

  public Question findQuestionByCode(@NotNull final String code) throws NoResultException {
    // System.out.println("FindQuestionByCode:"+code);
    List<Question> result = null;
    final String userRealmStr = getRealm();
    try {
      result = (List<Question>) getEntityManager()
          .createQuery("SELECT a FROM Question a where a.code=:code and a.realm=:realmStr")
          .setParameter("realmStr", userRealmStr).setParameter("code", code.toUpperCase())
          .getResultList();

    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    if (result.isEmpty())
      return null;
    return result.get(0);
  }

  public DataType findDataTypeByCode(@NotNull final String code) throws NoResultException {

    final DataType result =
        (DataType) getEntityManager().createQuery("SELECT a FROM DataType a where a.code=:code")
            .setParameter("code", code.toUpperCase()).getSingleResult();

    return result;
  }

  public Validation findValidationByCode(@NotNull final String code) throws NoResultException {
    Validation result = null;
    result =
        (Validation) getEntityManager().createQuery("SELECT a FROM Validation a where a.code=:code")
            .setParameter("code", code).getSingleResult();

    return result;
  }

  public AttributeLink findAttributeLinkByCode(@NotNull final String code)
      throws NoResultException {

    final AttributeLink result = (AttributeLink) getEntityManager()
        .createQuery("SELECT a FROM AttributeLink a where a.code=:code")
        .setParameter("code", code.toUpperCase()).getSingleResult();

    return result;
  }

  public Attribute findAttributeByCode(@NotNull final String code) throws NoResultException {

    final String userRealmStr = getRealm();
    final Attribute result = (Attribute) getEntityManager()
        .createQuery("SELECT a FROM Attribute a where a.code=:code and a.realm=:realmStr")
        .setParameter("code", code.toUpperCase()).setParameter("realmStr", userRealmStr)
        .getSingleResult();

    return result;
  }

  public AnswerLink findAnswerLinkByCodes(@NotNull final String targetCode,
      @NotNull final String sourceCode, @NotNull final String attributeCode) {

    List<AnswerLink> results = null;

    try {
      results = getEntityManager().createQuery(
          "SELECT a FROM AnswerLink a where a.targetCode=:targetCode and a.sourceCode=:sourceCode and  attributeCode=:attributeCode")
          .setParameter("targetCode", targetCode).setParameter("sourceCode", sourceCode)
          .setParameter("attributeCode", attributeCode).getResultList();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    if ((results == null) || (results.isEmpty())) {
      return null; // throw new NoResultException(sourceCode + ":" + targetCode + ":" +
                   // attributeCode);
    } else
      return results.get(0); // return first one for now TODO

  }

  public BaseEntity findUserByAttributeValue(@NotNull final String attributeCode,
      final Integer value) {
    final String userRealmStr = getRealm();

    final List<EntityAttribute> results = getEntityManager().createQuery(
        "SELECT ea FROM EntityAttribute ea where ea.pk.attribute.code=:attributeCode and ea.valueInteger=:valueInteger and ea.source.realm=:realmStr")
        .setParameter("attributeCode", attributeCode).setParameter("valueInteger", value)
        .setParameter("realmStr", userRealmStr).setMaxResults(1).getResultList();
    if ((results == null) || (results.size() == 0))
      return null;

    final BaseEntity ret = results.get(0).getPk().getBaseEntity();

    return ret;
  }

  public BaseEntity findUserByAttributeValue(@NotNull final String attributeCode,
      final String value) {
    final String userRealmStr = getRealm();

    final List<EntityAttribute> results = getEntityManager().createQuery(
        "SELECT ea FROM EntityAttribute ea where ea.pk.attribute.code=:attributeCode and ea.valueString=:value and ea.source.realm=:realmStr")
        .setParameter("attributeCode", attributeCode).setParameter("value", value).setMaxResults(1)
        .setParameter("realmStr", userRealmStr).getResultList();
    if ((results == null) || (results.size() == 0))
      return null;

    final BaseEntity ret = results.get(0).getPk().getBaseEntity();
    return ret;
  }

  public List<BaseEntity> findChildrenByAttributeLink(@NotNull final String sourceCode,
      final String linkCode, final boolean includeAttributes, final Integer pageStart,
      final Integer pageSize, final Integer level, final MultivaluedMap<String, String> params) {

    final List<BaseEntity> eeResults;
    new HashMap<String, BaseEntity>();
    final String userRealmStr = getRealm();

    System.out.println("findChildrenByAttributeLink");
    if (includeAttributes) {
      System.out.println("findChildrenByAttributeLink - includesAttributes");

      // ugly and insecure
      final Integer pairCount = params.size();
      if (pairCount.equals(0)) {
        System.out.println("findChildrenByAttributeLink - PairCount==0");
        eeResults = getEntityManager().createQuery(
            "SELECT distinct be FROM BaseEntity be,EntityEntity ee JOIN be.baseEntityAttributes bee where ee.pk.targetCode=be.code and ee.pk.attribute.code=:linkAttributeCode and ee.pk.source.code=:sourceCode  and ee.pk.source.realm=:realmStr")
            .setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode)
            .setParameter("realmStr", userRealmStr)

            .setFirstResult(pageStart).setMaxResults(pageSize).getResultList();

      } else {
        System.out.println("findChildrenByAttributeLink - PAIR COUNT IS  " + pairCount);
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
            + "  ee.pk.targetCode=be.code and ee.pk.attribute.code=:linkAttributeCode and  ee.pk.source.realm=:realmStr and ee.pk.source.code=:sourceCode and ";
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
            System.out.println("findChildrenByAttributeLink Key : " + entry.getKey() + " Value : "
                + entry.getValue());
          }
          attributeCodeIndex++;

        }
        System.out.println("findChildrenByAttributeLink KIDS + ATTRIBUTE Query=" + queryStr);
        final Query query = getEntityManager().createQuery(queryStr);
        int index = 0;
        for (final String attributeParm : attributeCodeList) {
          query.setParameter("attributeCode" + index, attributeParm);
          System.out
              .println("findChildrenByAttributeLink attributeCode" + index + "=:" + attributeParm);
          index++;
        }
        index = 0;
        for (final String valueParm : valueList) {
          query.setParameter("valueString" + index, valueParm);
          System.out.println("valueString" + index + "=:" + valueParm);
          index++;
        }
        query.setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode);
        query.setParameter("realmStr", userRealmStr);

        query.setFirstResult(pageStart).setMaxResults(pageSize);
        eeResults = query.getResultList();

      }
    } else {
      System.out.println("**************** ENTITY ENTITY WITH NO ATTRIBUTES ****************");

      // ugly and insecure
      final Integer pairCount = params.size();
      if (pairCount.equals(0)) {

        eeResults = getEntityManager().createQuery(
            "SELECT distinct be FROM BaseEntity be,EntityEntity ee  where ee.pk.targetCode=be.code and ee.pk.attribute.code=:linkAttributeCode and ee.pk.source.code=:sourceCode   and ee.pk.source.realm=:realmStr")
            .setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode)
            .setParameter("realmStr", userRealmStr).setFirstResult(pageStart)
            .setMaxResults(pageSize).getResultList();

      } else {
        System.out.println("findChildrenByAttributeLink PAIR COUNT  " + pairCount);
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
            + " ee.pk.targetCode=be.code and ee.pk.attribute.code=:linkAttributeCode and ee.pk.source.realm=:realmStr and ee.pk.source.code=:sourceCode and ";
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
            System.out.println("findChildrenByAttributeLink Key : " + entry.getKey() + " Value : "
                + entry.getValue());
          }
          attributeCodeIndex++;

        }
        System.out.println("findChildrenByAttributeLink KIDS + ATTRIBUTE Query=" + queryStr);
        final Query query = getEntityManager().createQuery(queryStr);
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
        query.setParameter("realmStr", userRealmStr);

        query.setFirstResult(pageStart).setMaxResults(pageSize);
        eeResults = query.getResultList();
        System.out.println("findChildrenByAttributeLink NULL THE ATTRIBUTES");
        // for (BaseEntity be : eeResults) {
        // be.setBaseEntityAttributes(null); // ugly
        // }
      }

    }
    // TODO: improve

    return eeResults;
  }

  public Long findChildrenByAttributeLinkCount(@NotNull final String sourceCode,
      final String linkCode, final MultivaluedMap<String, String> params) {
    final String userRealmStr = getRealm();

    Long total = 0L;
    final Integer pairCount = params.size();
    System.out.println("findChildrenByAttributeLinkCount PAIR COUNT IS " + pairCount);
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

    String queryStr = "SELECT count(distinct be) FROM BaseEntity be,EntityEntity ee" + eaStrings
        + "  where " + eaStringsQ
        + "  ee.pk.targetCode=be.code and ee.pk.attribute.code=:linkAttributeCode  and ee.pk.source.realm=:realmStr and ee.pk.source.code=:sourceCode  ";
    int attributeCodeIndex = 0;
    int valueIndex = 0;
    final List<String> attributeCodeList = new ArrayList<String>();
    final List<String> valueList = new ArrayList<String>();

    for (final Map.Entry<String, List<String>> entry : params.entrySet()) {
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
        queryStr += " and  ea" + attributeCodeIndex + ".attributeCode=:attributeCode"
            + attributeCodeIndex + " and " + valueQuery;
        System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
      }
      attributeCodeIndex++;

    }
    System.out.println("findChildrenByAttributeLinkCount KIDS + ATTRIBUTE Query=" + queryStr);
    final Query query = getEntityManager().createQuery(queryStr);
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
    query.setParameter("realmStr", userRealmStr);

    try {
      total = (Long) query.getSingleResult();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return total;
  }

  public List<EntityAttribute> findAttributesByBaseEntityId(final Long id) {
    final List<EntityAttribute> results = getEntityManager()
        .createQuery("SELECT ea FROM EntityAttribute ea where ea.pk.baseEntity.id=:baseEntityId")
        .setParameter("baseEntityId", id).getResultList();

    return results;
  }

  public List<Ask> findAsksBySourceBaseEntityId(final Long id) {
    final List<Ask> results =
        getEntityManager().createQuery("SELECT ea FROM Ask ea where ea.source.id=:baseEntityId")
            .setParameter("baseEntityId", id).getResultList();

    return results;

  }

  public List<Ask> findAsksBySourceBaseEntityCode(final String code) {
    final List<Ask> results =
        getEntityManager().createQuery("SELECT ea FROM Ask ea where ea.source.code=:baseEntityCode")
            .setParameter("baseEntityCode", code).getResultList();

    return results;
  }

  public List<Ask> findAsksByAttribute(final Attribute attribute, final BaseEntity source,
      final BaseEntity target) {
    return findAsksByAttributeCode(attribute.getCode(), source.getCode(), target.getCode());
  }

  public List<Ask> findAsksByCode(final String attributeCode, String sourceCode,
      final String targetCode) {
    List<Ask> results = null;
    try {
      results = getEntityManager().createQuery(
          "SELECT ask FROM Ask ask where ask.attributeCode=:attributeCode and ask.sourceCode=:sourceCode and ask.targetCode=:targetCode")
          .setParameter("attributeCode", attributeCode).setParameter("sourceCode", sourceCode)
          .setParameter("targetCode", targetCode).getResultList();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return results;
  }

  public List<Ask> findAsksByQuestion(final Question question, final BaseEntity source,
      final BaseEntity target) {
    return findAsksByQuestionCode(question.getCode(), source.getCode(), target.getCode());
  }

  public List<Ask> findAsksByAttributeCode(final String attributeCode, String sourceCode,
      final String targetCode) {
    List<Ask> results = null;
    try {
      results = getEntityManager().createQuery(
          "SELECT ask FROM Ask ask where ask.attributeCode=:attributeCode and ask.sourceCode=:sourceCode and ask.targetCode=:targetCode")
          .setParameter("attributeCode", attributeCode).setParameter("sourceCode", sourceCode)
          .setParameter("targetCode", targetCode).getResultList();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return results;
  }

  public QuestionQuestion findQuestionQuestionByCode(final String sourceCode,
      final String targetCode) throws NoResultException {
    QuestionQuestion result = null;
    try {
      result = (QuestionQuestion) getEntityManager().createQuery(
          "SELECT qq FROM QuestionQuestion qq where qq.source.code=:sourceCode and qq.targetCode=:targetCode")
          .setParameter("sourceCode", sourceCode).setParameter("targetCode", targetCode)
          .getSingleResult();
    } catch (Exception e) {
      throw new NoResultException("Cannot find QQ " + sourceCode + ":" + targetCode);
    }
    return result;
  }

  public List<Ask> findAsksByQuestionCode(final String questionCode, String sourceCode,
      final String targetCode) {
    List<Ask> results = null;
    final String userRealmStr = getRealm();

    try {
      results = getEntityManager().createQuery(
          "SELECT ask FROM Ask ask where ask.questionCode=:questionCode and ask.sourceCode=:sourceCode and ask.targetCode=:targetCode ")
          .setParameter("questionCode", questionCode).setParameter("sourceCode", sourceCode)
          .setParameter("targetCode", targetCode).getResultList();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return results;
  }

  public List<Ask> findAsks(final Question rootQuestion, final BaseEntity source,
      final BaseEntity target) {
    return findAsks(rootQuestion, source, target, false);
  }

  public List<Ask> findAsks(final Question rootQuestion, final BaseEntity source,
      final BaseEntity target, Boolean childQuestionIsMandatory) {
    List<Ask> asks = new ArrayList<Ask>();

    if (rootQuestion.getAttributeCode().equals(Question.QUESTION_GROUP_ATTRIBUTE_CODE)) {
      // Recurse!
      List<QuestionQuestion> qqList =
          new ArrayList<QuestionQuestion>(rootQuestion.getChildQuestions());
      Collections.sort(qqList); // sort by priority
      for (QuestionQuestion qq : qqList) {
        String qCode = qq.getPk().getTargetCode();
        Question childQuestion = findQuestionByCode(qCode);
        asks.addAll(findAsks(childQuestion, source, target, qq.getMandatory()));
      }
    } else {
      // This is an actual leaf question, so we can create an ask ...
      Ask ask = null;
      // check if this already exists?
      List<Ask> myAsks = findAsksByQuestion(rootQuestion, source, target);
      if (!((myAsks == null) || (myAsks.isEmpty()))) {
        ask = myAsks.get(0);
        ask.setMandatory(rootQuestion.getMandatory() || childQuestionIsMandatory);
      } else {
        // create one
        Boolean mandatory = rootQuestion.getMandatory() || childQuestionIsMandatory;
        ask = new Ask(rootQuestion, source.getCode(), target.getCode(), mandatory);
        ask = upsert(ask); // save
      }
      asks.add(ask);
    }

    return asks;
  }

  public List<Ask> findAsks2(final Question rootQuestion, final BaseEntity source,
      final BaseEntity target) {
    return findAsks2(rootQuestion, source, target, false);
  }

  public List<Ask> findAsks2(final Question rootQuestion, final BaseEntity source,
      final BaseEntity target, Boolean childQuestionIsMandatory) {
    List<Ask> asks = new ArrayList<Ask>();
    Boolean mandatory = rootQuestion.getMandatory() || childQuestionIsMandatory;

    Ask ask = null;
    // check if this already exists?
    List<Ask> myAsks = findAsksByQuestion(rootQuestion, source, target);
    if (!((myAsks == null) || (myAsks.isEmpty()))) {
      ask = myAsks.get(0);
      ask.setMandatory(mandatory);
    } else {
      ask = new Ask(rootQuestion, source.getCode(), target.getCode(), mandatory);
      ask = upsert(ask);
    }
    // create one
    if (rootQuestion.getAttributeCode().startsWith(Question.QUESTION_GROUP_ATTRIBUTE_CODE)) {
      // Recurse!
      List<QuestionQuestion> qqList =
          new ArrayList<QuestionQuestion>(rootQuestion.getChildQuestions());
      Collections.sort(qqList); // sort by priority
      List<Ask> childAsks = new ArrayList<Ask>();
      for (QuestionQuestion qq : qqList) {
        String qCode = qq.getPk().getTargetCode();
        log.info(rootQuestion.getCode() + " -> Child Question -> " + qCode);
        Question childQuestion = findQuestionByCode(qCode);
        childAsks.addAll(findAsks2(childQuestion, source, target, qq.getMandatory()));
      }
      Ask[] asksArray = (Ask[]) childAsks.toArray(new Ask[0]);
      ask.setChildAsks(asksArray);
      // ask.setChildAsks(childAsks);
      ask = upsert(ask); // save
    }

    asks.add(ask);
    return asks;
  }

  public List<Ask> createAsksByQuestionCode(final String questionCode, final String sourceCode,
      final String targetCode) {
    Question rootQuestion = findQuestionByCode(questionCode);
    BaseEntity source = findBaseEntityByCode(sourceCode);
    BaseEntity target = findBaseEntityByCode(targetCode);
    return createAsksByQuestion(rootQuestion, source, target);
  }

  public List<Ask> createAsksByQuestion(final Question rootQuestion, final BaseEntity source,
      final BaseEntity target) {
    List<Ask> asks = findAsks(rootQuestion, source, target);
    return asks;
  }

  public List<Ask> createAsksByQuestionCode2(final String questionCode, final String sourceCode,
      final String targetCode) {
    Question rootQuestion = findQuestionByCode(questionCode);
    BaseEntity source = findBaseEntityByCode(sourceCode);
    BaseEntity target = findBaseEntityByCode(targetCode);
    return createAsksByQuestion2(rootQuestion, source, target);
  }

  public List<Ask> createAsksByQuestion2(final Question rootQuestion, final BaseEntity source,
      final BaseEntity target) {
    List<Ask> asks = findAsks2(rootQuestion, source, target);
    return asks;
  }

  public List<Ask> findAsksByTargetBaseEntityId(final Long id) {
    final List<Ask> results =
        getEntityManager().createQuery("SELECT ea FROM Ask ea where ea.target.id=:baseEntityId")
            .setParameter("baseEntityId", id).getResultList();

    return results;

  }

  public List<Ask> findAsksByTargetBaseEntityCode(final String code) {
    final List<Ask> results =
        getEntityManager().createQuery("SELECT ea FROM Ask ea where ea.target.code=:baseEntityCode")
            .setParameter("baseEntityCode", code).getResultList();

    return results;

  }

  public List<GPS> findGPSByTargetBaseEntityId(final Long id) {
    final List<GPS> results =
        getEntityManager().createQuery("SELECT ea FROM GPS ea where ea.targetId=:baseEntityId")
            .setParameter("baseEntityId", id).getResultList();

    return results;

  }

  public List<GPS> findGPSByTargetBaseEntityCode(final String targetCode) {
    final List<GPS> results =
        getEntityManager().createQuery("SELECT ea FROM GPS ea where ea.targetCode=:baseEntityCode")
            .setParameter("baseEntityCode", targetCode).getResultList();

    return results;

  }

  public List<AnswerLink> findAnswersByTargetBaseEntityId(final Long id) {
    final List<AnswerLink> results = getEntityManager()
        .createQuery("SELECT ea FROM AnswerLink ea where ea.pk.target.id=:baseEntityId")
        .setParameter("baseEntityId", id).getResultList();

    return results;

  }

  public List<AnswerLink> findAnswersByTargetBaseEntityCode(final String targetCode) {
    final List<AnswerLink> results = getEntityManager()
        .createQuery("SELECT ea FROM AnswerLink ea where ea.pk.targetCode=:baseEntityCode")
        .setParameter("baseEntityCode", targetCode).getResultList();

    return results;

  }

  public List<AnswerLink> findAnswersBySourceBaseEntityCode(final String sourceCode) {
    final List<AnswerLink> results = getEntityManager()
        .createQuery("SELECT ea FROM AnswerLink ea where ea.pk.source.code=:baseEntityCode")
        .setParameter("baseEntityCode", sourceCode).getResultList();

    return results;

  }

  public List<Question> findQuestions() throws NoResultException {

    final List<Question> results =
        getEntityManager().createQuery("SELECT a FROM Question a").getResultList();

    return results;
  }

  public List<Ask> findAsks() throws NoResultException {

    final List<Ask> results = getEntityManager().createQuery("SELECT a FROM Ask a").getResultList();

    return results;
  }

  public List<Ask> findAsksWithQuestions() throws NoResultException {

    // log.info("find asks Realm = " + securityService.getRealm());

    final List<Ask> results =
        getEntityManager().createQuery("SELECT a FROM Ask a JOIN a.question q").getResultList();

    return results;
  }

  public List<Rule> findRules() throws NoResultException {

    final List<Rule> results =
        getEntityManager().createQuery("SELECT a FROM Rule a").getResultList();

    return results;
  }

  public List<AnswerLink> findAnswerLinks() throws NoResultException {

    final List<AnswerLink> results =
        getEntityManager().createQuery("SELECT a FROM AnswerLink a").getResultList();

    return results;
  }

  public List<EntityAttribute> findAttributesByBaseEntityCode(final String code)
      throws NoResultException {

//    final List<EntityAttribute> results = getEntityManager()
//        .createQuery(
//            "SELECT ea FROM EntityAttribute ea where ea.baseEntityCode=:baseEntityCode")
//        .setParameter("baseEntityCode", code).getResultList();
//
//    return results;
	  // THIS IS REALLY BAD AND I AM SORRY....  COULD NOT QUICKLY SOLVE HIBERNATE RECURSION
//	  BaseEntity source = this.findBaseEntityByCode(code);
  final List<EntityAttribute> ret = new ArrayList<EntityAttribute>();
  BaseEntity be = this.findBaseEntityByCode(code);
  List<Object[]> results = getEntityManager()
      .createQuery(
          "SELECT ea.pk.attribute,ea.privacyFlag,ea.weight,ea.inferred,ea.valueString,ea.valueBoolean,ea.valueDate, ea.valueDateTime,ea.valueDouble, ea.valueInteger,ea.valueLong FROM EntityAttribute ea where ea.pk.baseEntity.code=:baseEntityCode")
      .setParameter("baseEntityCode", code).getResultList();
//VERY UGLY (ACC)
  for (Object[] objectArray : results) {
  		Attribute attribute = (Attribute)objectArray[0];
  		Double weight = (Double)objectArray[2];
  		Boolean privacyFlag = (Boolean)objectArray[1];
  		Boolean inferred = (Boolean)objectArray[3];
  		Object value = null;
  		
  		for (int i=4;i<11;i++) {
  			if (objectArray[i] == null) continue;
  			value = objectArray[i];
  			break;
  		}
   	EntityAttribute ea = new EntityAttribute(be,attribute,weight,value);
   	ea.setInferred(inferred);
   	ea.setPrivacyFlag(privacyFlag);
  		ret.add(ea);
  }
  return ret;
  }

  public List<BaseEntity> findBaseEntitysByAttributeValues(
      final MultivaluedMap<String, String> params, final boolean includeAttributes,
      final Integer pageStart, final Integer pageSize) {

    final List<BaseEntity> eeResults;
    new HashMap<String, BaseEntity>();

    if (includeAttributes) {

      // ugly and insecure
      final Integer pairCount = params.size();
      if (pairCount.equals(0)) {
        eeResults = getEntityManager()
            .createQuery("SELECT distinct be FROM BaseEntity be JOIN be.baseEntityAttributes bee ") // add
            // company
            // limiter
            .setFirstResult(pageStart).setMaxResults(pageSize).getResultList();

      } else {
        System.out.println("PAIR COUNT IS NOT ZERO " + pairCount);
        String eaStrings = "";
        String eaStringsQ = "(";
        for (int i = 0; i < (pairCount); i++) {

          eaStrings += ",EntityAttribute ea" + i;
          eaStringsQ += "ea" + i + ".baseEntityCode=be.code or ";
        }
        eaStringsQ = eaStringsQ.substring(0, eaStringsQ.length() - 4);
        eaStringsQ += ")";

        String queryStr = "SELECT distinct be FROM BaseEntity be" + eaStrings
            + "  JOIN be.baseEntityAttributes bee where " + eaStringsQ + " and  ";
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
        final Query query = getEntityManager().createQuery(queryStr);
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

        query.setFirstResult(pageStart).setMaxResults(pageSize);
        eeResults = query.getResultList();

      }
    } else {
      Log.info("**************** ENTITY ENTITY WITH NO ATTRIBUTES ****************");

      eeResults = getEntityManager().createQuery("SELECT be FROM BaseEntity be ")
          .setFirstResult(pageStart).setMaxResults(pageSize).getResultList();

    }
    // TODO: improve

    return eeResults;
  }

  public Long findBaseEntitysByAttributeValuesCount(final MultivaluedMap<String, String> params) {

    final Long result;
    new HashMap<String, BaseEntity>();

    Log.info(
        "**************** COUNT BE SEARCH WITH ATTRIBUTE VALUE WITH ATTRIBUTES!!  ****************");

    // ugly and insecure
    final Integer pairCount = params.size();
    if (pairCount.equals(0)) {
      result = (Long) getEntityManager()
          .createQuery("SELECT count(be.code) FROM BaseEntity be JOIN be.baseEntityAttributes bee")
          .getSingleResult();
    } else {
      String queryStr =
          "SELECT count(be.code) FROM BaseEntity be JOIN be.baseEntityAttributes bee where  ";
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
      final Query query = getEntityManager().createQuery(queryStr);
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
      result = (Long) query.getSingleResult();

    }

    return result;

  }

  public List<BaseEntity> findDescendantsByAttributeLink(@NotNull final String sourceCode,
      final String linkCode, final boolean includeAttributes, final Integer pageStart,
      final Integer pageSize) {

    final List<BaseEntity> eeResults;
    final Map<String, BaseEntity> beMap = new HashMap<String, BaseEntity>();

    if (includeAttributes) {
      Log.info("**************** ENTITY ENTITY DESCENDANTS WITH ATTRIBUTES!! pageStart = "
          + pageStart + " pageSize=" + pageSize + " ****************");

      eeResults = getEntityManager().createQuery(
          "SELECT be FROM BaseEntity be,EntityEntity ee JOIN be.baseEntityAttributes bee where ee.pk.targetCode=be.code and ee.pk.attribute.code=:linkAttributeCode and ee.pk.source.code=:sourceCode")
          .setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode)
          .setFirstResult(pageStart).setMaxResults(pageSize).getResultList();
      if (eeResults.isEmpty()) {
        System.out.println("EEE IS EMPTY");
      } else {
        System.out.println("EEE Count" + eeResults.size());
        System.out.println("EEE" + eeResults);
      }
      return eeResults;

    } else {
      Log.info("**************** ENTITY ENTITY WITH NO ATTRIBUTES ****************");

      eeResults = getEntityManager().createQuery(
          "SELECT be FROM BaseEntity be,EntityEntity ee where ee.pk.targetCode=be.code and ee.pk.attribute.code=:linkAttributeCode and ee.pk.source.code=:sourceCode")
          .setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode)
          .setFirstResult(pageStart).setMaxResults(pageSize).getResultList();

      for (final BaseEntity be : eeResults) {
        beMap.put(be.getCode(), be);
        Log.info("BECODE:" + be.getCode());
      }
    }
    // TODO: improve

    return beMap.values().stream().collect(Collectors.toList());
  }

  public List<Link> findLinks(@NotNull final String targetCode, final String linkCode) {

    final List<Link> eeResults;
    eeResults = getEntityManager().createQuery(
        "SELECT ee.link FROM EntityEntity ee where  ee.pk.targetCode=:targetCode and ee.pk.attribute.code=:linkAttributeCode ")
        .setParameter("targetCode", targetCode).setParameter("linkAttributeCode", linkCode)
        .getResultList();

    return eeResults;
  }

  public List<Link> findChildLinks(@NotNull final String sourceCode, final String linkCode) {

    final List<Link> eeResults;
    eeResults = getEntityManager().createQuery(
        "SELECT ee.link FROM EntityEntity ee where  ee.pk.source.code=:sourceCode and ee.pk.attribute.code=:linkAttributeCode ")
        .setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode)
        .getResultList();

    return eeResults;
  }

  public Link findLink(final String sourceCode, final String targetCode, final String linkCode)
      throws NoResultException {
    Link ee = null;

    try {
      ee = (Link) getEntityManager().createQuery(
          "SELECT ee.link FROM EntityEntity ee where ee.pk.targetCode=:targetCode and ee.pk.attribute.code=:linkAttributeCode and ee.pk.source.code=:sourceCode")
          .setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode)
          .setParameter("targetCode", targetCode).getSingleResult();

    } catch (Exception e) {
      // log.error("EntityEntity " + sourceCode + ":" + targetCode + ":" + linkCode +
      // " not found");
      throw new NoResultException(
          "Link " + sourceCode + ":" + targetCode + ":" + linkCode + " not found");
    }
    return ee;
  }

  public EntityEntity findEntityEntity(final String sourceCode, final String targetCode,
      final String linkCode) throws NoResultException {
    EntityEntity ee = null;

    try {
      ee = (EntityEntity) getEntityManager().createQuery(
          "SELECT ee FROM EntityEntity ee where ee.pk.targetCode=:targetCode and ee.pk.attribute.code=:linkAttributeCode and ee.pk.source.code=:sourceCode")
          .setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode)
          .setParameter("targetCode", targetCode).getSingleResult();

    } catch (Exception e) {
      // log.error("EntityEntity " + sourceCode + ":" + targetCode + ":" + linkCode +
      // " not found");
      throw new NoResultException(
          "EntityEntity " + sourceCode + ":" + targetCode + ":" + linkCode + " not found");
    }
    return ee;
  }


  @Transactional
  public void removeEntityEntity(final EntityEntity ee) {
    try {
      BaseEntity source = findBaseEntityByCode(ee.getLink().getSourceCode());
      source.getLinks().remove(ee);
      getEntityManager().merge(source);
      getEntityManager().remove(ee);

    } catch (Exception e) {
      // rollback
    }
  }
  
  public void removeEntityAttribute(final String baseEntityCode, final String attributeCode)
  {
	  BaseEntity be = this.findBaseEntityByCode(baseEntityCode);
	  List<EntityAttribute> results = getEntityManager()
	      .createQuery(
	          "SELECT ea FROM EntityAttribute ea where ea.pk.baseEntity.code=:baseEntityCode and ea.attributeCode=:attributeCode")
	      .setParameter("baseEntityCode", baseEntityCode)
	      .setParameter("attributeCode", attributeCode)
	      .getResultList();
	  
	  for (EntityAttribute ea: results) {
		  removeEntityAttribute(ea);
	  }

  }
  
  @Transactional
  public void removeEntityAttribute(final EntityAttribute ea) {
    try {
      BaseEntity source = findBaseEntityByCode(ea.getBaseEntityCode());
      source.getBaseEntityAttributes().remove(ea);
      getEntityManager().merge(source);
      getEntityManager().remove(ea);

    } catch (Exception e) {
      // rollback
    }
  } 

  @Transactional
  public EntityEntity addLink(final String sourceCode, final String targetCode,
      final String linkCode, Object value, Double weight)
      throws IllegalArgumentException, BadDataException {
    EntityEntity ee = null;
    Link link = null;

    try {
      link = findLink(sourceCode, targetCode, linkCode);

    } catch (NoResultException e) {
      BaseEntity beSource = null;
      BaseEntity beTarget = null;
      AttributeLink linkAttribute = null;

      try {
        beSource = findBaseEntityByCode(sourceCode);
      } catch (NoResultException es) {
        throw new IllegalArgumentException("sourceCode" + sourceCode + " not found");
      }

      try {
        beTarget = findBaseEntityByCode(targetCode);
      } catch (NoResultException es) {
        throw new IllegalArgumentException("targetCode" + targetCode + " not found");
      }

      try {
        linkAttribute = findAttributeLinkByCode(linkCode);
      } catch (NoResultException es) {
        throw new IllegalArgumentException("linkCode" + linkCode + " not found");
      }

      ee = beSource.addTarget(beTarget, linkAttribute, weight,value);
      beSource = getEntityManager().merge(beSource);

    }
    return ee;
  }

  public void removeLink(final Link link) {
    EntityEntity ee = null;

    try {
      ee = findEntityEntity(link.getSourceCode(), link.getTargetCode(), link.getAttributeCode());
      removeEntityEntity(ee);
    } catch (Exception e) {
      log.error("EntityEntity " + link + " not found");
    }
  }

  
  public void removeLink(final String sourceCode, final String targetCode, final String linkCode) {
    EntityEntity ee = null;

    try {
      ee = findEntityEntity(sourceCode, targetCode, linkCode);
      removeEntityEntity(ee);
    } catch (Exception e) {
      log.error("EntityEntity " + sourceCode + ":" + targetCode + ":" + linkCode + " not found");
    }
  }

  public void importBaseEntitys(final InputStream in, final String filename) {
    // import csv
    String line = "";
    final String cvsSplitBy = ",";
    boolean headerLine = true;
    final Map<Integer, Attribute> attributes = new HashMap<Integer, Attribute>();

    try (BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
      int rowNumber = 0;
      while ((line = br.readLine()) != null) {

        // use comma as separator
        final String[] columns = line.split(cvsSplitBy);
        if (headerLine) {
          headerLine = false;
          for (int i = 0; i < columns.length; i++) {
            final String[] code_name = columns[i].split(":");

            Attribute attribute = findAttributeByCode(code_name[0]);
            if (attribute == null) {
              attribute = new AttributeText(code_name[0], code_name[1]);
              getEntityManager().persist(attribute);
            }

            attributes.put(i, attribute);
          }
        } else {
          BaseEntity entity = null;
          final String code = filename + "-" + rowNumber;
          if (filename.toUpperCase().contains("PERSON")) {
            entity = new Person(code);
          } else if (filename.toUpperCase().contains("COMPANY")) {
            entity = new Company(code, "Import");
          } else if (filename.toUpperCase().contains("PRODUCT")) {
            entity = new Product(code, "Import");
          } else {
            entity = new BaseEntity(code);
          }

          for (int i = 0; i < columns.length; i++) {
            // determine if it is a person, company or product else baseentity

            final Attribute attribute = attributes.get(i);
            if (attribute.getCode().equalsIgnoreCase("NAME")) {
              entity.setName(columns[i]);
            }
            if (attribute.getCode().equalsIgnoreCase("CODE")) {
              entity.setCode(columns[i]);
            }
            try {
              entity.addAttribute(attribute, 1.0, columns[i]);
            } catch (final BadDataException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }

          }
          if (entity instanceof Person) {
            if (!entity.containsEntityAttribute("NAME")) {
              // get first
              final Optional<EntityAttribute> firstname =
                  entity.findEntityAttribute("PRI_FIRSTNAME");
              final Optional<EntityAttribute> lastname = entity.findEntityAttribute("PRI_LASTNAME");

              String name = "";
              if (firstname.isPresent()) {
                name += firstname.get().getValueString() + " ";
              }
              if (lastname.isPresent()) {
                name += lastname.get().getValueString() + " ";
              }
              Attribute nameAttribute = findAttributeByCode("PRI_NAME");
              if (nameAttribute == null) {
                nameAttribute = new AttributeText("PRI_NAME", "Name");
                getEntityManager().persist(nameAttribute);

              }
              try {
                entity.addAttribute(nameAttribute, 1.0, name);

              } catch (final BadDataException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
              }

            }
          }
          // Now check if not already there by comparing specific fields
          getEntityManager().persist(entity);
        }
        rowNumber++;
      }

    } catch (final IOException e) {
      e.printStackTrace();
    }

  }

  public Long importKeycloakUsers(final String keycloakUrl, final String realm,
      final String username, final String password, final String clientId,
      final Integer maxReturned, final String parentGroupCodes) {
    Long count = 0L;

    AttributeLink linkAttribute = this.findAttributeLinkByCode("LNK_CORE");
    List<BaseEntity> parentGroupList = new ArrayList<BaseEntity>();

    String[] parentCodes = parentGroupCodes.split(",");
    for (String parentCode : parentCodes) {
      BaseEntity group = null;

      try {
        group = this.findBaseEntityByCode(parentCode); // careful as GRPUSERS needs to
        parentGroupList.add(group);
      } catch (NoResultException e) {
        System.out.println("Group Code does not exist :" + parentCode);
      }

    }

    KeycloakService ks;
    final Map<String, Map<String, Object>> usersMap = new HashMap<String, Map<String, Object>>();

    try {
      ks = new KeycloakService(keycloakUrl, realm, username, password, clientId);
      final List<LinkedHashMap> users = ks.fetchKeycloakUsers(maxReturned);
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

    for (final String kcusername : usersMap.keySet()) {
      final MultivaluedMap params = new MultivaluedMapImpl();
      params.add("PRI_USERNAME", kcusername);
      final Map<String, Object> userMap = usersMap.get(kcusername);

      final List<BaseEntity> users = findBaseEntitysByAttributeValues(params, true, 0, 1);
      if (users.isEmpty()) {
        final String code = "PER_CH40_" + kcusername.toUpperCase().replaceAll("\\ ", "")
            .replaceAll("\\.", "").replaceAll("\\&", "");
        System.out.println("New User Code = " + code);
        String firstName = (String) userMap.get("firstName");
        firstName = firstName.replaceAll("\\.", " "); // replace dots
        firstName = firstName.replaceAll("\\_", " "); // replace dots
        String lastName = (String) userMap.get("lastName");
        lastName = lastName.replaceAll("\\.", " "); // replace dots
        lastName = lastName.replaceAll("\\_", " "); // replace dots
        String name = firstName + " " + lastName;

        final String email = (String) userMap.get("email");
        final String id = (String) userMap.get("id");
        final Long unixSeconds = (Long) userMap.get("createdTimestamp");
        final Date date = new Date(unixSeconds); // *1000 is to convert seconds to milliseconds
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z"); // the format of
                                                                                    // your date
        sdf.setTimeZone(TimeZone.getTimeZone("GMT+10")); // give a timezone reference for formating
        sdf.format(date);
        final Attribute firstNameAtt = findAttributeByCode("PRI_FIRSTNAME");
        final Attribute lastNameAtt = findAttributeByCode("PRI_LASTNAME");
        final Attribute nameAtt = findAttributeByCode("PRI_NAME");
        final Attribute emailAtt = findAttributeByCode("PRI_EMAIL");
        final Attribute uuidAtt = findAttributeByCode("PRI_UUID");
        final Attribute usernameAtt = findAttributeByCode("PRI_USERNAME");

        try {
          final BaseEntity user = new BaseEntity(code, name);

          user.addAttribute(firstNameAtt, 0.0, firstName);
          user.addAttribute(lastNameAtt, 0.0, lastName);
          user.addAttribute(nameAtt, 0.0, name);
          user.addAttribute(emailAtt, 0.0, email);
          user.addAttribute(uuidAtt, 0.0, id);
          user.addAttribute(usernameAtt, 0.0, kcusername);
          insert(user);

          // Now link to groups
          for (final BaseEntity parent : parentGroupList) {
            if (!parent.containsTarget(user.getCode(), linkAttribute.getCode())) {
              parent.addTarget(user, linkAttribute, 1.0);
            }
          }
          count++;
          System.out.println("BE:" + user);
        } catch (final BadDataException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }

      } else {
        users.get(0);
      }

    }

    // now save the parents
    for (BaseEntity parent : parentGroupList) {
      parent = getEntityManager().merge(parent);
    }

    return count;
  }

  /**
   * init
   */
  public void init(final KeycloakSecurityContext kContext) {
    // Entities
    if (kContext == null) {
      System.out.println("Null Keycloak Context");
      return;
    }

    final BaseEntity be = new BaseEntity("Test BaseEntity");
    be.setCode(BaseEntity.getDefaultCodePrefix() + "TEST");
    getEntityManager().persist(be);

    Person edison = new Person("Thomas Edison");
    edison.setCode(Person.getDefaultCodePrefix() + "EDISON");
    getEntityManager().persist(edison);

    final Person tesla = new Person("Nikola Tesla");
    tesla.setCode(Person.getDefaultCodePrefix() + "TESLA");
    getEntityManager().persist(tesla);

    final Company crowtech = new Company("crowtech", "Crowtech Pty Ltd");
    crowtech.setCode(Company.getDefaultCodePrefix() + "CROWTECH");
    getEntityManager().persist(crowtech);

    final Company spacex = new Company("spacex", "SpaceX");
    spacex.setCode(Company.getDefaultCodePrefix() + "SPACEX");
    getEntityManager().persist(spacex);

    final Product bmw316i = new Product("bmw316i", "BMW 316i");
    bmw316i.setCode(Product.getDefaultCodePrefix() + "BMW316I");
    getEntityManager().persist(bmw316i);

    final Product mazdaCX5 = new Product("maxdacx5", "Mazda CX-5");
    mazdaCX5.setCode(Product.getDefaultCodePrefix() + "MAXDACX5");
    getEntityManager().persist(mazdaCX5);

    final AttributeText attributeText1 =
        new AttributeText(AttributeText.getDefaultCodePrefix() + "TEST1", "Test 1");
    getEntityManager().persist(attributeText1);
    final AttributeText attributeText2 =
        new AttributeText(AttributeText.getDefaultCodePrefix() + "TEST2", "Test 2");
    getEntityManager().persist(attributeText2);
    final AttributeText attributeText3 =
        new AttributeText(AttributeText.getDefaultCodePrefix() + "TEST3", "Test 3");
    getEntityManager().persist(attributeText3);

    Person person = new Person("Barry Allen");
    person.setCode(Person.getDefaultCodePrefix() + "FLASH");
    getEntityManager().persist(person);

    try {
      person.addAttribute(attributeText1, 1.0);
      person.addAttribute(attributeText2, 0.8);
      person.addAttribute(attributeText3, 0.6, 3147);

      // Link some BaseEntities
      final AttributeText link1 =
          new AttributeText(AttributeText.getDefaultCodePrefix() + "LINK1", "Link1");
      getEntityManager().persist(link1);
      person.addTarget(bmw316i, link1, 1.0);
      person.addTarget(mazdaCX5, link1, 0.9);
      person.addTarget(edison, link1, 0.8);
      person.addTarget(tesla, link1, 0.7);
      edison.addTarget(spacex, link1, 0.5);
      edison.addTarget(crowtech, link1, 0.4);

      person = getEntityManager().merge(person);
      edison = getEntityManager().merge(edison);

    } catch (final BadDataException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public void importKeycloakUsers(final List<Group> parentGroupList,
      final AttributeLink linkAttribute, final Integer maxReturned)
      throws IOException, BadDataException {

    final Map<String, String> envParams = System.getenv();
    String keycloakUrl = envParams.get("KEYCLOAKURL");
    System.out.println("Keycloak URL=[" + keycloakUrl + "]");
    keycloakUrl = keycloakUrl.replaceAll("'", "");
    final String realm = envParams.get("KEYCLOAK_REALM");
    final String username = envParams.get("KEYCLOAK_USERNAME");
    final String password = envParams.get("KEYCLOAK_PASSWORD");
    final String clientid = envParams.get("KEYCLOAK_CLIENTID");
    final String secret = envParams.get("KEYCLOAK_SECRET");

    System.out.println("Realm is :[" + realm + "]");

    final KeycloakService kcs =
        new KeycloakService(keycloakUrl, realm, username, password, clientid, secret);
    final List<LinkedHashMap> users = kcs.fetchKeycloakUsers(maxReturned);
    for (final LinkedHashMap user : users) {
      final String name = user.get("firstName") + " " + user.get("lastName");
      final Person newUser = new Person(name);
      final String keycloakUUID = (String) user.get("id");
      newUser.setCode(Person.getDefaultCodePrefix() + keycloakUUID.toUpperCase());
      newUser.setName(name);
      newUser.addAttribute(createAttributeText("NAME"), 1.0, name);
      newUser.addAttribute(createAttributeText("FIRSTNAME"), 1.0, user.get("firstName"));
      newUser.addAttribute(createAttributeText("LASTNAME"), 1.0, user.get("lastName"));
      newUser.addAttribute(createAttributeText("UUID"), 1.0, user.get("id"));
      newUser.addAttribute(createAttributeText("EMAIL"), 1.0, user.get("email"));
      newUser.addAttribute(createAttributeText("USERNAME"), 1.0, user.get("username"));
      System.out.println("Code=" + newUser.getCode());;
      insert(newUser);
      // Now link to groups
      for (final Group parent : parentGroupList) {
        if (!parent.containsTarget(newUser.getCode(), linkAttribute.getCode())) {
          parent.addTarget(newUser, linkAttribute, 1.0);

        }
      }
    }
    // now save the parents
    for (Group parent : parentGroupList) {
      parent = getEntityManager().merge(parent);
    }
    System.out.println(users);
  }

  public Link moveLink(final String originalSourceCode, final String targetCode,
      final String linkCode, final String destinationSourceCode) throws IllegalArgumentException {
    Link ee = null;

    try {
      // getEntityManager().getTransaction().begin();

      // EntityEntity oldLink = findEntityEntity(originalSourceCode, targetCode, linkCode);
      Link oldLink = findLink(originalSourceCode, targetCode, linkCode);
      // add new link
      EntityEntity eee = addLink(destinationSourceCode, targetCode, linkCode,
          oldLink.getLinkValue(), oldLink.getWeight());
      // EntityEntity eee = addLink(destinationSourceCode, targetCode, linkCode, oldLink.getValue(),
      // oldLink.getWeight());

      // remove old one
      // removeEntityEntity(oldLink);
      removeLink(oldLink);
      ee = eee.getLink();
      // getEntityManager().getTransaction().commit();
    } catch (Exception e) {
      throw new IllegalArgumentException("linkCode" + linkCode + " not found");
    }
    return ee;
  }

  // public Long insert(Ask ask)
  // {
  // // always check if ask exists through check for source, target, and question,
  // and created
  // datetime
  // try {
  // getEntityManager().persist(ask);
  //
  // } catch (EntityExistsException e) {
  // // so update otherwise // TODO merge?
  // BaseEntity existing = findBaseEntityByCode(entity.getCode());
  // List<EntityAttribute> changes = existing.merge(entity);
  // System.out.println("Updated "+existing+ ":"+ changes);
  // existing = getEntityManager().merge(existing);
  // return existing.getId();
  //
  // }
  // return entity.getId();
  // }

  // public Long update(BaseEntity entity) {
  // // always check if baseentity exists through check for unique code
  // try {
  // // so persist otherwise
  // getEntityManager().persist(entity);
  // } catch (ConstraintViolationException e) {
  // entity = getEntityManager().merge(entity);
  // return entity.getId();
  // } catch (PersistenceException e) {
  // entity = getEntityManager().merge(entity);
  // return entity.getId();
  // } catch (EJBException e) {
  // entity = getEntityManager().merge(entity);
  // return entity.getId();
  // } catch (IllegalStateException e) {
  // entity = getEntityManager().merge(entity);
  // return entity.getId();
  // }
  // return entity.getId();
  // }

  public BaseEntity getUser() {
    return null;
  }

  public Long insert(final QBaseMSGMessageTemplate template) {
    try {
      getEntityManager().persist(template);

    } catch (final EntityExistsException e) {
      e.printStackTrace();

      /*
       * QBaseMSGMessageTemplate existing = findRuleById(template.getId()); existing =
       * getEntityManager().merge(existing); return existing.getId();
       */

    }
    return template.getId();
  }

  public QBaseMSGMessageTemplate findTemplateByCode(@NotNull final String templateCode)
      throws NoResultException {

    QBaseMSGMessageTemplate result = null;

    result = (QBaseMSGMessageTemplate) getEntityManager()
        .createQuery("SELECT temp FROM QBaseMSGMessageTemplate temp where temp.code=:templateCode")
        .setParameter("templateCode", templateCode.toUpperCase()).getSingleResult();


    return result;

  }

  protected String getRealm() {
    return DEFAULT_REALM;
  }
}
