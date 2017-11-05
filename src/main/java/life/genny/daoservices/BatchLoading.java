package life.genny.daoservices;

import static java.lang.System.out;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.PersistenceException;
import org.hibernate.exception.ConstraintViolationException;
import life.genny.qwanda.Ask;
import life.genny.qwanda.Question;
import life.genny.qwanda.attribute.Attribute;
import life.genny.qwanda.attribute.EntityAttribute;
import life.genny.qwanda.entity.BaseEntity;
import life.genny.qwanda.validation.Validation;

public class BatchLoading {

  EntityManager em;

  public BatchLoading(final EntityManager em) {
    this.em = em;
  }
  public Ask findAskById(final Long id) {
    return em.find(Ask.class, id);
  }
  public Long insert(final Ask ask) {
    // always check if question exists through check for unique code
    try {
      // check that these bes exist
      this.findBaseEntityByCode(ask.getSourceCode());
      this.findBaseEntityByCode(ask.getTargetCode());
      this.findAttributeByCode(ask.getAttributeCode());

      // if (ask.getQuestion() == null) {
      // Question question = this.findQuestionByCode(ask.getQuestionCode());
      // ask.setQuestion(question);
      // }
      em.persist(ask);
      // baseEntityEventSrc.fire(entity);
    } catch (final ConstraintViolationException e) {
      // so update otherwise // TODO merge?
      Ask existing = findAskById(ask.getId());
      existing = em.merge(existing);
      return existing.getId();
    } catch (final PersistenceException e) {
      // so update otherwise // TODO merge?
      Ask existing = findAskById(ask.getId());
      existing = em.merge(existing);
      return existing.getId();
    }  catch (final IllegalStateException e) {
      // so update otherwise // TODO merge?
      Ask existing = findAskById(ask.getId());
      existing = em.merge(existing);
      return existing.getId();
    }
    return ask.getId();
  }
  public Validation findValidationByCode(final String code) throws NoResultException {
    final Validation result =
        (Validation) em.createQuery("SELECT a FROM Validation a where a.code=:code")
            .setParameter("code", code).getSingleResult();
    return result;
  }

  public Long insert(final Validation validation) {
    em.persist(validation);
    return validation.getId();
  }

  public Validation upsert(Validation validation) {
    try {
      String code = validation.getCode();
      Validation val = findValidationByCode(code);
      BeanNotNullFields copyFields = new BeanNotNullFields();
      copyFields.copyProperties(val, validation);
      val = em.merge(val);
//      BeanUtils.copyProperties(validation, val);
      return val;
    } catch (NoResultException | IllegalAccessException | InvocationTargetException e) {
      em.persist(validation);
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
      val = em.merge(val);
//      BeanUtils.copyProperties(attr, val);
      return val;
    } catch (NoResultException | IllegalAccessException | InvocationTargetException e) {
      em.persist(attr);
      Long id = attr.getId();
      return attr;
    } 
  }

  public Attribute findAttributeByCode(final String code) throws NoResultException {
    final Attribute result =
        (Attribute) em.createQuery("SELECT a FROM Attribute a where a.code=:code")
            .setParameter("code", code.toUpperCase()).getSingleResult();
    return result;
  }

  public Long insert(final Attribute attribute) {
    em.persist(attribute);
    return attribute.getId();
  }

  public Long upsert(final BaseEntity be, Set<EntityAttribute> ba) {
    try {
//      be.setBaseEntityAttributes(null);
      out.println("****3*****"+be.getBaseEntityAttributes().stream().map(data->data.pk).reduce((d1,d2)->d1).get());
      String code = be.getCode();
      final BaseEntity val = findBaseEntityByCode(code);
      BeanNotNullFields copyFields = new BeanNotNullFields();
      // copyFields.copyProperties(val, be);
      em.merge(val);
      return val.getId();
    } catch (NoResultException e) {
      Long id = insert(be);
      return id;
    }
    
  }

  public BaseEntity findBaseEntityByCode(final String baseEntityCode) throws NoResultException {
    final BaseEntity result =
        (BaseEntity) em.createQuery("SELECT a FROM BaseEntity a where a.code=:baseEntityCode")
            .setParameter("baseEntityCode", baseEntityCode.toUpperCase()).getSingleResult();
    final List<EntityAttribute> attributes = em
        .createQuery(
            "SELECT ea FROM EntityAttribute ea where ea.pk.baseEntity.code=:baseEntityCode")
        .setParameter("baseEntityCode", baseEntityCode).getResultList();
    result.setBaseEntityAttributes(new HashSet<EntityAttribute>(attributes));
    return result;
  }

  public Long insert(final BaseEntity entity) {
    em.persist(entity);
    return entity.getId();
  }
  public Question findQuestionByCode(final String code) throws NoResultException {

    final Question result = (Question) em
        .createQuery("SELECT a FROM Question a where a.code=:code")
        .setParameter("code", code.toUpperCase()).getSingleResult();

    return result;
  }
  public Long insert(final Question question) {
    // always check if question exists through check for unique code
    try {
      em.persist(question);
      System.out.println("\n\n\n\n\n\n\n11111111\n\n\n\n\n\n\n\n");
      // baseEntityEventSrc.fire(entity);
    } catch (final ConstraintViolationException e) {
      Question existing = findQuestionByCode(question.getCode());
      existing = em.merge(existing);
      return existing.getId();
    } catch (final PersistenceException e) {
      Question existing = findQuestionByCode(question.getCode());
      existing = em.merge(existing);
      return existing.getId();
    }  catch (final IllegalStateException e) {
      Question existing = findQuestionByCode(question.getCode());
      existing = em.merge(existing);
      return existing.getId();
    }
    return question.getId();
  }

  public BaseEntity upsert(BaseEntity be) {
    try {
      String code = be.getCode();
      BaseEntity val = findBaseEntityByCode(code);
      
      BeanNotNullFields copyFields = new BeanNotNullFields();
      copyFields.copyProperties(val, be);
      System.out.println("***********"+val);
      val = em.merge(val);
      System.out.println("*******&&&&&&&&&&&&****");
      return be;
    } catch (NoResultException | IllegalAccessException | InvocationTargetException e) {
      Long id = insert(be);
      return be;
    }
  
    
  }
}
