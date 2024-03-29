package life.genny.services;

import static java.lang.System.out;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.UUID;

import javax.persistence.EntityExistsException;
import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Fetch;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.JoinType;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import javax.validation.constraints.NotNull;
import javax.ws.rs.core.MultivaluedMap;

import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.core.types.dsl.ComparableExpressionBase;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.StringExpression;
import com.querydsl.core.types.dsl.NumberExpression;
import com.querydsl.core.types.dsl.LiteralExpression;
import com.querydsl.core.types.dsl.NumberPath;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.core.types.dsl.BooleanPath;
import com.querydsl.core.types.dsl.DatePath;
import com.querydsl.core.types.dsl.DateTimePath;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.sql.JPASQLQuery;
import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.Path;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.JPQLQuery;
// import com.querydsl.sql.DerbyTemplates;
// import com.querydsl.sql.SQLTemplates;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.hibernate.Criteria;
import org.hibernate.Filter;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.jpa.QueryHints;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.keycloak.KeycloakSecurityContext;
import org.mortbay.log.Log;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import life.genny.qwanda.Answer;
import life.genny.qwanda.AnswerLink;
import life.genny.qwanda.Ask;
import life.genny.qwanda.Context;
import life.genny.qwanda.ContextList;
import life.genny.qwanda.GPS;
import life.genny.qwanda.Layout;
import life.genny.qwanda.Link;
import life.genny.qwanda.Question;
import life.genny.qwanda.QuestionQuestion;
import life.genny.qwanda.QuestionSourceTarget;
import life.genny.qwanda.EEntityStatus;
import life.genny.qwanda.attribute.Attribute;
import life.genny.qwanda.attribute.AttributeBoolean;
import life.genny.qwanda.attribute.AttributeDate;
import life.genny.qwanda.attribute.AttributeDateTime;
import life.genny.qwanda.attribute.AttributeDouble;
import life.genny.qwanda.attribute.AttributeInteger;
import life.genny.qwanda.attribute.AttributeLink;
import life.genny.qwanda.attribute.AttributeLong;
import life.genny.qwanda.attribute.AttributeMoney;
import life.genny.qwanda.attribute.AttributeText;
import life.genny.qwanda.attribute.AttributeTime;
import life.genny.qwanda.attribute.EntityAttribute;
import life.genny.qwanda.attribute.QEntityAttribute;
import life.genny.qwanda.datatype.DataType;
import life.genny.qwanda.entity.BaseEntity;
import life.genny.qwanda.entity.QBaseEntity;
import life.genny.qwanda.entity.Company;
import life.genny.qwanda.entity.EntityEntity;
import life.genny.qwanda.entity.QEntityEntity;
import life.genny.qwanda.entity.Group;
import life.genny.qwanda.entity.Person;
import life.genny.qwanda.entity.Product;
import life.genny.qwanda.entity.SearchEntity;
import life.genny.qwanda.exception.BadDataException;
import life.genny.qwanda.message.QBaseMSGMessageTemplate;
import life.genny.qwanda.message.QDataSubLayoutMessage;
import life.genny.qwanda.message.QEventAttributeValueChangeMessage;
import life.genny.qwanda.message.QEventLinkChangeMessage;
import life.genny.qwanda.message.QSearchBeResult;
import life.genny.qwanda.message.QSearchEntityMessage;
import life.genny.qwanda.rule.Rule;
import life.genny.qwanda.validation.Validation;
import life.genny.qwandautils.JsonUtils;
import life.genny.qwandautils.MergeUtil;
import life.genny.qwandautils.GennySettings;
import life.genny.utils.VertxUtils;
import life.genny.utils.BaseEntityUtils;
import life.genny.utils.RulesUtils;
import life.genny.models.GennyToken;
import life.genny.qwandautils.BeanNotNullFields;
import life.genny.qwandautils.ANSIColour;


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

	public static final String REALM_HIDDEN = "hidden";

	Map<String, String> ddtCacheMock = new ConcurrentHashMap<>();

	EntityManager em;

	List<String> allowedConditions = Arrays.asList("=", "<", ">", "<=", ">=", "LIKE", "!=", "<>", "&+", "&0");
	List<String> allowedLinkWeightConditions = Arrays.asList("=", "<", ">", "<=", ">=");

	Set<String> realms = new HashSet<>(Arrays.asList("genny", "hidden"));
	String realmsStr = getRealmsStr(realms);
	
	ValidatorFactory factory = javax.validation.Validation.buildDefaultValidatorFactory();
	Validator validator = factory.getValidator();


	public List<EntityAttribute> findBySearchEA22(@NotNull final String hql, final Integer pageStart, final Integer pageSize) {
		List<EntityAttribute> results = null;

		Query query = null;
	//	Set<String> attributeCodes = new HashSet<>(Arrays.asList("PRI_NAME"));
	//	Filter filter = getEntityManager().unwrap(Session.class).enableFilter("filterAttribute");
	//	filter.setParameterList("attributeCodes", attributeCodes);
		query = getEntityManager().createQuery(hql);
		query.setFirstResult(pageStart).setMaxResults(pageSize);

		results = query.setHint(QueryHints.HINT_READONLY, true).getResultList();
		log.info("RESULTS=" + results);
		return results;
	}
	
	
	public QSearchBeResult  findBySearchEA24(@NotNull final String hql, final Integer pageStart, final Integer pageSize) {
		QSearchBeResult result = null;

		Query query = null;
		List<String> codes = new ArrayList<String>();
	//	Set<String> attributeCodes = new HashSet<>(Arrays.asList("PRI_NAME"));
	//	Filter filter = getEntityManager().unwrap(Session.class).enableFilter("filterAttribute");
	//	filter.setParameterList("attributeCodes", attributeCodes);
		query = getEntityManager().createQuery(hql);
		query.setFirstResult(pageStart).setMaxResults(pageSize);

		codes = query.setHint(QueryHints.HINT_READONLY, true).getResultList();

//		List<Object[]> results = query.setHint(QueryHints.HINT_READONLY, true).getResultList();
//		for (Object[] row : results) {
//			codes.add((String)row[0]);
//		}
		//log.info("RESULTS=" + results);
		
		// now update the hql to look for count
		
		Integer fromIndex = hql.indexOf("from");
		String hqlGuts = hql.substring(fromIndex);
		String hqlStart = hql.substring(0,fromIndex);
		
		String[] afterSelect = hqlStart.split(" ", 2);
		String hqlCount = "select count ("+afterSelect[1]+") ";
		//hqlCount = hqlCount.replaceAll(",ed.valueString",""); // TODO, ugly
		String hql3 = hqlCount + hqlGuts;
	
		query = getEntityManager().createQuery(hql3);
		Long count = (Long)query.getSingleResult();
		
		result = new QSearchBeResult(codes,count);
		return result;
	}

	/**
	 * Perform a safe search using named parameters to
	 * protect from SQL Injection
	*/
	public QSearchBeResult findBySearch25(String passedToken, String realm, @NotNull final SearchEntity searchBE, Boolean countOnly, Boolean fetchEntities) {
		// Init necessary vars
		QSearchBeResult result = null;
		List<String> codes = new ArrayList<String>();
		// Get page start and page size from SBE
		Integer pageStart = searchBE.getPageStart(0);
		Integer pageSize = searchBE.getPageSize(GennySettings.defaultPageSize);

		JPAQuery<?> query = new JPAQuery<Void>(getEntityManager());

		QBaseEntity baseEntity = new QBaseEntity("baseEntity");
		// Define a join for link searches
		String linkCode = null;
		String linkValue = null;
		String sourceCode = null;
		String targetCode = null;

		query.from(baseEntity);

		List<EntityAttribute> sortAttributes = new ArrayList<>();

		// Find AND and OR attributes and remove these prefixs from each of them
		List<EntityAttribute> andAttributes = searchBE.findPrefixEntityAttributes("AND_");
		List<EntityAttribute> orAttributes = searchBE.findPrefixEntityAttributes("OR_");

		String[] wilcardWhiteList = searchBE.findPrefixEntityAttributes("WTL_").stream().map(x -> x.getAttributeCode().substring(4)).toArray(String[]::new);
		String[] wilcardBlackList = searchBE.findPrefixEntityAttributes("BKL_").stream().map(x -> x.getAttributeCode().substring(4)).toArray(String[]::new);

		BooleanBuilder builder = new BooleanBuilder();

		// Ensure only Entities from our realm are returned
		log.info("realm is " + realm);
		builder.and(baseEntity.realm.eq(realm));

		// Default Status level is ACTIVE
		EEntityStatus status = EEntityStatus.ACTIVE;
		Integer joinCounter = 0;

		for (EntityAttribute ea : searchBE.getBaseEntityAttributes()) {

			final String attributeCode = ea.getAttributeCode();

			// Create where condition for the BE Code Filter
			if (attributeCode.equals("PRI_CODE")) {
				log.info("PRI_CODE like " + ea.getAsString());

				BooleanBuilder entityCodeBuilder = new BooleanBuilder();
				entityCodeBuilder.and(baseEntity.code.like(ea.getAsString()));

				// Process any AND/OR filters for BaseEntity Code
				andAttributes.stream().filter(x -> removePrefixFromCode(x.getAttributeCode(), "AND").equals(attributeCode)).forEach(x -> {
					log.info("AND " + attributeCode + " like " + x.getAsString());
					entityCodeBuilder.and(baseEntity.code.like(x.getAsString()));
				});
				orAttributes.stream().filter(x -> removePrefixFromCode(x.getAttributeCode(), "OR").equals(attributeCode)).forEach(x -> {
					log.info("OR " + attributeCode + " like " + x.getAsString());
					entityCodeBuilder.or(baseEntity.code.like(x.getAsString()));
				});
				builder.and(entityCodeBuilder);

			// Handle Created Date Filters
			} else if (attributeCode.startsWith("PRI_CREATED")) {
				builder.and(getDateTimePredicate(ea, baseEntity.created));
				// Process any AND/OR filters for this attribute
				andAttributes.stream().filter(x -> removePrefixFromCode(x.getAttributeCode(), "AND").equals(attributeCode)).forEach(x -> {
					builder.and(getDateTimePredicate(x, baseEntity.created));
				});
				orAttributes.stream().filter(x -> removePrefixFromCode(x.getAttributeCode(), "OR").equals(attributeCode)).forEach(x -> {
					builder.or(getDateTimePredicate(x, baseEntity.created));
				});

			// Handle Updated Date Filters
			} else if (attributeCode.startsWith("PRI_UPDATED")) {
				builder.and(getDateTimePredicate(ea, baseEntity.updated));
				// Process any AND/OR filters for this attribute
				andAttributes.stream().filter(x -> removePrefixFromCode(x.getAttributeCode(), "AND").equals(attributeCode)).forEach(x -> {
					builder.and(getDateTimePredicate(x, baseEntity.updated));
				});
				orAttributes.stream().filter(x -> removePrefixFromCode(x.getAttributeCode(), "OR").equals(attributeCode)).forEach(x -> {
					builder.or(getDateTimePredicate(x, baseEntity.updated));
				});

			// Create a Join for each attribute filters
			} else if (
					(attributeCode.startsWith("PRI_") || attributeCode.startsWith("LNK_"))
					&& !attributeCode.equals("PRI_CODE") && !attributeCode.equals("PRI_TOTAL_RESULTS")
					&& !attributeCode.startsWith("PRI_CREATED") && !attributeCode.startsWith("PRI_UPDATED")
					&& !attributeCode.equals("PRI_INDEX")
				) {

				// Generally don't accept filter LIKE "%", unless other filters present for this attribute
				Boolean isAnyStringFilter = false;
				try {
					if (ea.getValueString() != null && "%".equals(ea.getValueString()) && "LIKE".equals(ea.getAttributeName())) {
						isAnyStringFilter = true;
					}
				} catch (Exception e) {
					log.error("Bad Null ["+ea+"]"+e.getLocalizedMessage());
				}

				String filterName = "eaFilterJoin_"+joinCounter.toString();
				QEntityAttribute eaFilterJoin = new QEntityAttribute(filterName);
				joinCounter++;

				BooleanBuilder currentAttributeBuilder = new BooleanBuilder();
				BooleanBuilder extraFilterBuilder = new BooleanBuilder();

				Boolean orTrigger = false;

				String joinAttributeCode = attributeCode;
				if (attributeCode.contains(".")) {
					// Ensure we now join on this LNK attr
					joinAttributeCode = attributeCode.split("\\.")[0];
					// Create a new set of filters just for this subquery
					List<EntityAttribute> subQueryEaList = searchBE.getBaseEntityAttributes().stream().filter(x -> (
							removePrefixFromCode(x.getAttributeCode(), "AND").equals(attributeCode)
							|| removePrefixFromCode(x.getAttributeCode(), "OR").equals(attributeCode)
							)).collect(Collectors.toList());

					// Prepare for subquery by removing base attribute codes
					detatchBaseAttributeCode(subQueryEaList);
					// Must strip value to get clean code
					currentAttributeBuilder.and(
							Expressions.stringTemplate("replace({0},'[\"','')", 
								Expressions.stringTemplate("replace({0},'\"]','')", eaFilterJoin.valueString)
							) .in(generateSubQuery(subQueryEaList)));
				} else {
					currentAttributeBuilder.and(getAttributeSearchColumn(ea, eaFilterJoin));

					// Process any AND/OR filters for this attribute
					andAttributes.stream().filter(x -> removePrefixFromCode(x.getAttributeCode(), "AND").equals(attributeCode)).forEach(x -> {
						extraFilterBuilder.and(getAttributeSearchColumn(x, eaFilterJoin));
					});
					// Using Standard for-loop to allow updating trigger variable
					for (EntityAttribute x : orAttributes) {
						if (removePrefixFromCode(x.getAttributeCode(), "OR").equals(attributeCode)) {
							extraFilterBuilder.or(getAttributeSearchColumn(x, eaFilterJoin));
							orTrigger = true;
						}
					}
				}

				// This should get around the bug that occurs with filter LIKE "%"
				if (!isAnyStringFilter) {

					query.leftJoin(eaFilterJoin)
						.on(eaFilterJoin.pk.baseEntity.id.eq(baseEntity.id)
						.and(eaFilterJoin.attributeCode.eq(joinAttributeCode)));
						
					if (extraFilterBuilder.hasValue()) {
						if (orTrigger) {
							currentAttributeBuilder.or(extraFilterBuilder);
						} else {
							currentAttributeBuilder.and(extraFilterBuilder);
						}
					}
					builder.and(currentAttributeBuilder);
				}
			// Create a filter for wildcard
			} else if (attributeCode.startsWith("SCH_WILDCARD")) {
				if (ea.getValueString() != null) {
					if (!StringUtils.isBlank(ea.getValueString())) {
						String wildcardValue = ea.getValueString();
						// wildcardValue = wildcardValue.replaceAll("[^A-zA-Z0-9 .,'@()_-]", "");
						wildcardValue = "%" + wildcardValue + "%";
						log.info("WILDCARD like " + wildcardValue);

						QEntityAttribute eaWildcardJoin = new QEntityAttribute("eaWildcardJoin");
						query.leftJoin(eaWildcardJoin);

						if (wilcardWhiteList.length > 0) {
							log.info("Whitelist = " + Arrays.toString(wilcardWhiteList));
							query.on(eaWildcardJoin.pk.baseEntity.id.eq(baseEntity.id).and(eaWildcardJoin.attributeCode.in(wilcardWhiteList)));
						} else if (wilcardBlackList.length > 0) {
							log.info("Blacklist = " + Arrays.toString(wilcardBlackList));
							query.on(eaWildcardJoin.pk.baseEntity.id.eq(baseEntity.id).and(eaWildcardJoin.attributeCode.notIn(wilcardBlackList)));
						} else {
							query.on(eaWildcardJoin.pk.baseEntity.id.eq(baseEntity.id));
						}

						// Find the depth level for associated wildcards
						EntityAttribute depthLevelAttribute = searchBE.findEntityAttribute("SCH_WILDCARD_DEPTH").orElse(null);
						Integer depth = 0;
						if (depthLevelAttribute != null) {
							depth = depthLevelAttribute.getValueInteger();
						}

						if (depth != null && depth > 0) {
							builder.and(baseEntity.name.like(wildcardValue)
									.or(eaWildcardJoin.valueString.like(wildcardValue)
										.or(Expressions.stringTemplate("replace({0},'[\"','')", 
												Expressions.stringTemplate("replace({0},'\"]','')", eaWildcardJoin.valueString)
												).in(generateWildcardSubQuery(wildcardValue, depth, wilcardWhiteList, wilcardBlackList))
										   )
									   )
									);
						} else {
							builder.and(baseEntity.name.like(wildcardValue)
									.or(eaWildcardJoin.valueString.like(wildcardValue)));

						}
					}
				}
			} else if (attributeCode.startsWith("SCH_LINK_CODE")) {
				linkCode = ea.getValue();
			} else if (attributeCode.startsWith("SCH_LINK_VALUE")) {
				linkValue = ea.getValue();
			} else if (attributeCode.startsWith("SCH_SOURCE_CODE")) {
				sourceCode = ea.getValue();
			} else if (attributeCode.startsWith("SCH_TARGET_CODE")) {
				targetCode = ea.getValue();
			} else if (attributeCode.startsWith("SCH_STATUS")) {
				Integer ordinal = ea.getValueInteger();
				status = EEntityStatus.values()[ordinal];
			// Add to sort list if it is a sort attribute
			} else if (attributeCode.startsWith("SRT_")) {
				sortAttributes.add(ea);
			}
		}
		// Add BaseEntity Status expression
		// builder.and(baseEntity.status.coalesce(EEntityStatus.ACTIVE).getValue().loe(status));
		builder.and(baseEntity.status.loe(status));
		// Order the sorts by weight
		Comparator<EntityAttribute> compareByWeight = (EntityAttribute a, EntityAttribute b) -> a.getWeight().compareTo(b.getWeight());
		Collections.sort(sortAttributes, compareByWeight);
		// Create a Join for each sort
		for (EntityAttribute sort : sortAttributes) {

			String attributeCode = sort.getAttributeCode();
			log.info("Sorting with " + attributeCode);
			QEntityAttribute eaOrderJoin = new QEntityAttribute("eaOrderJoin_"+joinCounter.toString());
			joinCounter++;

			if (!(attributeCode.startsWith("SRT_PRI_CREATED") || attributeCode.startsWith("SRT_PRI_UPDATED")
				|| attributeCode.startsWith("SRT_PRI_CODE") || attributeCode.startsWith("SRT_PRI_NAME"))) {
				query.leftJoin(eaOrderJoin)
					.on(eaOrderJoin.pk.baseEntity.id.eq(baseEntity.id)
					.and(eaOrderJoin.attributeCode.eq(attributeCode)));
			}

			ComparableExpressionBase orderColumn = null;
			if (attributeCode.startsWith("SRT_PRI_CREATED")) {
				// Use ID because there is no index on created, and this gives same result
				orderColumn = baseEntity.id;
			} else if (attributeCode.startsWith("SRT_PRI_UPDATED")) {
				orderColumn = baseEntity.updated;
			} else if (attributeCode.startsWith("SRT_PRI_CODE")) {
				orderColumn = baseEntity.code;
			} else if (attributeCode.startsWith("SRT_PRI_NAME")) {
				orderColumn = baseEntity.name;
			} else {
				// Use Attribute Code to find the datatype, and thus the DB field to sort on
				Attribute attr = RulesUtils.getAttribute(attributeCode.substring("SRT_".length()), passedToken);
				String dtt = attr.getDataType().getClassName();
				orderColumn = getPathFromDatatype(dtt, eaOrderJoin);
			}

			if (orderColumn != null) {
				if (sort.getValueString().equals("ASC")) {
					query.orderBy(orderColumn.asc());
				} else {
					query.orderBy(orderColumn.desc());
				}
			} else {
				log.info("orderColumn is null for attribute " + attributeCode);
			}
		}

		Instant start = Instant.now();
		String debugStr = "Search25DEBUG";
		log.info(debugStr + " Start building link join");
		// Build link join if necessary
		if (sourceCode != null || targetCode != null || linkCode != null || linkValue != null) {
			QEntityEntity linkJoin = new QEntityEntity("linkJoin");
			BooleanBuilder linkBuilder = new BooleanBuilder();

			log.info("Source Code is "+sourceCode);
			log.info("Target Code is "+targetCode);
			log.info("Link Code is "+linkCode);
			log.info("Link Value is "+linkValue);
			if (sourceCode == null && targetCode == null) {
				// Only look in targetCode if both are null
				linkBuilder.and(linkJoin.link.targetCode.eq(baseEntity.code));
			} else if (sourceCode != null) {
				linkBuilder.and(linkJoin.link.sourceCode.eq(sourceCode));
				if (targetCode == null) {
					linkBuilder.and(linkJoin.link.targetCode.eq(baseEntity.code));
				}
			} else if (targetCode != null) {
				linkBuilder.and(linkJoin.link.targetCode.eq(targetCode));
				if (sourceCode == null) {
					linkBuilder.and(linkJoin.link.sourceCode.eq(baseEntity.code));
				}
			}

			query.join(linkJoin).on(linkBuilder);

			if (linkCode != null) {
				builder.and(linkJoin.link.attributeCode.eq(linkCode));
			}
			if (linkValue != null) {
				builder.and(linkJoin.link.linkValue.eq(linkValue));
			}

			// Order By Weight Of ENTITY_ENTITY link
			if (sortAttributes.size() == 0) {
				query.orderBy(linkJoin.weight.asc());
			}
		}
		// Search across people and companies if from searchbar
		if (searchBE.getCode().startsWith("SBE_SEARCHBAR")) {
			// search across people and companies
			builder.and(baseEntity.code.like("PER_%"))
				.or(baseEntity.code.like("CPY_%"));
		}
		// Add all builder conditions to query
		query.where(builder);
		// Set page start and page size, then fetch codes
		query.offset(pageStart).limit(pageSize);

		Instant end = Instant.now();
		Duration timeElapsed = Duration.between(start, end);
		log.info(debugStr + " Finished building link join, cost:" + timeElapsed.toMillis() + " millSeconds.");

		start = Instant.now();
		log.info(debugStr + " Start query, countOnly=" + countOnly);
		if (countOnly) {
			// Fetch only the count
			long count = query.select(baseEntity.code).distinct().fetchCount();
			result = new QSearchBeResult(count);
		} else {
			// Fetch codes and count
			codes = query.select(baseEntity.code).distinct().fetch();
			long count = query.fetchCount();
			result = new QSearchBeResult(codes, count);

			if (fetchEntities != null && fetchEntities) {

				String[] filterArray = VertxUtils.getSearchColumnFilterArray(searchBE).toArray(new String[0]);
				BaseEntity[] beArray = new BaseEntity[codes.size()];

				for (int i = 0; i < codes.size(); i++) {

					String code = codes.get(i);
					BaseEntity be = findBaseEntityByCode(code);
					be = VertxUtils.privacyFilter(be, filterArray);
					be.setIndex(i);
					beArray[i] = be;
				}

				result.setEntities(beArray);
			}
		}
		end = Instant.now();
		log.info(debugStr + " Finished building link join, cost:" + Duration.between(start, end).toMillis() + " millSeconds.");
		// Return codes and count
		log.info("SQL = " + query.toString());
		return result;
	}

	public static Predicate getDateTimePredicate(EntityAttribute ea, DateTimePath path) {
		String condition = SearchEntity.convertFromSaveable(ea.getAttributeName());
		LocalDateTime dateTime = ea.getValueDateTime();

		if (dateTime == null) {
			LocalDate date = ea.getValueDate();
			log.info(ea.getAttributeCode() + " " + condition + " " + date);

			// Convert Date into two DateTime boundaries
			LocalDateTime lowerBound = date.atStartOfDay();
			log.info("lowerBound = " + lowerBound);
			LocalDateTime upperBound = lowerBound.plusDays(1);
			log.info("upperBound = " + upperBound);

			if (condition.equals(">")) {
				return path.after(upperBound);
			} else if (condition.equals(">=")) {
				return path.after(lowerBound);
			} else if (condition.equals("<")) {
				return path.before(lowerBound);
			} else if (condition.equals("<=")) {
				return path.before(upperBound);
			} else if (condition.equals("!=")) {
				return path.notBetween(lowerBound, upperBound);
			} else {
				return path.between(lowerBound, upperBound);
			}
		}
		log.info(ea.getAttributeCode() + " " + condition + " " + dateTime);
			
		if (condition.equals(">=") || condition.equals(">")) {
			return path.after(dateTime);
		} else if (condition.equals("<=") || condition.equals("<")) {
			return path.before(dateTime);
		} else if (condition.equals("!=")) {
			return path.ne(dateTime);
		}
		// Default to equals
		return path.eq(dateTime);
	}
	
	public static Predicate getAttributeSearchColumn(EntityAttribute ea, QEntityAttribute entityAttribute) {

		// TODO: Make this function more neat, and less repetitive - Jasper (19/08/2021)

		String attributeFilterValue = ea.getValue().toString();
		String condition = SearchEntity.convertFromSaveable(ea.getAttributeName());
		System.out.println(ea.getAttributeCode() + " " + condition + " " + attributeFilterValue);

		String valueString = "";
		if (ea.getValueString() != null) {
			valueString = ea.getValueString();
		}
		// Null check on condition and default to equals valuestring
		if (condition == null) {
			log.error("SQL condition is NULL, " + "EntityAttribute baseEntityCode is:" + ea.getBaseEntityCode()
					+ ", attributeCode is: " + ea.getAttributeCode());
			// LIKE
		} else if (condition.equals("LIKE")) {
			return entityAttribute.valueString.like(valueString);
			// NOT LIKE
		} else if (condition.equals("NOT LIKE")) {
			return entityAttribute.valueString.notLike(valueString);
			// EQUALS
		} else if (condition.equals("=")) {
			if (ea.getValueBoolean() != null) {
				return entityAttribute.valueBoolean.eq(ea.getValueBoolean());
			} else if (ea.getValueDouble() != null) {
				return entityAttribute.valueDouble.eq(ea.getValueDouble());
			} else if (ea.getValueInteger() != null) {
				return entityAttribute.valueInteger.eq(ea.getValueInteger());
			} else if (ea.getValueLong() != null) {
				return entityAttribute.valueLong.eq(ea.getValueLong());
			} else if (ea.getValueDate() != null) {
				return entityAttribute.valueDate.eq(ea.getValueDate());
			} else if (ea.getValueDateTime() != null) {
				return entityAttribute.valueDateTime.eq(ea.getValueDateTime());
			} else {
				return entityAttribute.valueString.eq(valueString);
			}
			// NOT EQUALS
		} else if (condition.equals("!=")) {
			if (ea.getValueBoolean() != null) {
				return entityAttribute.valueBoolean.ne(ea.getValueBoolean());
			} else if (ea.getValueDouble() != null) {
				return entityAttribute.valueDouble.ne(ea.getValueDouble());
			} else if (ea.getValueInteger() != null) {
				return entityAttribute.valueInteger.ne(ea.getValueInteger());
			} else if (ea.getValueLong() != null) {
				return entityAttribute.valueLong.ne(ea.getValueLong());
			} else if (ea.getValueDate() != null) {
				return entityAttribute.valueDate.ne(ea.getValueDate());
			} else if (ea.getValueDateTime() != null) {
				return entityAttribute.valueDateTime.ne(ea.getValueDateTime());
			} else {
				return entityAttribute.valueString.ne(valueString);
			}
			// GREATER THAN OR EQUAL TO
		} else if (condition.equals(">=")) {
			if (ea.getValueDouble() != null) {
				return entityAttribute.valueDouble.goe(ea.getValueDouble());
			} else if (ea.getValueInteger() != null) {
				return entityAttribute.valueInteger.goe(ea.getValueInteger());
			} else if (ea.getValueLong() != null) {
				return entityAttribute.valueLong.goe(ea.getValueLong());
			} else if (ea.getValueDate() != null) {
				return entityAttribute.valueDate.goe(ea.getValueDate());
			} else if (ea.getValueDateTime() != null) {
				return entityAttribute.valueDateTime.goe(ea.getValueDateTime());
			}
			// LESS THAN OR EQUAL TO
		} else if (condition.equals("<=")) {
			if (ea.getValueDouble() != null) {
				return entityAttribute.valueDouble.loe(ea.getValueDouble());
			} else if (ea.getValueInteger() != null) {
				return entityAttribute.valueInteger.loe(ea.getValueInteger());
			} else if (ea.getValueLong() != null) {
				return entityAttribute.valueLong.loe(ea.getValueLong());
			} else if (ea.getValueDate() != null) {
				return entityAttribute.valueDate.loe(ea.getValueDate());
			} else if (ea.getValueDateTime() != null) {
				return entityAttribute.valueDateTime.loe(ea.getValueDateTime());
			}
			// GREATER THAN
		} else if (condition.equals(">")) {
			if (ea.getValueDouble() != null) {
				return entityAttribute.valueDouble.gt(ea.getValueDouble());
			} else if (ea.getValueInteger() != null) {
				return entityAttribute.valueInteger.gt(ea.getValueInteger());
			} else if (ea.getValueLong() != null) {
				return entityAttribute.valueLong.gt(ea.getValueLong());
			} else if (ea.getValueDate() != null) {
				return entityAttribute.valueDate.after(ea.getValueDate());
			} else if (ea.getValueDateTime() != null) {
				return entityAttribute.valueDateTime.after(ea.getValueDateTime());
			}
			// LESS THAN
		} else if (condition.equals("<")) {
			if (ea.getValueDouble() != null) {
				return entityAttribute.valueDouble.lt(ea.getValueDouble());
			} else if (ea.getValueInteger() != null) {
				return entityAttribute.valueInteger.lt(ea.getValueInteger());
			} else if (ea.getValueLong() != null) {
				return entityAttribute.valueLong.lt(ea.getValueLong());
			} else if (ea.getValueDate() != null) {
				return entityAttribute.valueDate.before(ea.getValueDate());
			} else if (ea.getValueDateTime() != null) {
				return entityAttribute.valueDateTime.before(ea.getValueDateTime());
			}
		}
		// Default
		return entityAttribute.valueString.eq(valueString);
	}

	public static ComparableExpressionBase getPathFromDatatype(String dtt, QEntityAttribute entityAttribute) {

		if (dtt.equals("Text")) {
			return entityAttribute.valueString;
		} else if (dtt.equals("java.lang.String") || dtt.equals("String")) {
			return entityAttribute.valueString;
		} else if (dtt.equals("java.lang.Boolean") || dtt.equals("Boolean")) {
			return entityAttribute.valueBoolean;
		} else if (dtt.equals("java.lang.Double") || dtt.equals("Double")) {
			return entityAttribute.valueDouble;
		} else if (dtt.equals("java.lang.Integer") || dtt.equals("Integer")) {
			return entityAttribute.valueInteger;
		} else if (dtt.equals("java.lang.Long") || dtt.equals("Long")) {
			return entityAttribute.valueLong;
		} else if (dtt.equals("java.time.LocalDateTime") || dtt.equals("LocalDateTime")) {
			return entityAttribute.valueDateTime;
		} else if (dtt.equals("java.time.LocalDate") || dtt.equals("LocalDate")) {
			return entityAttribute.valueDate;
		} else if (dtt.equals("java.time.LocalTime") || dtt.equals("LocalTime")) {
			return entityAttribute.valueTime;
		}

		log.warn("Unable to read datatype");
		return entityAttribute.valueString;
	}


	/**
	 * For association filter of format like LNK_PERSON.LNK_COMPANY.PRI_NAME,
	 * this function wil strip the first code in that chain of attributes
	 * whilst retaining any AND/OR prefixs.
	 */
	public static void detatchBaseAttributeCode(List<EntityAttribute> eaList) {

		eaList.stream().forEach(ea -> {
			String[] associationArray = ea.getAttributeCode().split("\\.");

			if (associationArray.length > 1) {
				String baseAttributeCode = associationArray[0];

				String prefix = "";
				if (baseAttributeCode.startsWith("AND_")) {
					prefix = "AND_";
				}
				if (baseAttributeCode.startsWith("OR_")) {
					prefix = "OR_";
				}
				// Remove first item and update with prefix at beginning
				String[] newAssociationArray = Arrays.copyOfRange(associationArray, 1, associationArray.length);
				ea.setAttributeCode(prefix + String.join(".", newAssociationArray));
			} else {
				log.warn("Association array length too small. Not updating!");
			}
		});
	}
	/**
	* Create a sub query for searhing across LNK associations
	*
	* This is a recursive function that can run as many 
	* times as is specified by the attribute.
	*
	* @param ea The EntityAttribute filter from SBE
	* @return the subquery object
	 */
	public static JPQLQuery generateSubQuery(List<EntityAttribute> eaList) {

		// Find first attribute that is not AND/OR. There should be only one
		EntityAttribute ea = eaList.stream().filter(x -> (!x.getAttributeCode().startsWith("AND_") && !x.getAttributeCode().startsWith("OR_"))).findFirst().get();

		// Random uuid for uniqueness in the query string
		String uuid = UUID.randomUUID().toString().substring(0, 8);

		// Define items to base query upon
		QBaseEntity baseEntity = new QBaseEntity("baseEntity_"+uuid);
		QEntityAttribute entityAttribute = new QEntityAttribute("entityAttribute_"+uuid);

		// Unpack each attributeCode
		String[] associationArray = ea.getAttributeCode().split("\\.");
		String baseAttributeCode = associationArray[0];
		if (associationArray.length > 1) {
			// Prepare for next iteration by removing base code
			detatchBaseAttributeCode(eaList);

			// Recursive search
			return JPAExpressions.selectDistinct(baseEntity.code)
				.from(baseEntity)
				.leftJoin(entityAttribute)
				.on(entityAttribute.pk.baseEntity.id.eq(baseEntity.id)
						.and(entityAttribute.attributeCode.eq(baseAttributeCode)))
				.where(
					Expressions.stringTemplate("replace({0},'[\"','')", 
						Expressions.stringTemplate("replace({0},'\"]','')", entityAttribute.valueString)
					).in(generateSubQuery(eaList)));
		} else {

			// Create SubQuery Builder parameters using filters
			BooleanBuilder builder = new BooleanBuilder();
			builder.and(getAttributeSearchColumn(ea, entityAttribute));

			// Process AND Filters
			eaList.stream().filter(x -> 
					x.getAttributeCode().startsWith("AND")
					&& removePrefixFromCode(x.getAttributeCode(), "AND").equals(baseAttributeCode))
				.forEach(x -> {
					builder.and(getAttributeSearchColumn(x, entityAttribute));
			});
			// Process OR Filters
			eaList.stream().filter(x -> 
					x.getAttributeCode().startsWith("OR")
					&& removePrefixFromCode(x.getAttributeCode(), "OR").equals(baseAttributeCode))
				.forEach(x -> {
					builder.or(getAttributeSearchColumn(x, entityAttribute));
			});

			// Return the final SubQuery
			return JPAExpressions.selectDistinct(baseEntity.code)
				.from(baseEntity)
				.leftJoin(entityAttribute)
				.on(entityAttribute.pk.baseEntity.id.eq(baseEntity.id)
					.and(entityAttribute.attributeCode.eq(baseAttributeCode)))
				.where(builder);
		}
	}

	public static JPQLQuery generateWildcardSubQuery(String value, Integer recursion, String[] whitelist, String[] blacklist) {

		// Random uuid to for uniqueness in the query string
		String uuid = UUID.randomUUID().toString().substring(0, 8);

		// Define items to query base upon
		QBaseEntity baseEntity = new QBaseEntity("baseEntity_"+uuid);
		QEntityAttribute entityAttribute = new QEntityAttribute("entityAttribute_"+uuid);


		JPQLQuery exp = JPAExpressions.selectDistinct(baseEntity.code)
			.from(baseEntity)
			.leftJoin(entityAttribute);

		// Handle whitelisting and blacklisting
		if (whitelist.length > 0) {
			exp.on(entityAttribute.pk.baseEntity.id.eq(baseEntity.id).and(entityAttribute.attributeCode.in(whitelist)));
		} else if (blacklist.length > 0) {
			exp.on(entityAttribute.pk.baseEntity.id.eq(baseEntity.id).and(entityAttribute.attributeCode.notIn(blacklist)));
		} else {
			exp.on(entityAttribute.pk.baseEntity.id.eq(baseEntity.id));
		}

		if (recursion > 1) {
			exp.where(entityAttribute.valueString.like(value)
				.or(Expressions.stringTemplate("replace({0},'[\"','')", 
					Expressions.stringTemplate("replace({0},'\"]','')", entityAttribute.valueString)
					).in(generateWildcardSubQuery(value, recursion-1, whitelist, blacklist))
			   ));
		} else {
			exp.where(entityAttribute.valueString.like(value));
		}
		return exp;
	}

	/**
	 * Quick tool to remove any prefix strings from attribute codes, even if the
	 * prefix occurs multiple times.
	 * 
	 * @param code   The attribute code
	 * @param prefix The prefix to remove
	 * @return formatted The formatted code
	 */
	public static String removePrefixFromCode(String code, String prefix) {

		String formatted = code;
		while (formatted.startsWith(prefix + "_")) {
			formatted = formatted.substring(prefix.length() + 1);
		}
		return formatted;
	}
	
	public Long findBySearchEA24Count(@NotNull final String hql) {
		QSearchBeResult result = null;

		Query query = null;
			
		Integer fromIndex = hql.indexOf("from");
		String hqlGuts = hql.substring(fromIndex);
		String hqlStart = hql.substring(0,fromIndex);
		
		String[] afterSelect = hqlStart.split(" ", 2);
		String hqlCount = "select count ("+afterSelect[1]+") ";
		String hql3 = hqlCount + hqlGuts;
	
		query = getEntityManager().createQuery(hql3);
		Long count = (Long)query.getSingleResult();
		return count;
	}
	
	public List<BaseEntity> findBySearchBE2(@NotNull final String hql) {
		List<BaseEntity> results = null;

		Query query = null;
		Set<String> attributeCodes = new HashSet<>(Arrays.asList("PRI_FIRSTNAME", "PRI_LASTNAME"));
		Filter filter = getEntityManager().unwrap(Session.class).enableFilter("filterAttribute");
		filter.setParameterList("attributeCodes", attributeCodes);
		query = getEntityManager().createQuery(hql);
		query.setFirstResult(0).setMaxResults(1000);

		results = query.setHint(QueryHints.HINT_READONLY, true).getResultList();
		log.debug("RESULTS=" + results);
		return results;
	}

	public List<Object> findBySearchBE3(@NotNull final String hql) {
		List<Object> results = null;
		log.debug("Object result:" + hql);
		Query query = null;
		query = getEntityManager().createQuery(hql);
		query.setFirstResult(0).setMaxResults(1000);

		results = query.setHint(QueryHints.HINT_READONLY, true).getResultList();
		log.debug("RESULTS=" + results);
		return results;
	}

	class Column implements Comparable<Column> {
		private String fieldName;
		private String fieldCode;
		private Double weight;
		private String linkCode;
		private String linkType;

		public Column(final String fieldCode, final String fieldName, final Double weight) {
			this.fieldName = fieldName;
			this.fieldCode = fieldCode;
			this.weight = weight;
		}

		@Override
		public int compareTo(Column compareColumn) {
			return this.weight.compareTo(compareColumn.getWeight());
		}

		/**
		 * @return the fieldName
		 */
		public String getFieldName() {
			return fieldName;
		}

		/**
		 * @return the fieldCode
		 */
		public String getFieldCode() {
			return fieldCode;
		}

		/**
		 * @return the weight
		 */
		public Double getWeight() {
			return weight;
		}

		/**
		 * @return the linkCode
		 */
		public String getLinkCode() {
			return linkCode;
		}

		/**
		 * @param linkCode the linkCode to set
		 */
		public void setLinkCode(String linkCode) {
			this.linkCode = linkCode;
		}

		/**
		 * @return the linkType
		 */
		public String getLinkType() {
			return linkType;
		}

		/**
		 * @param linkType the linkType to set
		 */
		public void setLinkType(String linkType) {
			this.linkType = linkType;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "Column [" + (fieldName != null ? "fieldName=" + fieldName + ", " : "")
					+ (fieldCode != null ? "fieldCode=" + fieldCode + ", " : "")
					+ (weight != null ? "weight=" + weight + ", " : "")
					+ (linkCode != null ? "linkCode=" + linkCode + ", " : "")
					+ (linkType != null ? "linkType=" + linkType : "") + "]";
		}

	}

	class Order implements Comparable<Order> {
		private String fieldName;
		private String ascdesc;
		private Double weight;

		public Order(final String fieldName, final String ascdesc, final Double weight) {
			this.fieldName = fieldName;
			this.ascdesc = ascdesc;
			this.weight = weight;
		}

		/**
		 * @return the fieldName
		 */
		public String getFieldName() {
			return fieldName;
		}

		/**
		 * @return the ascdesc
		 */
		public String getAscdesc() {
			return ascdesc;
		}

		/**
		 * @return the weight
		 */
		public Double getWeight() {
			return weight;
		}

		@Override
		public int compareTo(Order o) {
			return this.weight.compareTo(o.getWeight());
		}

	}

	class OrderCompare implements Comparator<Order> {

		@Override
		public int compare(Order o1, Order o2) {
			// write comparison logic here like below , it's just a sample
			return o1.getWeight().compareTo(o2.getWeight());
		}
	}

	class ColumnCompare implements Comparator<Column> {

		@Override
		public int compare(Column o1, Column o2) {
			// write comparison logic here like below , it's just a sample
			return o1.getWeight().compareTo(o2.getWeight());
		}
	}

	public Long findBySearchBECount(@NotNull final BaseEntity searchBE) {
		Long count = 0L;
		final String userRealmStr = getRealm();

		String stakeholderCode = searchBE.getValue("SCH_STAKEHOLDER_CODE", null);
		String sourceStakeholderCode = searchBE.getValue("SCH_SOURCE_STAKEHOLDER_CODE", null);
		String linkCode = searchBE.getValue("SCH_LINK_CODE", null);
		String linkValue = searchBE.getValue("SCH_LINK_VALUE", null);
		Double linkWeight = searchBE.getValue("SCH_LINK_WEIGHT", 0.0);
		String linkWeightFilter = searchBE.getValue("SCH_LINK_FILTER", ">");

		String sourceCode = searchBE.getValue("SCH_SOURCE_CODE", null);
		String targetCode = searchBE.getValue("SCH_TARGET_CODE", null);

		searchBE.removeAttribute("PRI_INDEX");
		searchBE.removeAttribute("PRI_TOTAL_RESULTS"); // ugly
		searchBE.removeAttribute("SCH_INDEX");
		searchBE.removeAttribute("SCH_INDEX"); // ugly

		// Check for bad linkWeight filtering
		if (!allowedLinkWeightConditions.stream().anyMatch(str -> str.trim().equals(linkWeightFilter))) {
			log.error("Error! Illegal link Weight condition!(" + linkWeightFilter + ") for user " + getUser());
			return 0L;
		}

		SearchSettings ss = this.buildQuerySettings(searchBE, 0);

		Set<String> realms = new HashSet<>();

		realms.add(getRealm());

		// realms.add("genny");
		String realmsStr = getRealmsStr(realms);

		String sql = createSearchSQL("count(distinct ea.pk.baseEntity)", stakeholderCode, sourceStakeholderCode,
				linkCode, linkValue, linkWeight, linkWeightFilter, sourceCode, targetCode, ss.filterStrings,
				ss.filterStringsQ, "", ss.codeFilter, realmsStr);

		Query query = null;

		query = getEntityManager().createQuery(sql);

		// query.setParameter("realms", realms); This would not work

		if (sourceCode != null) {
			query.setParameter("sourceCode", sourceCode);
		}
		if (targetCode != null) {
			query.setParameter("targetCode", targetCode);
		}
		if (linkCode != null) {
			query.setParameter("linkCode", linkCode);
		}
		if (linkValue != null) {
			query.setParameter("linkValue", linkValue);
		}
		if (linkWeight > 0.0) {
			query.setParameter("linkWeight", linkWeight);
		}

		if (stakeholderCode != null) {
			query.setParameter("stakeholderCode", stakeholderCode);
		}
		if (sourceStakeholderCode != null) {
			query.setParameter("sourceStakeholderCode", sourceStakeholderCode);
		}
		//
		for (Tuple2<String, Object> value : ss.valueList) {
			log.info("Value: " + value._1 + " =: " + value._2);
			if (value._2 instanceof String) {
				String result = (String) value._2;
				query.setParameter(value._1, result);
			} else if (value._2 instanceof Boolean) {
				Boolean result = (Boolean) value._2;
				query.setParameter(value._1, result);
			} else if (value._2 instanceof LocalDateTime) {
				LocalDateTime result = (LocalDateTime) value._2;
				query.setParameter(value._1, result);
			} else if (value._2 instanceof LocalDate) {
				LocalDate result = (LocalDate) value._2;
				query.setParameter(value._1, result);
			} else if (value._2 instanceof Long) {
				Long result = (Long) value._2;
				query.setParameter(value._1, result);
			} else if (value._2 instanceof Integer) {
				Integer result = (Integer) value._2;
				query.setParameter(value._1, result);
			} else if (value._2 instanceof LocalTime) {
				LocalTime result = (LocalTime) value._2;
				query.setParameter(value._1, result);
			} else {
				query.setParameter(value._1, value._2);
			}
		}
		// Long count1 = 100L;
		
		Object count1 = query.getSingleResult();
		log.info("The Count Object is :: " + count1.toString());
		count = (Long) count1;

		return count;
	}

	protected String getRealm() {

		return "genny"; // TODO HACK, is overridden
	}

	private String getRealmsStr(Set<String> realms) {
		String ret = "";
		for (String realm : realms) {
			ret += "'" + realm + "',";
		}
		return ret.substring(0, ret.length() - 1);
	}

	private <T> String getHQL(Range rangeValue, String attributeCodeEA, String valueType, Integer filterIndex,
			List<Tuple2<String, Object>> valueList) {
		String ret = "";
		if (rangeValue.hasLowerBound() && rangeValue.hasUpperBound()) {
			if (rangeValue.lowerBoundType().equals(BoundType.CLOSED)
					&& rangeValue.lowerBoundType().equals(BoundType.CLOSED)) {
				ret = " and " + attributeCodeEA + "." + valueType + ">=:v" + filterIndex + " ";
				valueList.add(Tuple.of("vd" + filterIndex++, rangeValue.lowerEndpoint()));
				ret += " and " + attributeCodeEA + "." + valueType + "<=:v" + filterIndex + " ";
				valueList.add(Tuple.of("vd" + filterIndex, rangeValue.upperEndpoint()));
			} else if (rangeValue.lowerBoundType().equals(BoundType.CLOSED)
					&& rangeValue.lowerBoundType().equals(BoundType.OPEN)) {
				ret = " and " + attributeCodeEA + "." + valueType + ">=:v" + filterIndex++ + " ";
				valueList.add(Tuple.of("vd" + filterIndex++, rangeValue.lowerEndpoint()));
				ret += " and " + attributeCodeEA + "." + valueType + "<:v" + filterIndex + " ";
				valueList.add(Tuple.of("vd" + filterIndex, rangeValue.upperEndpoint()));
			} else if (rangeValue.lowerBoundType().equals(BoundType.OPEN)
					&& rangeValue.lowerBoundType().equals(BoundType.CLOSED)) {
				ret = " and " + attributeCodeEA + "." + valueType + ">:v" + filterIndex++ + " ";
				valueList.add(Tuple.of("vd" + filterIndex++, rangeValue.lowerEndpoint()));
				ret += " and " + attributeCodeEA + "." + valueType + "<=:v" + filterIndex + " ";
				valueList.add(Tuple.of("vd" + filterIndex, rangeValue.upperEndpoint()));
			} else if (rangeValue.lowerBoundType().equals(BoundType.OPEN)
					&& rangeValue.lowerBoundType().equals(BoundType.OPEN)) {
				ret = " and " + attributeCodeEA + "." + valueType + ">:v" + filterIndex++ + " ";
				valueList.add(Tuple.of("vd" + filterIndex++, rangeValue.lowerEndpoint()));
				ret += " and " + attributeCodeEA + "." + valueType + "<:v" + filterIndex + " ";
				valueList.add(Tuple.of("vd" + filterIndex, rangeValue.upperEndpoint()));
			}
		} else if (rangeValue.hasLowerBound() && !rangeValue.hasUpperBound()) {
			if (rangeValue.lowerBoundType().equals(BoundType.CLOSED)) {
				ret = " and " + attributeCodeEA + "." + valueType + ">=:v" + filterIndex + " ";
			} else {
				ret = " and " + attributeCodeEA + "." + valueType + ">=:v" + filterIndex + " ";
			}
			valueList.add(Tuple.of("vd" + filterIndex, rangeValue.lowerEndpoint()));
		} else if (rangeValue.hasUpperBound() && !rangeValue.hasLowerBound()) {
			if (rangeValue.upperBoundType().equals(BoundType.CLOSED)) {
				ret = " and " + attributeCodeEA + "." + valueType + "<=:v" + filterIndex + " ";
			} else {
				ret = " and " + attributeCodeEA + "." + valueType + "<:v" + filterIndex + " ";
			}
			valueList.add(Tuple.of("vd" + filterIndex, rangeValue.upperEndpoint()));
		}
		filterIndex++;
		return ret;
	}

	public Long findBySearchBECount(@NotNull final QSearchEntityMessage searchBE) {
		Long count = 0L;
		final String userRealmStr = getRealm();

		Integer pageStart = searchBE.getParent().getValue("SCH_PAGE_START", 0);
		Integer pageSize = searchBE.getParent().getValue("SCH_PAGE_SIZE", 100);
		String stakeholderCode = searchBE.getParent().getValue("SCH_STAKEHOLDER_CODE", null);
		String sourceStakeholderCode = searchBE.getParent().getValue("SCH_SOURCE_STAKEHOLDER_CODE", null);
		String linkCode = searchBE.getParent().getValue("SCH_LINK_CODE", null);
		String linkValue = searchBE.getParent().getValue("SCH_LINK_VALUE", null);
		Double linkWeight = searchBE.getParent().getValue("SCH_LINK_WEIGHT", 0.0);
		String linkWeightFilter = searchBE.getParent().getValue("SCH_LINK_FILTER", ">");
		String sourceCode = searchBE.getParent().getValue("SCH_SOURCE_CODE", null);
		String targetCode = searchBE.getParent().getValue("SCH_TARGET_CODE", null);

		Integer filterIndex = 0;

		SearchEntity se = searchBE.getParent();

		SearchSettings ss = buildQuerySettings(se, filterIndex);
		filterIndex = ss.filterIndex;
		List<SearchSettings> filters = new ArrayList<SearchSettings>();

		for (BaseEntity filter : searchBE.getItems()) {
			SearchSettings myFilter = (buildQuerySettings(filter, filterIndex));
			filters.add(myFilter);
			filterIndex = myFilter.filterIndex; // update the filterIndex to keep up to date across all the 'Ors'
		}

		Set<String> realms = new HashSet<>();
		realms.add(getRealm());

		realms.add("genny");
		String realmsStr = getRealmsStr(realms);

		ss.orderString = createOrderString(ss.attributeCodeMap, ss.orderList);
		String sql = createSearchSQL("count(distinct ea.pk.baseEntity)", stakeholderCode, sourceStakeholderCode,
				linkCode, linkValue, linkWeight, linkWeightFilter, sourceCode, targetCode, ss.filterStrings,
				ss.filterStringsQ, "", ss.codeFilter, realmsStr);

		Query query = null;

		query = getEntityManager().createQuery(sql);

		// query.setParameter("realms", realms); This would not work

		if (sourceCode != null) {
			query.setParameter("sourceCode", sourceCode);
		}
		if (targetCode != null) {
			query.setParameter("targetCode", targetCode);
		}
		if (linkCode != null) {
			query.setParameter("linkCode", linkCode);
		}
		if (linkValue != null) {
			query.setParameter("linkValue", linkValue);
		}
		if (linkWeight > 0.0) {
			query.setParameter("linkWeight", linkWeight);
		}

		if (stakeholderCode != null) {
			query.setParameter("stakeholderCode", stakeholderCode);
		}
		if (sourceStakeholderCode != null) {
			query.setParameter("sourceStakeholderCode", sourceStakeholderCode);
		}
		//
		for (SearchSettings filter : filters) {
			for (Tuple2<String, Object> value : filter.valueList) {
				log.debug("Value: " + value._1 + " =: " + value._2);
				if (value._2 instanceof Boolean) {
					Boolean result = (Boolean) value._2;
					query.setParameter(value._1, result);
				} else if (value._2 instanceof LocalDateTime) {
					LocalDateTime result = (LocalDateTime) value._2;
					query.setParameter(value._1, result);
				} else if (value._2 instanceof LocalDate) {
					LocalDate result = (LocalDate) value._2;
					query.setParameter(value._1, result);
				} else if (value._2 instanceof LocalTime) {
					LocalTime result = (LocalTime) value._2;
					query.setParameter(value._1, result);
				} else {
					query.setParameter(value._1, value._2);
				}
			}
		}
		Object count1 = query.getSingleResult();
		log.info("The Count Object is :: " + count1.toString());
		count = (Long) count1;
		// count = (Long)query.getSingleResult();

		return count;
	}

	public List<BaseEntity> findBySearchBE(@NotNull final QSearchEntityMessage searchBE) {
		List<BaseEntity> results = new ArrayList<BaseEntity>();
		final String userRealmStr = getRealm();

		Integer pageStart = searchBE.getParent().getValue("SCH_PAGE_START", 0);
		Integer pageSize = searchBE.getParent().getValue("SCH_PAGE_SIZE", 100);
		String stakeholderCode = searchBE.getParent().getValue("SCH_STAKEHOLDER_CODE", null);
		String sourceStakeholderCode = searchBE.getParent().getValue("SCH_SOURCE_STAKEHOLDER_CODE", null);
		String linkCode = searchBE.getParent().getValue("SCH_LINK_CODE", null);
		String linkValue = searchBE.getParent().getValue("SCH_LINK_VALUE", null);
		Double linkWeight = searchBE.getParent().getValue("SCH_LINK_WEIGHT", 0.0);
		String linkWeightFilter = searchBE.getParent().getValue("SCH_LINK_FILTER", ">");
		String sourceCode = searchBE.getParent().getValue("SCH_SOURCE_CODE", null);
		String targetCode = searchBE.getParent().getValue("SCH_TARGET_CODE", null);

		// Check for bad linkWeight filtering
		if (!allowedLinkWeightConditions.stream().anyMatch(str -> str.trim().equals(linkWeightFilter))) {
			log.error("Error! Illegal link Weight condition!(" + linkWeightFilter + ") for user " + getUser());
			return results;
		}

		Integer filterIndex = 0;

		SearchEntity se = searchBE.getParent();

		SearchSettings ss = buildQuerySettings(se, filterIndex);
		filterIndex = ss.filterIndex;
		List<SearchSettings> filters = new ArrayList<SearchSettings>();

		for (BaseEntity filter : searchBE.getItems()) {
			SearchSettings myFilter = (buildQuerySettings(filter, filterIndex));
			filters.add(myFilter);
			filterIndex = myFilter.filterIndex; // update the filterIndex to keep up to date across all the 'Ors'
		}

		Set<String> realms = new HashSet<String>();
		realms.add(userRealmStr);
		realms.add("genny");
		String realmsStr = getRealmsStr(realms);

		ss.orderString = createOrderString(ss.attributeCodeMap, ss.orderList);

		String sql = createSearchSQL("distinct ea.pk.baseEntity", stakeholderCode, sourceStakeholderCode, linkCode,
				linkValue, linkWeight, linkWeightFilter, sourceCode, targetCode, ss.filterStrings, ss.filterStringsQ,
				ss.orderString, filters, realmsStr);

		Query query = null;

		// Limit column attributes returned using Hibernate filter
		// If empty then return everything
		if (!ss.attributeCodes.isEmpty()) {
			Filter filter = getEntityManager().unwrap(Session.class).enableFilter("filterAttribute");
			filter.setParameterList("attributeCodes", ss.attributeCodes);
		}

		query = getEntityManager().createQuery(sql);

		query.setFirstResult(pageStart).setMaxResults(pageSize);

		// query.setParameter("realms", realms); This would not work

		if (sourceCode != null) {
			query.setParameter("sourceCode", sourceCode);
		}
		if (targetCode != null) {
			query.setParameter("targetCode", targetCode);
		}
		if (linkCode != null) {
			query.setParameter("linkCode", linkCode);
		}
		if (linkValue != null) {
			query.setParameter("linkValue", linkValue);
		}
		if (linkWeight > 0.0) {
			query.setParameter("linkWeight", linkWeight);
		}
		if (stakeholderCode != null) {
			query.setParameter("stakeholderCode", stakeholderCode);
		}
		if (sourceStakeholderCode != null) {
			query.setParameter("sourceStakeholderCode", sourceStakeholderCode);
		}
		//
		for (SearchSettings filter : filters) {
			for (Tuple2<String, Object> value : filter.valueList) {
				log.debug("Value: " + value._1 + " =: " + value._2);
				if (value._2 instanceof Boolean) {
					Boolean result = (Boolean) value._2;
					query.setParameter(value._1, result);
				} else if (value._2 instanceof LocalDateTime) {
					LocalDateTime result = (LocalDateTime) value._2;
					query.setParameter(value._1, result);
				} else if (value._2 instanceof LocalDate) {
					LocalDate result = (LocalDate) value._2;
					query.setParameter(value._1, result);
				} else if (value._2 instanceof LocalTime) {
					LocalTime result = (LocalTime) value._2;
					query.setParameter(value._1, result);
				} else {
					query.setParameter(value._1, value._2);
				}

			}
		}
		results = query.setHint(QueryHints.HINT_READONLY, true).getResultList();

		// Work out associated Columns
		List<Column> columns = ss.getColumnList();
		List<Column> associatedColumns = new ArrayList<Column>();
		Map<String, Attribute> attributeMap = new ConcurrentHashMap<String, Attribute>();
		Map<String, Integer> columnIndexMap = new ConcurrentHashMap<String, Integer>();

		Integer columnIndex = 0;
		for (Column column : columns) { // TODO: stream
			if (column.linkCode != null) {
				associatedColumns.add(column);
				attributeMap.put(column.fieldCode, findAttributeByCode(column.fieldCode)); // prepare cache to be
																							// quicker
				columnIndexMap.put(column.fieldCode, columnIndex++);
			} else {
				columnIndexMap.put(column.fieldCode, columnIndex++);
			}
		}

		// Set simple sort index for frontend to use
		Integer index = 0;
		if (associatedColumns.isEmpty()) { // for speed
			for (BaseEntity be : results) {
				be.setIndex(index++);
				// Set the baseentity entityAttribute order
				for (EntityAttribute ea : be.getBaseEntityAttributes()) {
					ea.setIndex(columnIndexMap.get(ea.getAttributeCode()));
				}
			}
		} else {
			Map<String, BaseEntity> associatedBaseEntityMap = new ConcurrentHashMap<String, BaseEntity>(); // store
																											// baseentitys
																											// as a map
																											// to speed
																											// up
																											// duplicates
			for (BaseEntity be : results) {
				be.setIndex(index++);
				// Now add any extra associated Attributes
				for (Column associatedColumn : associatedColumns) {
					String requiredAttributeCode = associatedColumn.getFieldCode();
					String requiredAttributeName = associatedColumn.getFieldName();
					Attribute requiredAttribute = attributeMap.get(requiredAttributeCode);
					requiredAttribute.setName(requiredAttributeName); // Override the virtual name

					Set<String> associatedBaseEntityCodes = new HashSet<String>();

					if (associatedColumn.getLinkCode().startsWith("LNK_")) {
						// (1) find if any links have the linkCode requested
						if (associatedColumn.getLinkType().equalsIgnoreCase("CHILD")) {
							List<EntityEntity> associatedLinks = be.getLinks().parallelStream() // convert list to
																								// stream
									.filter(ti -> ti.getPk().getAttribute().getCode()
											.equals(associatedColumn.getLinkCode())) // check
																						// for
																						// linkCode
									.collect(Collectors.toList()); // collect the output and convert streams to a List

							// (2) Now find the associated BaseEntitys
							associatedBaseEntityCodes = associatedLinks.parallelStream() // convert list to stream
									.map(ee -> ee.getLink().getTargetCode()).collect(Collectors.toSet()); // collect the
																											// output
																											// and
																											// convert
																											// streams
																											// to a Set
						} else {
							// find the parents that match the linkCode!
							List<Link> links = findParentLinks(be.getCode(), associatedColumn.getLinkCode());
							// (2) Now find the associated BaseEntitys
							associatedBaseEntityCodes = links.parallelStream() // convert list to stream
									.map(ee -> ee.getSourceCode()).collect(Collectors.toSet()); // collect the output
																								// and convert
																								// streams to a Set

						}

					} else {
						// The associated baseentity is actually contained in an attribute within the be
						// (1) find if any links have the linkCode requested
						String associatedBaseEntityCode = be.getValue(associatedColumn.getLinkCode(), null);
						if (associatedBaseEntityCode != null) {
							associatedBaseEntityCodes.add(associatedBaseEntityCode);
						}

					}

					// (3) Now get the BaseEntitys (so we can get the value of the attributeCode
					// required
					Set<BaseEntity> associatedBaseEntitySet = associatedBaseEntityCodes.parallelStream().map(abe -> {
						BaseEntity foundBe = associatedBaseEntityMap.getOrDefault(abe, findBaseEntityByCode(abe));
						associatedBaseEntityMap.put(abe, foundBe); // don't worry about putting it back in of already
																	// there
						return foundBe;
					}).collect(Collectors.toSet());

					// (3.5) create fake EntityAttributes for any attributes that this baseentity
					// does not have ...
					if (associatedBaseEntitySet.isEmpty()) {
						EntityAttribute virtualEA = new EntityAttribute(be, requiredAttribute, 1.0,
								requiredAttribute.getDefaultValue());
						virtualEA.setAttributeCode(":" + requiredAttributeCode);
						be.getBaseEntityAttributes().add(virtualEA);
						continue;
					}

					// (4) Now create a Set of EntityAttributes
					Set<EntityAttribute> virtualEntityAttributeList = associatedBaseEntitySet.parallelStream()
							.map(abe -> {
								EntityAttribute virtualEA = new EntityAttribute(be, requiredAttribute, 1.0,
										abe.getValue(requiredAttributeCode, requiredAttribute.getDefaultValue()));
								virtualEA.setAttributeCode(abe.getCode() + ":" + requiredAttributeCode);
								return virtualEA;
							}).collect(Collectors.toSet());

					// (5) Add to the be!
					be.getBaseEntityAttributes().addAll(virtualEntityAttributeList);

				}

				// Set the baseentity entityAttribute order
				for (EntityAttribute ea : be.getBaseEntityAttributes()) {
					ea.setIndex(columnIndexMap.get(ea.getAttributeCode()));
				}

			}
		}
		return results;

	}

	/**
	 * @param se
	 * @param ss
	 * @throws NoResultException
	 * @throws IllegalArgumentException
	 */
	private SearchSettings buildQuerySettings(BaseEntity se, Integer filterIndex)
			throws NoResultException, IllegalArgumentException {

		SearchSettings ss = new SearchSettings();
		ss.filterIndex = filterIndex;

		for (EntityAttribute ea : se.getBaseEntityAttributes()) {
			if (ea.getAttributeCode().startsWith("SCH_")) {
				continue;
			} else if (ea.getAttributeCode().startsWith("SRT_")) {
				String sortAttributeCode = ea.getAttributeCode().substring("SRT_".length());
				if (ea.getWeight() == null) {
					ea.setWeight(1000.0); // If no weight is given setting it to be in the last of the sort
				}
				ss.orderList.add(new Order(sortAttributeCode, ea.getValueString().toUpperCase(), ea.getWeight())); // weight

				if (!(sortAttributeCode.equalsIgnoreCase("PRI_CODE")
						|| (sortAttributeCode.equalsIgnoreCase("PRI_CREATED"))
						|| (sortAttributeCode.equalsIgnoreCase("PRI_UPDATED"))
						|| (sortAttributeCode.equalsIgnoreCase("PRI_ID"))
						|| (sortAttributeCode.equalsIgnoreCase("PRI_NAME")))) {
					// specifies
					// the sort
					// order
					String attributeCodeEA = "ea" + ss.filterIndex;
					ss.filterStrings += ",EntityAttribute " + attributeCodeEA;
					ss.filterStringsQ += " and " + attributeCodeEA + ".pk.baseEntity.id=ea.pk.baseEntity.id and "
							+ attributeCodeEA + ".pk.attribute.code='" + sortAttributeCode + "' ";
					if ((ea.getPk() == null) || ea.getPk().getAttribute() == null) {
						Attribute attribute = this.findAttributeByCode(sortAttributeCode);
						ea.getPk().setAttribute(attribute);
					}
					String sortType = "valueString";
					sortType = getSortType(ea);
					ss.attributeCodeMap.put(sortAttributeCode, attributeCodeEA + "." + sortType);
					ss.filterIndex++;
				}
			} else if (ea.getAttributeCode().startsWith("COL_")) {
				String columnAttributeCode = ea.getAttributeCode().substring("COL_".length());
				if (!columnAttributeCode.equalsIgnoreCase("code")) {
					ss.columnList.add(new Column(columnAttributeCode, ea.getAttributeName(), ea.getWeight()));
					ss.attributeCodes.add(columnAttributeCode);
				}
			} else if (ea.getAttributeCode().startsWith("CAL_")) {
				Map<String, String> associatedCodes = QSearchEntityMessage.getAssociationCodes(ea.getAttributeCode());
				String columnAttributeCode = associatedCodes.get("ATTRIBUTE_CODE");

				if (!columnAttributeCode.equalsIgnoreCase("code")) {
					Column calColumn = new Column(columnAttributeCode, ea.getAttributeName(), ea.getWeight());
					calColumn.setLinkCode(associatedCodes.get("LINK_CODE"));
					calColumn.setLinkType(associatedCodes.get("LINK_TYPE"));
					ss.columnList.add(calColumn);
					ss.attributeCodes.add(columnAttributeCode);
				}
			} else {
				String priAttributeCode = ea.getAttributeCode();

				// Check no nasty SQL injection
				// Need named procedures
				// Quick and dirty ; check
				String condition = ea.getAttributeName();
				if (condition != null) {
					condition = SearchEntity.convertFromSaveable(condition);
					final String conditionTest = condition.trim();
					if (!allowedConditions.stream().anyMatch(str -> str.trim().equals(conditionTest))) {
						throw new IllegalArgumentException("Illegal condition!(" + conditionTest + ") ["
								+ ea.getAttributeCode() + "] for user " + getUser());
					}

				}

				String valueString = ea.getValueString();
				if (valueString != null) {
					if (valueString.contains(";")) {
						throw new IllegalArgumentException("Error! Illegal condition!(" + valueString + ") ["
								+ ea.getAttributeCode() + "] for user " + getUser());
					}
				}

				if (priAttributeCode.equalsIgnoreCase("PRI_CODE")) {
					if (ea.getAttributeName() == null) {
						ss.codeFilter += " and ea.pk.baseEntity.code=:v" + ss.filterIndex + " ";
					} else {
						ss.codeFilter += " and ea.pk.baseEntity.code " + condition + " :v" + ss.filterIndex + " ";
					}
					String attributeCodeEA = "ea" + ss.filterIndex;
					ss.filterStrings += ",EntityAttribute " + attributeCodeEA;
					ss.valueList.add(Tuple.of("v" + ss.filterIndex, ea.getValueString()));
					ss.filterIndex++;
				} else if (priAttributeCode.equalsIgnoreCase("PRI_CREATED")) {
					if (ea.getAttributeName() == null) {
						ss.codeFilter += " and ea.pk.baseEntity.created=:v" + ss.filterIndex + " ";
					} else {
						ss.codeFilter += " and ea.pk.baseEntity.created " + condition + " :v" + ss.filterIndex + " ";
					}
					String attributeCodeEA = "ea" + ss.filterIndex;
					ss.filterStrings += ",EntityAttribute " + attributeCodeEA;
					ss.valueList.add(Tuple.of("v" + ss.filterIndex, ea.getValueDateTime()));
					ss.filterIndex++;
				} else {
					String attributeCodeEA = "ea" + ss.filterIndex;
					ss.setPrefix(attributeCodeEA);
					ss.filterStrings += ",EntityAttribute " + attributeCodeEA;
					ss.filterStringsQ += " and " + attributeCodeEA + ".pk.baseEntity.id=ea.pk.baseEntity.id and "
							+ attributeCodeEA + ".pk.attribute.code='" + priAttributeCode + "' ";
					if ((ea.getPk() == null) || ea.getPk().getAttribute() == null) {
						Attribute attribute = this.findAttributeByCode(priAttributeCode);
						ea.getPk().setAttribute(attribute);
					}
					switch (ea.getPk().getAttribute().getDataType().getClassName()) {
					case "java.lang.Integer":
					case "Integer":
						ss.filterStringsQ = updateFilterStringsQ(ss.filterStringsQ, ss.filterIndex, ea, condition,
								attributeCodeEA, "Integer");
						ss.valueList.add(Tuple.of("v" + ss.filterIndex, ea.getValueInteger()));
						ss.attributeCodeMap.put(priAttributeCode, attributeCodeEA + ".valueInteger");
						break;
					case "java.lang.Long":
					case "Long":
						ss.filterStringsQ = updateFilterStringsQ(ss.filterStringsQ, ss.filterIndex, ea, condition,
								attributeCodeEA, "Long");
						ss.valueList.add(Tuple.of("v" + ss.filterIndex, ea.getValueLong()));
						ss.attributeCodeMap.put(priAttributeCode, attributeCodeEA + ".valueLong");
						break;
					case "java.lang.Double":
					case "Double":
						ss.filterStringsQ = updateFilterStringsQ(ss.filterStringsQ, ss.filterIndex, ea, condition,
								attributeCodeEA, "Double");
						ss.valueList.add(Tuple.of("v" + ss.filterIndex, ea.getValueDouble()));
						ss.attributeCodeMap.put(priAttributeCode, attributeCodeEA + ".valueDouble");
						break;
//					case "range.LocalDate":
//						Range<LocalDate> rangeLocalDate = ea.getValueDateRange();
//						ss.filterStringsQ += getHQL(rangeLocalDate, attributeCodeEA, "valueDate", ss.filterIndex,
//								ss.valueList);
//						ss.attributeCodeMap.put(priAttributeCode, attributeCodeEA + ".valueDate");
//						break;
					case "java.lang.Boolean":
					case "Boolean":
						ss.filterStringsQ += " and " + attributeCodeEA + ".valueBoolean=:v" + ss.filterIndex + " ";
						ss.valueList.add(Tuple.of("v" + ss.filterIndex, ea.getValueBoolean()));
						ss.attributeCodeMap.put(priAttributeCode, attributeCodeEA + ".valueBoolean");
						break;
					case "java.time.LocalDate":
					case "LocalDate":
						ss.filterStringsQ = updateFilterStringsQ(ss.filterStringsQ, ss.filterIndex, ea, condition,
								attributeCodeEA, "Date");
						ss.valueList.add(Tuple.of("v" + ss.filterIndex, ea.getValueDate()));
						ss.attributeCodeMap.put(priAttributeCode, attributeCodeEA + ".valueDate");
						break;
					case "java.time.LocalDateTime":
					case "LocalDateTime":
						ss.filterStringsQ = updateFilterStringsQ(ss.filterStringsQ, ss.filterIndex, ea, condition,
								attributeCodeEA, "DateTime");
						ss.valueList.add(Tuple.of("v" + ss.filterIndex, ea.getValueDateTime()));
						ss.attributeCodeMap.put(priAttributeCode, attributeCodeEA + ".valueDate");
						break;
					case "java.time.LocalTime":
					case "LocalTime":
						ss.filterStringsQ = updateFilterStringsQ(ss.filterStringsQ, ss.filterIndex, ea, condition,
								attributeCodeEA, "Time");
						ss.valueList.add(Tuple.of("v" + ss.filterIndex, ea.getValueTime()));
						ss.attributeCodeMap.put(priAttributeCode, attributeCodeEA + ".valueTime");
						break;
					// case "org.javamoney.moneta.Money":
					// return (T) getValueMoney();
					case "java.lang.String":
					case "String":
					default:
						ss.filterStringsQ = updateFilterStringsQ(ss.filterStringsQ, ss.filterIndex, ea, condition,
								attributeCodeEA, "String");
						ss.valueList.add(Tuple.of("v" + ss.filterIndex, ea.getValueString()));
						ss.attributeCodeMap.put(priAttributeCode, attributeCodeEA + ".valueString");
					}
					ss.codeFilter = ss.filterStringsQ;
					ss.filterIndex++;

				}
			}
		}

		ss.filterStringsQ = fixFilterStringsQ(ss.filterStringsQ, ss.filterIndex);
		ss.filterStrings = ss.filterStrings.replaceFirst(",", "");
		return ss;
	}

	public List<BaseEntity> findBySearchBE(@NotNull final BaseEntity searchBE) {
		// select distinct ea.pk.baseEntity from EntityAttribute ea , EntityAttribute eb
		// where
		// ea.attributeCode='PRI_LASTNAME'
		// and ea.valueString like '%CROW%'
		// and eb.pk.baseEntity.code = ea.pk.baseEntity.code
		// and eb.attributeCode='PRI_FIRSTNAME'
		//
		// order by ea.valueString ASC,eb.valueString DESC

		// String attributes have Exact Value matching and SORT ASC/DESC

		// Double attributes have Range matching and Sort ASC/DESC

		// Money attributes have Range matching and Sort ASC/DESC

		// Integer and Long attributes have Range matching and Sort ASC/DESC

		List<BaseEntity> results = null;
		final String userRealmStr = getRealm();

		Integer pageStart = searchBE.getValue("SCH_PAGE_START", 0);
		Integer pageSize = searchBE.getValue("SCH_PAGE_SIZE", 100);
		String stakeholderCode = searchBE.getValue("SCH_STAKEHOLDER_CODE", null);
		String sourceStakeholderCode = searchBE.getValue("SCH_SOURCE_STAKEHOLDER_CODE", null);
		String linkCode = searchBE.getValue("SCH_LINK_CODE", null);
		String linkValue = searchBE.getValue("SCH_LINK_VALUE", null);
		Double linkWeight = searchBE.getValue("SCH_LINK_WEIGHT", 0.0);
		String linkWeightFilter = searchBE.getValue("SCH_LINK_FILTER", ">");
		String sourceCode = searchBE.getValue("SCH_SOURCE_CODE", null);
		String targetCode = searchBE.getValue("SCH_TARGET_CODE", null);

		// remove these because they screw up search , should have been prefixed SCH
		searchBE.removeAttribute("PRI_INDEX");
		searchBE.removeAttribute("PRI_TOTAL_RESULTS"); // ugly
		searchBE.removeAttribute("SCH_INDEX");
		searchBE.removeAttribute("SCH_TOTAL"); // ugly

		// Construct the filters for the attributes
		String filterStrings = "";
		String filterStringsQ = "";
		String orderString = "";
		String codeFilter = "";

		// Check for bad linkWeight filtering
		if (!allowedLinkWeightConditions.stream().anyMatch(str -> str.trim().equals(linkWeightFilter))) {
			log.error("Error! Illegal link Weight condition!(" + linkWeightFilter + ") for user " + getUser());
			return results;
		}

		Integer filterIndex = 0;
		final HashMap<String, String> attributeCodeMap = new HashMap<>();
		final List<Tuple2<String, Object>> valueList = new ArrayList<>();
		final List<Order> orderList = new ArrayList<>(); // attributeCode , ASC/DESC
		final List<Column> columnList = new ArrayList<>(); // column to be searched for and returned
		Set<String> attributeCodes = new HashSet<>();

		for (EntityAttribute ea : searchBE.getBaseEntityAttributes()) {
			if (ea.getAttributeCode().startsWith("SCH_") || ea.getAttributeCode().equals("PRI_INDEX") || ea.getAttributeCode().equals("SCH_INDEX")
					|| ea.getAttributeCode().equals("PRI_TOTAL_RESULTS") || ea.getAttributeCode().equals("SCH_TOTAL")) {
				continue;
			} else if (ea.getAttributeCode().startsWith("SRT_")) {
				String sortAttributeCode = ea.getAttributeCode().substring("SRT_".length());
				if (ea.getWeight() == null) {
					ea.setWeight(1000.0); // If no weight is given setting it to be in the last of the sort
				}
				orderList.add(new Order(sortAttributeCode, ea.getValueString().toUpperCase(), ea.getWeight())); // weight

				if (!(sortAttributeCode.equalsIgnoreCase("PRI_CODE")
						|| sortAttributeCode.equalsIgnoreCase("PRI_CREATED")
						|| sortAttributeCode.equalsIgnoreCase("PRI_UPDATED")
						|| sortAttributeCode.equalsIgnoreCase("PRI_ID")
						|| sortAttributeCode.equalsIgnoreCase("PRI_NAME"))) {
					// specifies
					// the sort
					// order
					String attributeCodeEA = "ea" + filterIndex;
					filterStrings += ",EntityAttribute " + attributeCodeEA;
					filterStringsQ += " and " + attributeCodeEA + ".pk.baseEntity.id=ea.pk.baseEntity.id and "
							+ attributeCodeEA + ".pk.attribute.code='" + sortAttributeCode + "' ";
					if (ea.getPk() == null || ea.getPk().getAttribute() == null) {
						Attribute attribute = this.findAttributeByCode(sortAttributeCode);
						ea.getPk().setAttribute(attribute);
					}
					String sortType = "valueString";
					sortType = getSortType(ea);
					attributeCodeMap.put(sortAttributeCode, attributeCodeEA + "." + sortType);
					filterIndex++;
				}
			} else if (ea.getAttributeCode().startsWith("COL_")) {
				String columnAttributeCode = ea.getAttributeCode().substring("COL_".length());
				if (!columnAttributeCode.equalsIgnoreCase("code")) {
					columnList.add(new Column(columnAttributeCode, ea.getAttributeName(), ea.getWeight()));
					attributeCodes.add(columnAttributeCode);
				}
			} else {
				String priAttributeCode = ea.getAttributeCode();

				// Check no nasty SQL injection
				// Need named procedures
				// Quick and dirty ; check
				String condition = SearchEntity.convertFromSaveable(ea.getAttributeName());
				if (condition != null) {
					final String conditionTest = condition.trim();
					if (!allowedConditions.stream().anyMatch(str -> str.trim().equals(conditionTest))) {
						log.error("Error! Illegal condition!(" + conditionTest + ") [" + ea.getAttributeCode()
								+ "] for user " + getUser());
						return results;
					}
				}
				String valueString = ea.getValueString();
				if (valueString != null) {
					if (valueString.contains(";")) {
						log.error("Error! Illegal condition!(" + valueString + ") [" + ea.getAttributeCode()
								+ "] for user " + getUser());
						return results;
					}
				}

				if (priAttributeCode.equalsIgnoreCase("PRI_CODE")) {
					if (ea.getAttributeName() == null) {
						codeFilter += " and ea.pk.baseEntity.code=:v" + filterIndex + " ";
					} else {
						codeFilter += " and ea.pk.baseEntity.code " + condition + " :v" + filterIndex + " ";
					}
					valueList.add(Tuple.of("v" + filterIndex, ea.getValueString()));
					filterIndex++;
				} else if (priAttributeCode.equalsIgnoreCase("PRI_CREATED")) {
					if (ea.getAttributeName() == null) {
						codeFilter += " and ea.pk.baseEntity.created=:v" + filterIndex + " ";
					} else {
						codeFilter += " and ea.pk.baseEntity.created " + condition + " :v" + filterIndex + " ";
					}
					valueList.add(Tuple.of("v" + filterIndex, ea.getValueDateTime()));
					filterIndex++;
				} else {
					String attributeCodeEA = "ea" + filterIndex;
					filterStrings += ",EntityAttribute " + attributeCodeEA;
					filterStringsQ += " and " + attributeCodeEA + ".pk.baseEntity.id=ea.pk.baseEntity.id and "
							+ attributeCodeEA + ".pk.attribute.code='" + priAttributeCode + "' ";
					if (ea.getPk() == null || ea.getPk().getAttribute() == null) {
						Attribute attribute = this.findAttributeByCode(priAttributeCode);
						ea.getPk().setAttribute(attribute);
					}
					String className = null;

					try {
						className = ea.getPk().getAttribute().getDataType().getClassName();
					} catch (Exception e) {
						log.error("Attribute does not exist in Search query");
						return results;
					}
					switch (className) {
					case "java.lang.Integer":
					case "Integer":
						filterStringsQ = updateFilterStringsQ(filterStringsQ, filterIndex, ea, condition,
								attributeCodeEA, "Integer");
						valueList.add(Tuple.of("v" + filterIndex, ea.getValueInteger()));
						attributeCodeMap.put(priAttributeCode, attributeCodeEA + ".valueInteger");
						break;
					case "java.lang.Long":
					case "Long":
						filterStringsQ = updateFilterStringsQ(filterStringsQ, filterIndex, ea, condition,
								attributeCodeEA, "Long");
						valueList.add(Tuple.of("v" + filterIndex, ea.getValueLong()));
						attributeCodeMap.put(priAttributeCode, attributeCodeEA + ".valueLong");
						break;
					case "java.lang.Double":
					case "Double":
						filterStringsQ = updateFilterStringsQ(filterStringsQ, filterIndex, ea, condition,
								attributeCodeEA, "Double");
						valueList.add(Tuple.of("v" + filterIndex, ea.getValueDouble()));
						attributeCodeMap.put(priAttributeCode, attributeCodeEA + ".valueDouble");
						break;
//					case "range.LocalDate":
//						Range<LocalDate> rangeLocalDate = ea.getValueDateRange();
//						filterStringsQ += getHQL(rangeLocalDate, attributeCodeEA, "valueDate", filterIndex, valueList);
//						attributeCodeMap.put(priAttributeCode, attributeCodeEA + ".valueDate");
//						break;
					case "java.lang.Boolean":
					case "Boolean":
						filterStringsQ += " and " + attributeCodeEA + ".valueBoolean=:v" + filterIndex + " ";
						valueList.add(Tuple.of("v" + filterIndex, ea.getValueBoolean()));
						attributeCodeMap.put(priAttributeCode, attributeCodeEA + ".valueBoolean");
						break;
					case "java.time.LocalDate":
					case "LocalDate":
						filterStringsQ = updateFilterStringsQ(filterStringsQ, filterIndex, ea, condition,
								attributeCodeEA, "Date");
						valueList.add(Tuple.of("v" + filterIndex, ea.getValueDate()));
						attributeCodeMap.put(priAttributeCode, attributeCodeEA + ".valueDate");
						break;
					case "java.time.LocalDateTime":
					case "LocalDateTime":
						filterStringsQ = updateFilterStringsQ(filterStringsQ, filterIndex, ea, condition,
								attributeCodeEA, "DateTime");
						valueList.add(Tuple.of("v" + filterIndex, ea.getValueDateTime()));
						attributeCodeMap.put(priAttributeCode, attributeCodeEA + ".valueDate");
						break;
					case "java.time.LocalTime":
					case "LocalTime":
						filterStringsQ = updateFilterStringsQ(filterStringsQ, filterIndex, ea, condition,
								attributeCodeEA, "Time");
						valueList.add(Tuple.of("v" + filterIndex, ea.getValueTime()));
						attributeCodeMap.put(priAttributeCode, attributeCodeEA + ".valueTime");
						break;
					case "java.lang.String":
					case "String":
					default:
						filterStringsQ = updateFilterStringsQ(filterStringsQ, filterIndex, ea, condition,
								attributeCodeEA, "String");
						valueList.add(Tuple.of("v" + filterIndex, ea.getValueString()));
						attributeCodeMap.put(priAttributeCode, attributeCodeEA + ".valueString");
					}
					filterIndex++;

				}
			}
		}

		filterStringsQ = fixFilterStringsQ(filterStringsQ, filterIndex);

		Set<String> realms = new HashSet<>();

		realms.add(getRealm());

		realms.add("genny");
		String realmsStr = getRealmsStr(realms);

		orderString = createOrderString(attributeCodeMap, orderList);

		String sql = createSearchSQL("distinct ea.pk.baseEntity", stakeholderCode, sourceStakeholderCode, linkCode,
				linkValue, linkWeight, linkWeightFilter, sourceCode, targetCode, filterStrings, filterStringsQ,
				orderString, codeFilter, realmsStr);

		Query query = null;

		// Limit column attributes returned using Hibernate filter
		// If empty then return everything
		if (!attributeCodes.isEmpty()) {
			Filter filter = getEntityManager().unwrap(Session.class).enableFilter("filterAttribute");
			filter.setParameterList("attributeCodes", attributeCodes);
		}

		query = getEntityManager().createQuery(sql);

		query.setFirstResult(pageStart).setMaxResults(pageSize);

		// query.setParameter("realms", realms); This would not work

		if (sourceCode != null) {
			query.setParameter("sourceCode", sourceCode);
		}
		if (targetCode != null) {
			query.setParameter("targetCode", targetCode);
		}
		if (linkCode != null) {
			query.setParameter("linkCode", linkCode);
		}
		if (linkValue != null) {
			query.setParameter("linkValue", linkValue);
		}
		if (linkWeight > 0.0) {
			query.setParameter("linkWeight", linkWeight);
		}
		if (stakeholderCode != null) {
			query.setParameter("stakeholderCode", stakeholderCode);
		}
		if (sourceStakeholderCode != null) {
			query.setParameter("sourceStakeholderCode", sourceStakeholderCode);
		}
		//
		for (Tuple2<String, Object> value : valueList) {
			log.debug("Value: " + value._1 + " =: " + value._2);
			if (value._2 instanceof Boolean) {
				Boolean result = (Boolean) value._2;
				query.setParameter(value._1, result);
			} else if (value._2 instanceof LocalDateTime) {
				LocalDateTime result = (LocalDateTime) value._2;
				query.setParameter(value._1, result);
			} else if (value._2 instanceof LocalDate) {
				LocalDate result = (LocalDate) value._2;
				query.setParameter(value._1, result);
			} else if (value._2 instanceof LocalTime) {
				LocalTime result = (LocalTime) value._2;
				query.setParameter(value._1, result);
			} else {
				query.setParameter(value._1, value._2);
			}
		}
		results = query.setHint(QueryHints.HINT_READONLY, true).getResultList();

		// Set simple sort index for frontend to use
		Integer index = 0;
		for (BaseEntity be : results) {
			be.setIndex(index++);
		}
		return results;

	}

	/**
	 * @param filterStringsQ
	 * @param filterIndex
	 * @param ea
	 * @param condition
	 * @param attributeCodeEA
	 * @param typeName
	 * @return
	 */
	private String updateFilterStringsQ(String filterStringsQ, Integer filterIndex, EntityAttribute ea,
			String condition, String attributeCodeEA, String typeName) {
		if (ea.getAttributeName() == null) {
			String newString = " and " + attributeCodeEA + ".value" + typeName + "=:v" + filterIndex + " ";
			if (!filterStringsQ.contains(newString)) { // don't double up
				filterStringsQ +=  newString; 
			}
		} else {
			if (condition.equalsIgnoreCase(SearchEntity.Filter.BIT_MASK_POSITIVE.toString())) {
				filterStringsQ += " and (bitwise_and(" + attributeCodeEA + ".value" + typeName + " , :v" + filterIndex
						+ ") <> 0) ";
			} else if (condition.equalsIgnoreCase(SearchEntity.Filter.BIT_MASK_ZERO.toString())) {
				filterStringsQ += " and (bitwise_and(" + attributeCodeEA + ".value" + typeName + " , :v" + filterIndex
						+ ") = 0) ";
			} else {
				String newString = filterStringsQ += " and " + attributeCodeEA + ".value" + typeName + " " + condition + " :v"
						+ filterIndex + " ";
				if (!filterStringsQ.contains(newString)) { // don't double up
					filterStringsQ +=  newString; 
				}

			}
		}
		return filterStringsQ;
	}

	/**
	 * @param stakeholderCode
	 * @param sourceStakeholderCode
	 * @param linkCode
	 * @param linkValue
	 * @param sourceCode
	 * @param targetCode
	 * @param filterStrings
	 * @param filterStringsQ
	 * @param orderString
	 * @param codeFilter
	 * @param realmsStr
	 * @return
	 */
	private String createSearchSQL(String prefix, String stakeholderCode, String sourceStakeholderCode, String linkCode,
			String linkValue, Double linkWeight, String linkWeightFilter, String sourceCode, String targetCode,
			String filterStrings, String filterStringsQ, String orderString, String codeFilter, String realmsStr) {

		if (!StringUtils.isBlank(filterStrings)) {
			if (!filterStrings.startsWith(",")) {
				filterStrings = "," + filterStrings;
			}
		}

		filterStringsQ = filterStringsQ.replace("(", "").replace(")","").replaceAll("  ", " ");
		codeFilter = codeFilter.replaceAll("  ", " ");
		
		
		String sql = "select " + prefix + " from EntityAttribute ea "
				+ (stakeholderCode != null ? " ,EntityEntity ff " : "")
				+ (sourceStakeholderCode != null ? " ,EntityEntity gg " : "")
				// + " EntityAttribute ea JOIN be.baseEntityAttributes bea,"
				+ (sourceCode != null || targetCode != null || linkCode != null || linkValue != null
						? " ,EntityEntity ee  "
						: "")
				+ filterStrings + " where " + " ea.pk.baseEntity.realm in (" + realmsStr + ")  " + codeFilter
				+ (linkCode != null ? " and ee.link.attributeCode=:linkCode and " : "")
				+ (linkValue != null ? " and ee.link.linkValue=:linkValue and " : "")
				+ (linkWeight > 0.0 ? " and ee.link.weight " + linkWeightFilter + " :linkWeight and " : "")
				+ (sourceCode != null
						? " and ee.pk.source.code=:sourceCode and ee.pk.targetCode=ea.pk.baseEntity.code and "
						: "")
				+ (targetCode != null
						? " and ee.pk.targetCode=:targetCode and ee.pk.source.code=ea.pk.baseEntity.code and "
						: "")
				+ (stakeholderCode != null
						? " and ((ff.pk.targetCode=:stakeholderCode and ff.pk.source.code=ea.pk.baseEntity.code) or (ff.pk.source.code=:stakeholderCode and ff.pk.targetCode=ea.pk.baseEntity.code)  ) "
						: "")
				+ (sourceStakeholderCode != null
						? " and ((gg.pk.targetCode=:sourceStakeholderCode and gg.pk.source.code=ee.pk.source.code) or (gg.pk.targetCode=:sourceStakeholderCode and gg.pk.targetCode=ee.pk.source.code)  ) "
						: "")
				+ ((!codeFilter.contains(filterStringsQ ))?filterStringsQ:" ")
				+ orderString;

		// Ugly
		if (StringUtils.isBlank(orderString)) {
			sql = sql.trim();
			if (sql.endsWith("and")) {
				sql = sql.substring(0, sql.length() - 3);
			}
		} else {
			sql = sql.trim();
			sql = sql.replaceAll("and  order", " order"); // omg this is ugly

		}

		sql = sql.replaceAll("and  and", "and"); // even more ugly...

		log.info("SQL =[" + sql + "]");

		return sql;
	}

	/**
	 * @param stakeholderCode
	 * @param sourceStakeholderCode
	 * @param linkCode
	 * @param linkValue
	 * @param sourceCode
	 * @param targetCode
	 * @param filterStrings
	 * @param filterStringsQ
	 * @param orderString
	 * @param codeFilter
	 * @param realmsStr
	 * @return
	 */
	private String createSearchSQL(String prefix, String stakeholderCode, String sourceStakeholderCode, String linkCode,
			String linkValue, Double linkWeight, String linkWeightFilter, String sourceCode, String targetCode,
			String filterStrings, String filterStringsQ, String orderString, List<SearchSettings> filters,
			String realmsStr) {
		String sql = "select " + prefix + " from " + filterStrings + " " // EntityAttribute ea "
				+ ((stakeholderCode != null) ? " ,EntityEntity ff " : "")
				+ ((sourceStakeholderCode != null) ? " ,EntityEntity gg " : "")
				// + " EntityAttribute ea JOIN be.baseEntityAttributes bea,"
				+ (((sourceCode != null) || (targetCode != null) || (linkCode != null) || (linkValue != null))
						? " ,EntityEntity ee  "
						: "")
				+ filterStrings + " where " + " ea.pk.baseEntity.realm in (" + realmsStr + ") ";
		if (!filters.isEmpty()) {
			sql += "and (";
			for (SearchSettings filter : filters) {
				String codeFilter = StringUtils.removeStart(filter.codeFilter, " and ");
				sql += "(" + codeFilter + ") or ";
			}
			sql = StringUtils.removeEnd(sql, "or ");
			sql += " ) ";
		}

		sql += ((linkCode != null) ? " and ee.link.attributeCode=:linkCode and " : "")
				+ ((linkValue != null) ? " and ee.link.linkValue=:linkValue and " : "")
				+ ((linkWeight > 0.0) ? " and ee.link.weight " + linkWeightFilter + " :linkWeight and " : "")
				+ ((sourceCode != null)
						? " and ee.pk.source.code=:sourceCode and ee.pk.targetCode=ea.pk.baseEntity.code and "
						: "")
				+ ((targetCode != null)
						? " and ee.pk.targetCode=:targetCode and ee.pk.source.code=ea.pk.baseEntity.code and "
						: "")
				+ ((stakeholderCode != null)
						? " and ((ff.pk.targetCode=:stakeholderCode and ff.pk.source.code=ea.pk.baseEntity.code) or (ff.pk.source.code=:stakeholderCode and ff.pk.targetCode=ea.pk.baseEntity.code)  ) "
						: "")
				+ ((sourceStakeholderCode != null)
						? " and ((gg.pk.targetCode=:sourceStakeholderCode and gg.pk.source.code=ee.pk.source.code) or (gg.pk.targetCode=:sourceStakeholderCode and gg.pk.targetCode=ee.pk.source.code)  ) "
						: "")
				+ filterStringsQ + orderString;

		// Ugly
		if (StringUtils.isBlank(orderString)) {
			sql = sql.trim();
			if (sql.endsWith("and")) {
				sql = sql.substring(0, sql.length() - 3);
			}
		} else {
			sql = sql.trim();
			sql = sql.replaceAll("and  order", " order"); // omg this is ugly

		}

		sql = sql.replaceAll("and  and", "and"); // even more ugly...

		log.info("SQL =[" + sql + "]");

		return sql;
	}

	/**
	 * @param filterStringsQ
	 * @param filterIndex
	 * @return
	 */
	private String fixFilterStringsQ(String filterStringsQ, Integer filterIndex) {
		if (filterIndex > 0) {
			// This is ugly
			filterStringsQ = filterStringsQ.trim();
			if (filterStringsQ.startsWith("and")) {
				filterStringsQ = filterStringsQ.substring("and".length());
			}
			if (!StringUtils.isBlank(filterStringsQ)) {
				filterStringsQ = " and (" + filterStringsQ.substring(0, filterStringsQ.length()) + ")  ";
			}
		}
		return filterStringsQ;
	}

	/**
	 * @param ea
	 * @return
	 */
	private String getSortType(EntityAttribute ea) {
		String sortType;
		switch (ea.getPk().getAttribute().getDataType().getClassName()) {
		case "java.time.LocalTime":
		case "LocalTime":
			sortType = "valueTime";
			break;
		case "java.time.LocalDate":
		case "LocalDate":
			sortType = "valueDate";
			break;
		case "java.time.LocalDateTime":
		case "LocalDateTime":
			sortType = "valueDateTime";
			break;
		case "java.lang.Boolean":
		case "Boolean":
			sortType = "valueBoolean";
			break;
		case "java.lang.Double":
		case "Double":
			sortType = "valueDouble";
			break;
		case "java.lang.Long":
		case "Long":
			sortType = "valueLong";
			break;
		case "java.lang.Integer":
		case "Integer":
			sortType = "valueInteger";
			break;
		case "java.lang.String":
		case "String":
		default:
			sortType = "valueString";
		}
		return sortType;
	}

	private String createOrderString(HashMap<String, String> attributeCodeMap, List<Order> orderList) {
		if (orderList.isEmpty()) {
			return "";
		}
		String ret = " order by ";
		// Sort
		Collections.sort(orderList, new OrderCompare());

		for (Order order : orderList) {
			String sqlAttribute = null;
			if (order.getFieldName().equalsIgnoreCase("PRI_CREATED")) {
				log.info(">>> Reached CreateOrderString with PRI_CREATED ");
				sqlAttribute = " ea.pk.baseEntity.created";
			} else if (order.getFieldName().equalsIgnoreCase("PRI_CODE")) {
				log.info(">>> Reached CreateOrderString with PRI_CODE ");
				sqlAttribute = " ea.pk.baseEntity.code";
			} else if (order.getFieldName().equalsIgnoreCase("PRI_UPDATED")) {
				log.info(">>> Reached CreateOrderString with PRI_UPDATED ");
				sqlAttribute = " ea.pk.baseEntity.updated";
			} else if (order.getFieldName().equalsIgnoreCase("PRI_ID")) {
				log.info(">>> Reached CreateOrderString with PRI_ID ");
				sqlAttribute = " ea.pk.baseEntity.id";
			} else if (order.getFieldName().equalsIgnoreCase("PRI_NAME")) {
				log.info(">>> Reached CreateOrderString with PRI_NAME ");
				sqlAttribute = " ea.pk.baseEntity.name";
			} else {
				sqlAttribute = attributeCodeMap.get(order.getFieldName());
			}
			if (sqlAttribute != null) {
				ret += sqlAttribute + " " + order.getAscdesc() + ",";
			} else {
				log.debug("ERROR - Cannot map " + order.getFieldName());
			}
		}
		ret = ret.substring(0, ret.length() - 1);

		return ret;
	}

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
			attribute = findAttributeByCode(Attribute.getDefaultCodePrefix() + attributeName);
		} catch (final NoResultException e) {

			attribute = new AttributeText(Attribute.getDefaultCodePrefix() + attributeName,
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
	 * Remove {@link BaseEntity} one by one and throws an exception at a given point
	 * to simulate a real error and test Transaction bahaviour
	 *
	 * @throws IllegalStateException when removing {@link BaseEntity} at given index
	 */
	public void remove(final BaseEntity entity) {
		resetMsgLists();
		final BaseEntity baseEntity = findBaseEntityById(entity.getId());
		getEntityManager().remove(baseEntity);
	}

	/**
	 * Remove {@link BaseEntity} one by one and throws an exception at a given point
	 * to simulate a real error and test Transaction bahaviour
	 *
	 * @throws IllegalStateException when removing {@link BaseEntity} at given index
	 */
	// @Transactional
	public void removeBaseEntity(final String code) {
		final BaseEntity be = findBaseEntityByCode(code);
		if (be != null) {

			// remove all answers

			// remove all answerlinks
			List<AnswerLink> answers = findAnswersByTargetOrSourceBaseEntityCode(be.getCode());
			log.info("Answers count: " + answers.size());

			// remove all attributes - It is automatically removed by Hibernate.

			// remove all links
			List<EntityEntity> links = findLinksBySourceOrTargetBaseEntityCode(be.getCode());
			log.info("Links count: " + links.size());
			// removeEntityEntity(be.getCode());

			for (final AnswerLink answerLink : answers) {
				removeAnswerLink(code, answerLink);
			}

			for (final EntityEntity entityEntity : links) {
				removeEntityEntity(code, entityEntity);
			}

			// remove the be
			getEntityManager().remove(be);
			// clear cache
			writeToDDT(code, null);
		}
	}

	/**
	 * Remove {@link Attribute} one by one and throws an exception at a given point
	 * to simulate a real error and test Transaction bahaviour
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

	// Override as required
	public void sendQEventLinkChangeMessage(final QEventLinkChangeMessage event) {
		log.info("Send Link Change:" + event);
	}

	public void sendQEventSystemMessage(final String systemCode) {
		log.info("Send System Code");
	}

	public void sendQEventSystemMessage(final String systemCode, final String token) {
		log.info("Send System Code");
	}

	public void sendQEventSystemMessage(final String systemCode, final Properties properties, final String token) {
		log.info("Send System Code");
	}

	protected String getCurrentToken() {
		return "DUMMY_TOKEN";
	}

	private List<String> commitMsg = new ArrayList<>();

	private List<String> rollbackMsg = new ArrayList<>();

	public static String set(final Object item) {

		final ObjectMapper mapper = new ObjectMapper();

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

	// @Transactional
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
				newAsk = new Ask(attribute.getCode(), beSource.getCode(), beTarget.getCode(), attribute.getName());
			}

			newAsk.setRealm(getRealm());

			Log.info("Creating new Ask " + beSource.getCode() + ":" + beTarget.getCode() + ":" + attribute.getCode()
					+ ":" + (question == null ? "No Question" : question.getCode()));
			List<Ask> existingList = findAsksByRawAsk(newAsk);
			if (existingList.isEmpty()) {
				getEntityManager().persist(newAsk);
				ask.setId(newAsk.getId());
			} else {
				ask.setId(existingList.get(0).getId());
			}

		} catch (final ConstraintViolationException e) {
			Ask existing = findAskById(ask.getId());

			existing.setRealm(getRealm());

			existing = getEntityManager().merge(existing);
			return existing.getId();
		} catch (final PersistenceException e) {
			log.error("Cannot save ask with id=[" + ask.getId() + " , already in system " + e.getLocalizedMessage());
			List<Ask> existingList = findAsksByRawAsk(ask);
			if (existingList.isEmpty()) { // TODO
				return 0L;
			}
			Ask existing = existingList.get(0);
			return existing.getId();

		} catch (final IllegalStateException e) {
			Ask existing = findAskById(ask.getId());

			existing.setRealm(getRealm());

			existing = getEntityManager().merge(existing);
			return existing.getId();
		}
		return ask.getId();
	}

	// @Transactional
	public Long insert(final GPS entity) {
		try {
			getEntityManager().persist(entity);

		} catch (final EntityExistsException e) {
			GPS existing = findGPSById(entity.getId());
			existing = getEntityManager().merge(existing);
			return existing.getId();

		}
		return entity.getId();
	}

	// @Transactional
	public Long insert(final Question question) {
		// always check if question exists through check for unique code
		try {

			question.setRealm(getRealm());

			getEntityManager().persist(question);
			log.debug("Loaded " + question.getCode());
		} catch (final ConstraintViolationException e) {
			Question existing = findQuestionByCode(question.getCode());

			existing.setRealm(getRealm());

			existing = getEntityManager().merge(existing);
			return existing.getId();
		} catch (final PersistenceException e) {
			Question existing = findQuestionByCode(question.getCode());

			existing.setRealm(getRealm());

			existing = getEntityManager().merge(existing);
			return existing.getId();
		} catch (final IllegalStateException e) {
			Question existing = findQuestionByCode(question.getCode());

			existing.setRealm(getRealm());

			existing = getEntityManager().merge(existing);
			return existing.getId();
		} catch (Exception ex) {
			log.error("Invalid questionCode:" + question.getCode() + ", errorMsg:" + ex.getMessage());
			throw ex;
		}
		return question.getId();
	}

	// @Transactional
	public Long insert(final Rule rule) {
		// always check if rule exists through check for unique code
		try {

			rule.setRealm(getRealm());

			getEntityManager().persist(rule);

		} catch (final EntityExistsException e) {
			Rule existing = findRuleById(rule.getId());

			existing.setRealm(getRealm());

			existing = getEntityManager().merge(existing);
			return existing.getId();

		}
		return rule.getId();
	}

	// @Transactional
	public Long insert(final Validation validation) {
		// always check if rule exists through check for unique code
		try {
			log.debug("______________________________________");

			validation.setRealm(getRealm());

			getEntityManager().persist(validation);
			log.debug("Loaded Validation " + validation.getCode());
		} catch (final ConstraintViolationException e) {
			log.debug("\n\n\n\n\n\n222222222222\n\n\n\n\n\n\n\n\n");
			final Validation existing = findValidationByCode(validation.getCode());
			log.debug("\n\n\n\n\n\n" + existing + "\n\n\n\n\n\n\n\n\n");
			return existing.getId();
		} catch (final PersistenceException e) {
			log.debug("\n\n\n\n\n\n22222***" + validation.getCode() + "****2222222\n\n\n\n\n\n\n\n\n");
			final Validation existing = findValidationByCode(validation.getCode());
			log.debug("\n\n\n\n\n\n" + existing.getRegex() + "\n\n\n\n\n\n\n\n\n");
			return existing.getId();
		} catch (final IllegalStateException e) {
			log.debug("\n\n\n\n\n\n222222222222\n\n\n\n\n\n\n\n\n");
			final Validation existing = findValidationByCode(validation.getCode());
			log.debug("\n\n\n\n\n\n" + existing + "\n\n\n\n\n\n\n\n\n");
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
			existing = null;
		}
		if (existing == null) {

			try {
				getEntityManager().persist(answerLink);
			} catch (Exception e) {
				log.error("Eror in persisting answerlink");
			}

			QEventAttributeValueChangeMessage msg = new QEventAttributeValueChangeMessage(answerLink.getSourceCode(),
					answerLink.getTargetCode(), answerLink.getAttributeCode(), null, answerLink.getValue(),
					getCurrentToken());

			sendQEventAttributeValueChangeMessage(msg);
		}

		else {
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

			QEventAttributeValueChangeMessage msg = new QEventAttributeValueChangeMessage(answerLink.getSourceCode(),
					answerLink.getTargetCode(), answerLink.getAttributeCode(), oldValue.toString(),
					answerLink.getValue(), token);

			sendQEventAttributeValueChangeMessage(msg);

			return existing;

		}
		return answerLink;
	}

	// @Transactional(dontRollbackOn = { PersistenceException.class })
	public Long insert(BaseEntity entity) {

		// always check if baseentity exists through check for unique code
		try {
			if (StringUtils.isBlank(entity.getName())) {
				entity.setName(entity.getCode());
			}
			entity.setRealm(getRealm());

			getEntityManager().persist(entity);
			String json = JsonUtils.toJson(entity);
			writeToDDT(entity.getCode(), json);
		} catch (javax.validation.ConstraintViolationException e) {
			log.error("Cannot save BaseEntity with code " + entity.getCode() + "! " + e.getLocalizedMessage());
			return -1L;
		} catch (final ConstraintViolationException e) {
			log.error("Entity Already exists - cannot insert, so updationg existing" + entity.getCode());
			// fetch existing entity , save the new name and eentitystatus and return the id
			BaseEntity existing = findBaseEntityByCode(entity.getCode(), false);
			existing.setName(entity.getName());
			existing.setStatus(entity.getStatus());
			getEntityManager().persist(existing);
			return existing.getId();
		} catch (final PersistenceException e) {
			return entity.getId();
		} catch (final IllegalStateException e) {
			return entity.getId();
		}
		return entity.getId();
	}

	public Long insert(Answer[] answers) throws IllegalArgumentException {
		long insertStartMs = System.nanoTime();
		;

		// always check if answer exists through check for unique code
		BaseEntity beTarget = null;
		BaseEntity beSource = null;
		Attribute attribute = null;
		Ask ask = null;

		if (answers.length == 0) {
			return -1L;
		}

		if (answers[0].getTargetCode() == null) {
			Answer ans = answers[0];

			log.error("NULL TARGET CODE IN ANSWERS[0]" + ans.getSourceCode() + ":" + ans.getTargetCode() + ":"
					+ ans.getAttributeCode());
			throw new IllegalArgumentException("NULL TARGET CODE IN ANSWERS[0]" + ans.getSourceCode() + ":"
					+ ans.getTargetCode() + ":" + ans.getAttributeCode());
		}

		// The target and source are the same for all the answers
		try {
			beTarget = findBaseEntityByCode(answers[0].getTargetCode());
			beSource = findBaseEntityByCode(answers[0].getSourceCode());
		} catch (NoResultException e1) {
			log.error("BAD TARGET/SOURCE CODE IN ANSWERS[0]" + answers[0].getSourceCode() + ":"
					+ answers[0].getTargetCode());
			throw new IllegalArgumentException("BAD TARGET/SOURCE CODE IN ANSWERS[0]" + answers[0].getSourceCode() + ":"
					+ answers[0].getTargetCode());

		}

		BaseEntity safeBe = new BaseEntity(beTarget.getCode(), beTarget.getName());
		Set<EntityAttribute> safeSet = new HashSet<>();
		safeBe.setBaseEntityAttributes(safeSet);
		// Add Links
		safeBe.setLinks(beTarget.getLinks());
		QEventAttributeValueChangeMessage msg = new QEventAttributeValueChangeMessage(beSource.getCode(),
				beTarget.getCode(), safeBe, getCurrentToken());
		msg.setBe(safeBe);
		Boolean entityChanged = false;

		for (Answer answer : answers) {
			long answerStartMs = System.nanoTime();
			;
			try {
				Optional<EntityAttribute> optExisting = beTarget.findEntityAttribute(answer.getAttributeCode());
				Object old = null;

				if (optExisting.isPresent()) {
					EntityAttribute existing = optExisting.get();
					old = existing.getValue();
					if (existing.getReadonly()) {
						// do not save!
						log.error("Trying to save an answer to a readonly entityattribute! " + existing);
						continue;
					}
				}
				
				if ((answer.getAttributeCode().equals("PRI_SUBMIT")&&(answer.getAttributeCode().equals("QQQ_QUESTION_GROUP")))) {
					log.error("Trying to save a submit button!");
					continue;
				}
				
				// log.info("Answer processing 1 = "+((System.nanoTime() - answerStartMs) /
				// 1e6)+" ms");
				// check that the codes exist
				try {
					attribute = findAttributeByCode(answer.getAttributeCode());
				} catch (Exception e) {
					log.error("@@@@@@@@@@@@@ Attribute not found in attribute table when saving answers- "
							+ answer.getAttributeCode());
				}
//				} catch (NoResultException nre) {
//					log.error("Attribute not found in attribute table - "+answer.getAttributeCode());
//					try {
//						attribute = findAttributeLinkByCode(answer.getAttributeCode());
//					}
//					catch (NoResultException nre2) {
//						log.error("Attribute not found in attribute link table - "+answer.getAttributeCode());
//						
//					}
//				}
				if (attribute == null && (answer.getAttributeCode().startsWith("SRT_")
						|| answer.getAttributeCode().startsWith("SCH_")|| answer.getAttributeCode().startsWith("SER_"))) {
					attribute = new AttributeText(answer.getAttributeCode(), answer.getAttributeCode());
					attribute.setRealm(getRealm());
					getEntityManager().persist(attribute);
					log.info("Answer processing 2.1 = " + ((System.nanoTime() - answerStartMs) / 1e6)
							+ " ms - saving SEARCH Attribute " + attribute.getCode());
				}
				if (attribute == null) {

					if (answer.getAttributeCode().startsWith("PRI_IS_")) {
						attribute = new AttributeBoolean(answer.getAttributeCode(),
								StringUtils.capitalize(answer.getAttributeCode().substring(4).toLowerCase()));
					} else if (answer.getAttributeCode().startsWith("FLD_")) {
						// SAM BE mappings
						attribute = new AttributeText(answer.getAttributeCode(),
								StringUtils.capitalize(answer.getAttributeCode().substring(4).toLowerCase()));
					} else {
						if (answer.getDataType() != null) {
							switch (answer.getDataType()) {
							case "java.lang.Integer":
							case "Integer":
								attribute = new AttributeInteger(answer.getAttributeCode(),
										StringUtils.capitalize(answer.getAttributeCode().substring(4).toLowerCase()));
								break;
							case "java.time.LocalDateTime":
							case "LocalDateTime":
								attribute = new AttributeDateTime(answer.getAttributeCode(),
										StringUtils.capitalize(answer.getAttributeCode().substring(4).toLowerCase()));
								break;
							case "java.lang.Long":
							case "Long":
								attribute = new AttributeLong(answer.getAttributeCode(),
										StringUtils.capitalize(answer.getAttributeCode().substring(4).toLowerCase()));
								break;
							case "java.time.LocalTime":
							case "LocalTime":
								attribute = new AttributeTime(answer.getAttributeCode(),
										StringUtils.capitalize(answer.getAttributeCode().substring(4).toLowerCase()));
								break;
							case "org.javamoney.moneta.Money":
							case "Money":
								attribute = new AttributeMoney(answer.getAttributeCode(),
										StringUtils.capitalize(answer.getAttributeCode().substring(4).toLowerCase()));
								break;

							case "java.lang.Double":
							case "Double":
								attribute = new AttributeDouble(answer.getAttributeCode(),
										StringUtils.capitalize(answer.getAttributeCode().substring(4).toLowerCase()));
								break;

							case "java.lang.Boolean":
								attribute = new AttributeBoolean(answer.getAttributeCode(),
										StringUtils.capitalize(answer.getAttributeCode().substring(4).toLowerCase()));
								break;

							case "java.time.LocalDate":
								attribute = new AttributeDate(answer.getAttributeCode(),
										StringUtils.capitalize(answer.getAttributeCode().substring(4).toLowerCase()));
								break;

							case "java.lang.String":
							default:
								attribute = new AttributeText(answer.getAttributeCode(),
										StringUtils.capitalize(answer.getAttributeCode().substring(4).toLowerCase()));
								break;

							}
						} else {
							attribute = new AttributeText(answer.getAttributeCode(),
									StringUtils.capitalize(answer.getAttributeCode().substring(4).toLowerCase()));
						}
					}
					attribute.setRealm(getRealm());
					insert(attribute);
					log.info("Answer processing 2.2 = " + ((System.nanoTime() - answerStartMs) / 1e6)
							+ " ms - saving SEARCH Attribute " + attribute.getCode());
				} else {

				}

				answer.setAttribute(attribute);
				if (answer.getAskId() != null) {
					ask = findAskById(answer.getAskId());
					if (ask != null) {
						if (!(answer.getSourceCode().equals(ask.getSourceCode())
									&& answer.getAttributeCode().equals(ask.getAttributeCode())
									&& answer.getTargetCode().equals(ask.getTargetCode()))) {
							log.error("Answer codes do not match Ask codes! " + answer);
							// return -1L; // need to throw error
							}
					} else {
						log.error(ANSIColour.RED+"BAD BAD BAD BAD, PLEASE LOOK INTO THIS IMMEDIATELY!!!! Ask is null for " + answer.getAskId()+ANSIColour.RESET);
					}
					// log.info("Answer processing 3.1 = "+((System.nanoTime() - answerStartMs) /
					// 1e6)+" ms - ASK ID "+answer.getAskId());

				} else {
					// log.info("Answer processing 3.0 = "+((System.nanoTime() - answerStartMs) /
					// 1e6)+" ms - NO ASK");
				}

				if (answer.getChangeEvent()) {
					msg.getBe().addAnswer(answer);
					msg.setAnswer(answer);
				}
				// log.info("Answer processing 3.5 = "+((System.nanoTime() - answerStartMs) /
				// 1e6)+" ms - Before find Answer");
				// List<Answer> existingList = findAnswersByRawAnswer(answer);
				// if (existingList.isEmpty()) {
				try {
					if ((!answer.getAttributeCode().startsWith("RUL_"))
							&& (!answer.getAttributeCode().startsWith("PRI_HASHCODE"))
							&& (!answer.getAttributeCode().startsWith("PRI_KIE_TEXT"))) {
						// getEntityManager().persist(answer);
					}
					// log.info("Answer processing 4 = "+((System.nanoTime() - answerStartMs) /
					// 1e6)+" ms - Persisted Answer");

				} catch (Exception ee) {
					// log.warn("Answer already exists 4 "+((System.nanoTime() - answerStartMs) /
					// 1e6)+" " + answer.getRealm()+":"+
					// answer.getSourceCode()+":"+answer.getTargetCode()+":"+answer.getAttributeCode());
					// answer.setId(existingList.get(0).getId());

//						Answer existing = existingList.get(0);

//						existing.setChangeEvent(answer.getChangeEvent());
//						existing.setExpired(answer.getExpired());
//						existing.setInferred(answer.getInferred());
//						existing.setRefused(answer.getRefused());
//						existing.setValue(answer.getValue());
//						existing.setWeight(answer.getWeight());
//						answer = getEntityManager().merge(existing);
				}

				// Check if answer represents a link only
				if (attribute.getDataType().getClassName().startsWith("DTT_LINK_")) {
					// add a link
					EntityEntity ee = addLink(answer.getValue(), answer.getTargetCode(),
							attribute.getDataType().getTypeName(), "ANSWER", answer.getWeight());
					msg.getBe().getLinks().add(ee);
					log.info("Answer processing 5 = " + ((System.nanoTime() - answerStartMs) / 1e6)
							+ " ms - Represents a Link");

				} else {

					// update answerlink

					AnswerLink answerLink = null;
					try {

						answerLink = beTarget.addAnswer(beSource, answer, answer.getWeight());
						// log.info("Answer processing 6 = "+((System.nanoTime() - answerStartMs) /
						// 1e6)+" ms - Answer Link");

						if (answer.getAttributeCode().equalsIgnoreCase("PRI_NAME")) {
						//name can not be null or empty string
							if (answer.getValue() != null && !answer.getValue().equals("")) {
								beTarget.setName(answer.getValue());
							} else {
								log.error(String.format("Answer value is " +  answer.getValue() + " , won't set target name. " +
												"Answer target code:%s, source code:%s, attributeCode:%s. " +
												"TargetBaseEntity Code: %s, name:%s.",
												answer.getTargetCode(),
												answer.getSourceCode(),
												answer.getAttributeCode(),
												beTarget.getCode(),
												beTarget.getName()
												));
							}
						}

						boolean sendAttributeChangeEvent = false;
						if (!optExisting.isPresent()) {
							sendAttributeChangeEvent = true;
						}
						if (optExisting.isPresent()) {

							Object newOne = answerLink.getValue();
							if (newOne != null) {
								if (old == null || old.hashCode() != newOne.hashCode()) {
									sendAttributeChangeEvent = true;
								}
							} else {
								if (old != null && newOne == null) {
									sendAttributeChangeEvent = true;
								}
							}
						}
						entityChanged |= sendAttributeChangeEvent;
						if (sendAttributeChangeEvent && answer.getChangeEvent()) {
							String oldValue = null;
							if (old != null) {
								if (answerLink.getValueMoney() != null) {
									oldValue = JsonUtils.toJson(optExisting.get().getValue());
								} else {
									oldValue = old.toString();
								}
							}
							if (answerLink == null) {
								log.debug("answerLink is Null");
							}
							if (getCurrentToken() == null) {
								log.debug("getCurrentToken is Null");
							}
							if (answerLink.getValue() == null) {
								log.debug("answerLink.getValue() is Null");
							}
							if (answerLink.getTargetCode() == null) {
								log.debug("answerLink.getTargetCode() is Null");
							}
							if (answerLink.getSourceCode() == null) {
								log.debug("answerLink.getSourceCode() is Null");
							}
							// Hack: avoid stack overflow
							Answer pojo = new Answer(answer.getSourceCode(), answer.getTargetCode(),
									answer.getAttributeCode(), answer.getValue());
							pojo.setWeight(answer.getWeight());
							pojo.setInferred(answer.getInferred());
							pojo.setExpired(answer.getExpired());
							pojo.setRefused(answer.getRefused());
							pojo.setAskId(answer.getAskId());
							pojo.setDataType(answer.getDataType());
							Optional<EntityAttribute> optNewEA = beTarget
									.findEntityAttribute(answer.getAttributeCode());

							EntityAttribute safeOne = new EntityAttribute();
							safeOne.setWeight(answer.getWeight());
							safeOne.setAttributeCode(attribute.getCode());
							safeOne.setAttributeName(attribute.getName());
							safeOne.setBaseEntityCode(beTarget.getCode());
							safeOne.setInferred(optNewEA.get().getInferred());
							safeOne.setPrivacyFlag(optNewEA.get().getPrivacyFlag());

							safeOne.setLoopValue(optNewEA.get().getLoopValue());
							safeOne.setAttribute(attribute);
							safeOne.setBaseEntity(beTarget);
							safeOne.setValue(optNewEA.get().getValue());
							safeSet.add(safeOne);

							if (optNewEA.isPresent()) {
								msg.setEa(safeOne);
								// msg.getBe().getAnswers().add(answerLink);
								msg.getBe().addAttribute(safeOne);
							}

						}

					} catch (final Exception e) {
						e.printStackTrace();
					}
				}

			} catch (final EntityExistsException e) {
				log.debug("Answer Insert EntityExistsException " + answer.getRealm() + ":" + answer.getSourceCode()
						+ ":" + answer.getTargetCode() + ":" + answer.getAttributeCode());

			} catch (Exception transactionException) {
				String answerStr = "";
				if (answer.getValue() != null) {
					answerStr = StringUtils.abbreviate(answer.getValue().toString(), 25);
				}
				log.error("Transaction Exception in saving Answer -> " + answer.getSourceCode() + ":"
						+ answer.getTargetCode() + ":" + answer.getAttributeCode() + " : " + (answerStr));
				transactionException.printStackTrace();
			}

//				log.info("Answer processing  = "+((System.nanoTime() - answerStartMs) / 1e6)+" ms "+answer.getRealm()+":"+ answer.getSourceCode()+":"+answer.getTargetCode()+":"+answer.getAttributeCode());

		}

//		double difference = (System.nanoTime() - insertStartMs) / 1e6; // get ms
//		if (answers.length > 1) {
//			log.info("Answer[]s finished processing = "+difference+" ms , now updating entity");
//		}
		if (entityChanged) {

			try {
				beTarget = getEntityManager().merge(beTarget); // if nothing changed then no need to merge beTarget
//				log.info("Merged " + beTarget);
			} catch (Exception e) {
				 e.printStackTrace();
				log.error(
						"Transaction is required to perform this operation (either use a transaction or extended persistence context) for "
								+ beTarget.getCode());
			}
			String json = JsonUtils.toJson(beTarget);
			writeToDDT(beTarget.getCode(), json); // Update the DDT
			if (!msg.getBe().getBaseEntityAttributes().isEmpty()) {
				// Don't send service events
				if (!msg.getBe().getCode().equals("PER_SERVICE")) {
					sendQEventAttributeValueChangeMessage(msg);
				}
			}
		}


		log.info("Answer final processing  = " + ((System.nanoTime() - insertStartMs) / 1e6)
				+ " ms - Finished updating " + beTarget.getCode());

		return 0L;
	}

	// @Transactional(dontRollbackOn = { Exception.class })
	public void insert(Answer answer) {

		log.info("insert(Answer):" + answer.getSourceCode() + ":" + answer.getTargetCode() + ":"
				+ answer.getAttributeCode() + ":" + StringUtils.abbreviateMiddle(answer.getValue(), "...", 30));
		Answer[] answers = new Answer[1];
		answers[0] = answer;
		insert(answers);

	}

	public Long insert(final Attribute attribute) {
		// always check if baseentity exists through check for unique code
		try {
			Attribute existing = findAttributeByCode(attribute.getCode());
			if (existing == null) {

				attribute.setRealm(getRealm());

				getEntityManager().persist(attribute);
			}

			this.pushAttributes();
		} catch (final ConstraintViolationException e) {
			Attribute existing = findAttributeByCode(attribute.getCode());

			existing.setRealm(getRealm());

			existing = getEntityManager().merge(existing);
			return existing.getId();
		} catch (final PersistenceException e) {
			Attribute existing = findAttributeByCode(attribute.getCode());

			existing.setRealm(getRealm());

			existing = getEntityManager().merge(existing);
			return existing.getId();
		} catch (final IllegalStateException e) {
			Attribute existing = findAttributeByCode(attribute.getCode());

			existing.setRealm(getRealm());

			existing = getEntityManager().merge(existing);
			return existing.getId();
		}
		return attribute.getId();
	}

	public static <E> E deepClone(E e) {
		ByteArrayOutputStream bo = new ByteArrayOutputStream();
		ObjectOutputStream oo;
		try {
			oo = new ObjectOutputStream(bo);
			oo.writeObject(e);
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		ByteArrayInputStream bi = new ByteArrayInputStream(bo.toByteArray());
		ObjectInputStream oi;
		try {
			oi = new ObjectInputStream(bi);
			return (E) oi.readObject();
		} catch (IOException ex) {
			ex.printStackTrace();
			return null;
		} catch (ClassNotFoundException ex) {
			ex.printStackTrace();
			return null;
		}
	}

	public EntityEntity insertEntityEntity(final EntityEntity ee) {

		try {
			getEntityManager().persist(ee);
			QEventLinkChangeMessage msg = new QEventLinkChangeMessage(ee.getLink(), null, getCurrentToken());

			sendQEventLinkChangeMessage(msg);
			log.debug("Sent Event Link Change Msg " + msg);

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
		Long result = 0L;

		try {
			result = (long) getEntityManager()

					.createQuery(
							"update BaseEntity be set be.name =:name where be.code=:sourceCode and be.realm=:realmStr")
					.setParameter("sourceCode", entity.getCode()).setParameter("name", entity.getName())
					.setParameter("realmStr", getRealm()).executeUpdate();

			BaseEntity updated = this.findBaseEntityByCode(entity.getCode());
			String json = JsonUtils.toJson(updated);
			writeToDDT(entity.getCode(), json);

		} catch (Exception e) {

		}

		return entity.getId();
	}

	public Long updateRealm(BaseEntity entity) {
		Long result = 0L;

		try {
			result = (long) getEntityManager()
					.createQuery("update BaseEntity be set be.realm =:realm where be.code=:sourceCode")
					.setParameter("sourceCode", entity.getCode()).setParameter("realm", entity.getRealm())
					.executeUpdate();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return result;
	}

	public Long updateRealm(Attribute attr) {
		Long result = 0L;

		try {
			result = (long) getEntityManager()
					.createQuery("update Attribute attr set attr.realm =:realm where attr.code=:code")
					.setParameter("code", attr.getCode()).setParameter("realm", attr.getRealm()).executeUpdate();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return result;
	}

	public Long updateRealm(Question que) {
		Long result = 0L;

		try {
			result = (long) getEntityManager()
					.createQuery("update Question que set que.realm =:realm where que.code=:code")
					.setParameter("code", que.getCode()).setParameter("realm", que.getRealm()).executeUpdate();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return result;
	}

	public Long updateRealm(QuestionQuestion qq) {
		Long result = 0L;

		try {
			result = (long) getEntityManager().createQuery(
					"delete QuestionQuestion qq where qq.pk.sourceCode=:sourceCode and qq.pk.targetCode=:targetCode")
					.setParameter("sourceCode", qq.getPk().getSource().getCode())
					.setParameter("targetCode", qq.getPk().getTargetCode()).executeUpdate();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public Long updateRealm(Validation val) {
		Long result = 0L;

		try {
			result = (long) getEntityManager()
					.createQuery("update Validation val set val.realm =:realm where val.code=:code")
					.setParameter("code", val.getCode()).setParameter("realm", val.getRealm()).executeUpdate();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return result;
	}

	public Long updateRealm(QBaseMSGMessageTemplate msg) {
		Long result = 0L;

		try {
			result = (long) getEntityManager()
					.createQuery("update QBaseMSGMessageTemplate msg set msg.realm =:realm where msg.code=:code")
					.setParameter("code", msg.getCode()).setParameter("realm", msg.getRealm()).executeUpdate();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return result;
	}

	public Long updateWithAttributes(BaseEntity entity) {
		entity.setRealm(getRealm());

		try {
			// merge in entityAttributes
			entity = getEntityManager().merge(entity);
		} catch (final Exception e) {
			// so persist otherwise
			if (entity.getName()== null) { entity.setName(entity.getCode());}
			getEntityManager().persist(entity);
		}
		String json = JsonUtils.toJson(entity);
		writeToDDT(entity.getCode(), json);
		return entity.getId();
	}

	public void bulkUpdateWithAttributes(List<BaseEntity> entities) {
		for (BaseEntity entity : entities) {
			updateWithAttributes(entity);
		}
	}

	public Long update(Attribute attribute) {
		// always check if attribute exists through check for unique code

		attribute.setRealm(getRealm());
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

		ask.setRealm(getRealm());

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

		val.setRealm(getRealm());

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

	public Ask upsert(Ask ask) {

		ask.setRealm(getRealm());

		try {
			getEntityManager().persist(ask);
			log.debug("UPSERTING:" + ask);
			return ask;
		} catch (Exception e) {
			ask = getEntityManager().merge(ask);
			return ask;
		}
	}

	public QuestionQuestion upsert(QuestionQuestion qq) {
		try {
			QuestionQuestion existing = findQuestionQuestionByCode(qq.getPk().getSource().getCode(),
					qq.getPk().getTargetCode());
			existing.setMandatory(qq.getMandatory());
			existing.setWeight(qq.getWeight());
			existing.setReadonly(qq.getReadonly());
			existing = getEntityManager().merge(existing);
			return existing;
		} catch (NoResultException e) {
			log.debug("------- QUESTION 00 ------------");
			getEntityManager().persist(qq);
			QuestionQuestion id = qq;
			return id;
		}
	}



	public Validation upsert(Validation validation) {
		try {
			String code = validation.getCode();
			Validation val = null;

			val = findValidationByCode(code);
			if (val != null) {
				BeanNotNullFields copyFields = new BeanNotNullFields();
				copyFields.copyProperties(val, validation);

				val.setRealm(getRealm());

				val = getEntityManager().merge(val);
			} else {
				throw new NoResultException();
			}
			return val;
		} catch (NoResultException | IllegalAccessException | InvocationTargetException e) {
			try {

				validation.setRealm(getRealm());
				if (BatchLoading.isSynchronise()) {
					Validation val = findValidationByCode(validation.getCode(), REALM_HIDDEN);
					if (val != null) {
						val.setRealm(getRealm());
						updateRealm(val);
						return val;
					}
				}

				getEntityManager().persist(validation);
			} catch (javax.validation.ConstraintViolationException ce) {
				log.error("Error in saving attribute due to constraint issue:" + validation + " :"
						+ ce.getLocalizedMessage());

				log.info("Trying to update realm from hidden to genny");
				validation.setRealm("genny");
				updateRealm(validation);

			} catch (javax.persistence.PersistenceException pe) {
				log.error("Error in saving validation :" + validation + " :" + pe.getLocalizedMessage());
			}

			Validation id = validation;
			return id;
		}
	}

	public Attribute upsert(Attribute attr) {
		try {
			String code = attr.getCode();
			Attribute val = findAttributeByCode(code);
			if (val == null) {
				log.error("Failed to find Attribute:" + code);
				throw new NoResultException();
			}
			log.info("UPSERT BEFORE COPY AttributeCode = " + val.getCode() + ", dataType = " + val.getDataType().getDttCode());
			BeanNotNullFields copyFields = new BeanNotNullFields();
			copyFields.copyProperties(val, attr);
			log.info("UPSERT AFTER COPY AttributeCode = " + val.getCode() + ", dataType = " + val.getDataType().getDttCode());

			val.setRealm(getRealm());

			val = getEntityManager().merge(val);
			return val;
		} catch (NoResultException | IllegalAccessException | InvocationTargetException e) {
			try {

				attr.setRealm(getRealm());
				if (BatchLoading.isSynchronise()) {
					Attribute val = findAttributeByCode(attr.getCode(), REALM_HIDDEN);
					if (val != null) {
						val.setRealm(getRealm());
						updateRealm(val);
						return val;
					}
				}

				getEntityManager().persist(attr);
			} catch (javax.validation.ConstraintViolationException ce) {
				log.error(
						"Error in saving attribute due to constraint issue:" + attr + " :" + ce.getLocalizedMessage());
			} catch (javax.persistence.PersistenceException pe) {
				log.error("Error in saving attribute :" + attr + " :" + pe.getLocalizedMessage());
			}
			Long id = attr.getId();
			return attr;
		}
	}

	public Question upsert(Question q, HashMap<String, Question> mapping) {
		try {
			String code = q.getCode();
			Question val = mapping.get(code);
			BeanNotNullFields copyFields = new BeanNotNullFields();
			if (val == null) {
				throw new NoResultException();
			}
			copyFields.copyProperties(val, q);

			val.setRealm(getRealm());
			Set<ConstraintViolation<Question>> constraints = validator.validate(val);
			for (ConstraintViolation<Question> constraint : constraints) {
				log.error(constraint.getPropertyPath() + " " + constraint.getMessage());
			}
			if (constraints.isEmpty()) {
				val = getEntityManager().merge(val);
				return val;
			} else {
				log.error("Error in Hibernate Validation for quesiton "+q.getCode()+" with attribute code :"+q.getAttributeCode());
			}
			return null; // TODO throw an error

		} catch (NoResultException | IllegalAccessException | InvocationTargetException e) {
			try {

				q.setRealm(getRealm());
				if (BatchLoading.isSynchronise()) {
					Question val = findQuestionByCode(q.getCode(), REALM_HIDDEN);
					if (val != null) {
						val.setRealm(getRealm());
						updateRealm(val);
						return val;
					}
				}

				getEntityManager().persist(q);
			} catch (javax.validation.ConstraintViolationException ce) {
				log.error("Error in saving question due to constraint issue:" + q + " :" + ce.getLocalizedMessage());
			} catch (javax.persistence.PersistenceException pe) {
				log.error("Error in saving question :" + q + " :" + pe.getLocalizedMessage());
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			Long id = q.getId();
			return q;
		}
	}

	public Question upsert(Question q) {
		try {
			String code = q.getCode();
			Question val = findQuestionByCode(code);
			BeanNotNullFields copyFields = new BeanNotNullFields();
			if (val == null) {
				throw new NoResultException();
			}
			copyFields.copyProperties(val, q);

			val.setRealm(getRealm());
			Set<ConstraintViolation<Question>> constraints = validator.validate(val);
			for (ConstraintViolation<Question> constraint : constraints) {
				log.error(constraint.getPropertyPath() + " " + constraint.getMessage());
			}
			if (constraints.isEmpty()) {
				val = getEntityManager().merge(val);
				return val;
			} else {
				log.error("Error in Hibernate Validation for quesiton "+q.getCode()+" with attribute code :"+q.getAttributeCode());
			}
			return null; // TODO throw an error
			
		} catch (NoResultException | IllegalAccessException | InvocationTargetException e) {
			try {

				q.setRealm(getRealm());
				if (BatchLoading.isSynchronise()) {
					Question val = findQuestionByCode(q.getCode(), REALM_HIDDEN);
					if (val != null) {
						val.setRealm(getRealm());
						updateRealm(val);
						return val;
					}
				}
				
				getEntityManager().persist(q);
			} catch (javax.validation.ConstraintViolationException ce) {
				log.error("Error in saving question due to constraint issue:" + q + " :" + ce.getLocalizedMessage());
			} catch (javax.persistence.PersistenceException pe) {
				log.error("Error in saving question :" + q + " :" + pe.getLocalizedMessage());
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			Long id = q.getId();
			return q;
		}
	}

	public BaseEntity upsert(BaseEntity be) {
		String realm = getRealm();
		try {
			String code = be.getCode();
			BaseEntity val = findBaseEntityByCode(code);

//			Session session = getEntityManager().unwrap(org.hibernate.Session.class);
//			Criteria criteria = session.createCriteria(BaseEntity.class);
//			BaseEntity val = (BaseEntity)criteria
//					.add(Restrictions.eq("code", code))
//					.add(Restrictions.eq("realm", realm))
//			                             .uniqueResult();

			if (val == null) {
				throw new NoResultException();
			}
			BeanNotNullFields copyFields = new BeanNotNullFields();
			copyFields.copyProperties(val, be);
			// val.setRealm(realm);
			// log.debug("***********" + val);
			val.merge(be);
			
			val = getEntityManager().merge(val);

			return val;
		} catch (NoResultException | NullPointerException | IllegalAccessException | InvocationTargetException e) {

			if (BatchLoading.isSynchronise()) {
				BaseEntity val = findBaseEntityByCode(be.getCode(), REALM_HIDDEN);
				if (val != null) {
					val.setRealm(getRealm());
					updateRealm(val);
					return val;
				}
			}
			Long id = insert(be);

			return be;
		}
	}

	public Long upsert(final BaseEntity be, Set<EntityAttribute> ba) {
		try {
			out.println("****3*****"
					+ be.getBaseEntityAttributes().stream().map(data -> data.pk).reduce((d1, d2) -> d1).get());
			String code = be.getCode();
			final BaseEntity val = findBaseEntityByCode(code);

			if (val == null) {
				throw new NoResultException();
			}

			BeanNotNullFields copyFields = new BeanNotNullFields();

			val.setRealm(getRealm());
			// copyFields.copyProperties(val, be);

			getEntityManager().merge(val);
			return val.getId();
		} catch (NoResultException e) {
			Long id = insert(be);
			return id;
		}

	}

	public Ask findAskById(final Long id) {
		Ask ret = null;

		try {
			ret = getEntityManager().find(Ask.class, id);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return ret;
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

	public BaseEntity findBaseEntityByCode(@NotNull final String baseEntityCode) throws NoResultException {

		return findBaseEntityByCode(baseEntityCode, true);

	}

	public BaseEntity findBaseEntityByCode(@NotNull final String baseEntityCode, boolean includeEntityAttributes)
			throws NoResultException {
		String realm = getRealm();
		BaseEntity result = null;
		String userRealmStr = getRealm();

		// log.info("FIND BASEENTITY BY CODE ["+baseEntityCode+"]in realm
		// "+userRealmStr);
//		if (includeEntityAttributes) {
		String privacySQL = "";

//			String sql = "SELECT be FROM BaseEntity be LEFT JOIN be.baseEntityAttributes ea where be.code=:baseEntityCode and be.realm in (\"genny\",\"" + userRealmStr + "\")  "
//					+ privacySQL;
//			String sql = "SELECT be FROM BaseEntity be LEFT JOIN be.baseEntityAttributes ea where be.code=:baseEntityCode and be.realm=:realmStr  "
//					+ privacySQL;
		// log.info("FIND BASEENTITY BY CODE :"+sql);
		try {

			CriteriaBuilder cb = getEntityManager().getCriteriaBuilder();
			CriteriaQuery<BaseEntity> query = cb.createQuery(BaseEntity.class);
			Root<BaseEntity> root = query.from(BaseEntity.class);

			query = query.select(root).where(cb.equal(root.get("code"), baseEntityCode.toUpperCase()),
					cb.equal(root.get("realm"), realm));
			result = getEntityManager().createQuery(query).getSingleResult();

//			
//			
//			Session session = getEntityManager().unwrap(org.hibernate.Session.class);
//			Criteria criteria = session.createCriteria(BaseEntity.class);
//			result = (BaseEntity) criteria.add(Restrictions.eq("code", baseEntityCode.toUpperCase()))
//					.add(Restrictions.eq("realm", realm)).uniqueResult();

		} catch (Exception e) {

			try {
				CriteriaBuilder cb = getEntityManager().getCriteriaBuilder();
				CriteriaQuery<BaseEntity> query = cb.createQuery(BaseEntity.class);
				Root<BaseEntity> root = query.from(BaseEntity.class);

				query = query.select(root).where(cb.equal(root.get("code"), baseEntityCode.toUpperCase()),
						cb.equal(root.get("realm"), realm));
				List<BaseEntity> results = getEntityManager().createQuery(query).getResultList();
				if (results.isEmpty()) {
					throw new NoResultException("Cannot find " + baseEntityCode + " in db! with realm " + realm);
				}
				result = results.get(0);
			} catch (NoResultException ee) {

				throw new NoResultException("Cannot find " + baseEntityCode + " in db! with realm " + realm);

			}
		}

		if (includeEntityAttributes) {
			CriteriaBuilder eaBuilder = getEntityManager().getCriteriaBuilder();
			CriteriaQuery<EntityAttribute> eaQuery = eaBuilder.createQuery(EntityAttribute.class);
			Root<EntityAttribute> eaRoot = eaQuery.from(EntityAttribute.class);

			eaQuery = eaQuery.select(eaRoot).where(eaBuilder.equal(eaRoot.get("baseEntityCode"), baseEntityCode.toUpperCase()),
					eaBuilder.equal(eaRoot.get("realm"), realm));
			List<EntityAttribute> attributeList = getEntityManager().createQuery(eaQuery).getResultList();
			Set<EntityAttribute> attributes = new HashSet<>(attributeList);
			result.setBaseEntityAttributes(attributes);
		}

		// else {
//			try {
//
//				result = (BaseEntity) getEntityManager()
//						.createQuery(
//								"SELECT be FROM BaseEntity be where be.code=:baseEntityCode  and  be.realm=:realmStr ")
//
//						.setParameter("baseEntityCode", baseEntityCode.toUpperCase()).setParameter("realmStr", realm)
//						.getSingleResult();
//
//			} catch (Exception e) {
////				if ("GRP_ALL_CONTACTS".equalsIgnoreCase(baseEntityCode)) {
////					log.info("GRP_ADMIN_JOBS");
////				}
//
//				throw new NoResultException("Cannot find " + baseEntityCode + " in db ");
//			}
//
//		}

		return result;

	}

	public BaseEntity findBaseEntityByCode(@NotNull final String baseEntityCode, @NotNull final String realm)
			throws NoResultException {

		BaseEntity result = null;

		try {

			result = (BaseEntity) getEntityManager()
					.createQuery("SELECT be FROM BaseEntity be where be.code=:baseEntityCode  and be.realm=:realmStr")
					.setParameter("baseEntityCode", baseEntityCode.toUpperCase()).setParameter("realmStr", realm)
					.getSingleResult();
		} catch (Exception e) {
			return null;
		}

		return result;

	}

	public Rule findRuleByCode(@NotNull final String ruleCode) throws NoResultException {

		final Rule result = (Rule) getEntityManager().createQuery("SELECT a FROM Rule a where a.code=:ruleCode")
				.setParameter("ruleCode", ruleCode.toUpperCase()).getSingleResult();

		return result;
	}

	public Question findQuestionByCode(@NotNull final String code) throws NoResultException {
		List<Question> result = null;
		final String userRealmStr = getRealm();
		try {

			result = getEntityManager().createQuery("SELECT a FROM Question a where a.code=:code and a.realm=:realmStr")

					.setParameter("realmStr", getRealm()).setParameter("code", code.toUpperCase()).getResultList();

		} catch (Exception e) {
			e.printStackTrace();
		}
		if (result == null || result.isEmpty()) {
			return null;
		}
		return result.get(0);
	}

	public Question findQuestionByCode(@NotNull final String code, @NotNull final String realm)
			throws NoResultException {
		List<Question> result = null;
		try {
			result = getEntityManager().createQuery("SELECT a FROM Question a where a.code=:code and a.realm=:realmStr")
					.setParameter("realmStr", realm).setParameter("code", code.toUpperCase()).getResultList();

		} catch (Exception e) {
			return null;
		}
		return result.get(0);
	}

	public DataType findDataTypeByCode(@NotNull final String code) throws NoResultException {

		final DataType result = (DataType) getEntityManager().createQuery("SELECT a FROM DataType a where a.code=:code")
				.setParameter("code", code.toUpperCase()).getSingleResult();

		return result;
	}

	public Validation findValidationByCode(@NotNull final String code) throws NoResultException {

		return findValidationByCode(code, getRealm());

	}

	public Validation findValidationByCode(@NotNull final String code, @NotNull final String realm)
			throws NoResultException {
		Validation result = null;
		try {
			result = (Validation) getEntityManager()
					.createQuery("SELECT a FROM Validation a where a.code=:code and a.realm=:realmStr")
					.setParameter("realmStr", realm).setParameter("code", code).getSingleResult();
		} catch (Exception e) {
			return null;
		}

		return result;
	}

	public AttributeLink findAttributeLinkByCode(@NotNull final String code) throws NoResultException {

		final AttributeLink result = (AttributeLink) getEntityManager()

				.createQuery("SELECT a FROM AttributeLink a where a.code=:code and a.realm=:realmStr")
				.setParameter("realmStr", getRealm()).setParameter("code", code.toUpperCase()).getSingleResult();

		return result;
	}

	public Attribute findAttributeByCode(@NotNull final String code) throws NoResultException {

		return findAttributeByCode(code, getRealm());
	}

	public Attribute findAttributeByCode(@NotNull final String code, @NotNull final String realm)
			throws NoResultException {
		Attribute result = null;
//		List<Attribute> results = null;
		String cleanCode = code.trim().toUpperCase();
		try {
			result = (Attribute) getEntityManager()
					.createQuery("SELECT a FROM Attribute a where a.code=:code and a.realm=:realmStr")
					.setParameter("realmStr", realm).setParameter("code", cleanCode).getSingleResult();

		} catch (Exception e) {
			// throw new NoResultException("Attribute Code :"+code+" not found in db");
		}
//		if (results == null || results.isEmpty()) {
//			return null;
//		} else {
//			return results.get(0); // return first one for now TODO
//		}
		return result;
	}

	public AnswerLink findAnswerLinkByCodes(@NotNull final String targetCode, @NotNull final String sourceCode,
			@NotNull final String attributeCode) {

		List<AnswerLink> results = null;

		try {
			results = getEntityManager().createQuery(
					"SELECT a FROM AnswerLink a where a.targetCode=:targetCode and a.sourceCode=:sourceCode and  attributeCode=:attributeCode")
					.setParameter("targetCode", targetCode).setParameter("sourceCode", sourceCode)
					.setParameter("attributeCode", attributeCode).getResultList();
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (results == null || results.isEmpty()) {
			return null;
		} else {
			return results.get(0); // return first one for now TODO
		}

	}

	public BaseEntity findUserByAttributeValue(@NotNull final String attributeCode, final Integer value) {
		final String userRealmStr = getRealm();

		final List<EntityAttribute> results = getEntityManager().createQuery(
				"SELECT ea FROM EntityAttribute ea where ea.pk.attribute.code=:attributeCode and ea.valueInteger=:valueInteger and ea.source.realm=:realmStr")
				.setParameter("attributeCode", attributeCode).setParameter("valueInteger", value)

				.setParameter("realmStr", getRealm()).setMaxResults(1).getResultList();

		if (results == null || results.size() == 0) {
			return null;
		}

		final BaseEntity ret = results.get(0).getPk().getBaseEntity();

		return ret;
	}

	public BaseEntity findUserByAttributeValue(@NotNull final String attributeCode, final String value) {
		final String userRealmStr = getRealm();

		final List<EntityAttribute> results = getEntityManager().createQuery(
				"SELECT ea FROM EntityAttribute ea where ea.pk.attribute.code=:attributeCode and ea.valueString=:value and ea.source.realm=:realmStr")
				.setParameter("attributeCode", attributeCode).setParameter("value", value).setMaxResults(1)

				.setParameter("realmStr", getRealm()).getResultList();

		if (results == null || results.size() == 0) {
			return null;
		}

		final BaseEntity ret = results.get(0).getPk().getBaseEntity();
		return ret;
	}

	public List<BaseEntity> findChildrenByAttributeLink(@NotNull final String sourceCode, final String linkCode,
			final boolean includeAttributes, final Integer pageStart, final Integer pageSize, final Integer level,
			final MultivaluedMap<String, String> params) {

		return findChildrenByAttributeLink(sourceCode, linkCode, includeAttributes, pageStart, pageSize, level, params,
				null);
	}

	public List<BaseEntity> findChildrenByAttributeLink(@NotNull final String sourceCode, final String linkCode,
			final boolean includeAttributes, final Integer pageStart, final Integer pageSize, final Integer level,
			final MultivaluedMap<String, String> params, final String stakeholderCode) {

		// Ignore this bit if stakeholder is null
		String stakeholderFilter1 = "";
		String stakeholderFilter2 = "";
		if (stakeholderCode != null) {
			stakeholderFilter1 = "EntityEntity ff JOIN be.baseEntityAttributes bff,";
			stakeholderFilter2 = " and ff.pk.targetCode=:stakeholderCode and ff.pk.source.code=be.code";

		}

		final List<BaseEntity> eeResults;
		new HashMap<String, BaseEntity>();
		final String userRealmStr = getRealm();

		log.debug("findChildrenByAttributeLink");
		if (includeAttributes) {
			log.debug("findChildrenByAttributeLink - includesAttributes");

			// ugly and insecure
			final Integer pairCount = params.size();
			if (pairCount.equals(0)) {
				log.debug("findChildrenByAttributeLink - PairCount==0");
				Query query = null;

				query = getEntityManager().createQuery("SELECT distinct be FROM BaseEntity be," + stakeholderFilter1
						+ "EntityEntity ee JOIN be.baseEntityAttributes bee where ee.link.targetCode=be.code and ee.link.attributeCode=:linkAttributeCode and ee.link.sourceCode=:sourceCode  and be.realm=:realmStr and ee.pk.source.realm=:realmStr"
						+ stakeholderFilter2).setParameter("sourceCode", sourceCode)

						.setParameter("linkAttributeCode", linkCode).setParameter("realmStr", getRealm());

				if (stakeholderCode != null) {
					query.setParameter("stakeholderCode", stakeholderCode);
				}
				eeResults = query.setFirstResult(pageStart).setMaxResults(pageSize).getResultList();

			} else {
				log.debug("findChildrenByAttributeLink - PAIR COUNT IS  " + pairCount);
				String eaStrings = "";
				String eaStringsQ = "";
				if (pairCount > 0) {
					eaStringsQ = "(";
					for (int i = 0; i < pairCount; i++) {
						eaStrings += ",EntityAttribute ea" + i;
						eaStringsQ += "ea" + i + ".baseEntityCode=be.code or ";
					}
					eaStringsQ = eaStringsQ.substring(0, eaStringsQ.length() - 4);
					eaStringsQ += ") and ";
				}

				String queryStr = "SELECT distinct be FROM BaseEntity be," + stakeholderFilter1 + "EntityEntity ee"
						+ eaStrings + "  JOIN be.baseEntityAttributes bee where " + eaStringsQ
						+ "  ee.link.targetCode=be.code" + stakeholderFilter2
						+ " and ee.link.attributeCode=:linkAttributeCode and  be.realm=:realmStr and ee.link.sourceCode=:sourceCode and ";
				int attributeCodeIndex = 0;
				int valueIndex = 0;
				final List<String> attributeCodeList = new ArrayList<>();
				final List<String> valueList = new ArrayList<>();

				for (final Map.Entry<String, List<String>> entry : params.entrySet()) {
					if (entry.getKey().equals("pageStart") || entry.getKey().equals("pageSize")) { // ugly
						continue;
					}
					final List<String> qvalueList = entry.getValue();
					if (!qvalueList.isEmpty()) {
						// create the value or
						String valueQuery = "(";
						for (final String value : qvalueList) {
							valueQuery += "ea" + attributeCodeIndex + ".valueString=:valueString" + valueIndex + " or ";
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
						queryStr += " ea" + attributeCodeIndex + ".attributeCode=:attributeCode" + attributeCodeIndex
								+ " and " + valueQuery;
						log.debug(
								"findChildrenByAttributeLink Key : " + entry.getKey() + " Value : " + entry.getValue());
					}
					attributeCodeIndex++;

				}
				log.debug("findChildrenByAttributeLink KIDS + ATTRIBUTE Query=" + queryStr);
				final Query query = getEntityManager().createQuery(queryStr);
				int index = 0;
				for (final String attributeParm : attributeCodeList) {
					query.setParameter("attributeCode" + index, attributeParm);
					log.debug("findChildrenByAttributeLink attributeCode" + index + "=:" + attributeParm);
					index++;
				}
				index = 0;
				for (final String valueParm : valueList) {
					query.setParameter("valueString" + index, valueParm);
					log.debug("valueString" + index + "=:" + valueParm);
					index++;
				}
				query.setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode);

				query.setParameter("realmStr", getRealm());

				if (stakeholderCode != null) {
					query.setParameter("stakeholderCode", stakeholderCode);
				}

				query.setFirstResult(pageStart).setMaxResults(pageSize);
				eeResults = query.getResultList();

			}
		} else {
			log.debug("**************** ENTITY ENTITY WITH NO ATTRIBUTES ****************");

			// ugly and insecure
			final Integer pairCount = params.size();
			if (pairCount.equals(0)) {

				Query query = getEntityManager().createQuery("SELECT distinct be FROM BaseEntity be,"
						+ stakeholderFilter1 + "EntityEntity ee  where ee.link.targetCode=be.code " + stakeholderFilter2
						+ " and ee.link.attributeCode=:linkAttributeCode and ee.link.sourceCode=:sourceCode   and be.realm=:realmStr")
						.setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode)

						.setParameter("realmStr", getRealm()).setFirstResult(pageStart);

				if (stakeholderCode != null) {
					query.setParameter("stakeholderCode", stakeholderCode);
				}

				eeResults = query.setMaxResults(pageSize).getResultList();

			} else {
				log.debug("findChildrenByAttributeLink PAIR COUNT  " + pairCount);
				String eaStrings = "";
				String eaStringsQ = "";
				if (pairCount > 0) {
					eaStringsQ = "(";
					for (int i = 0; i < pairCount; i++) {
						eaStrings += ",EntityAttribute ea" + i;
						eaStringsQ += "ea" + i + ".baseEntityCode=be.code or ";
					}
					eaStringsQ = eaStringsQ.substring(0, eaStringsQ.length() - 4);
					eaStringsQ += ") and ";
				}

				String queryStr = "SELECT distinct be FROM BaseEntity be," + stakeholderFilter1 + " EntityEntity ee"
						+ eaStrings + "  where " + eaStringsQ + " ee.link.targetCode=be.code " + stakeholderFilter2
						+ " and ee.link.attributeCode=:linkAttributeCode and be.realm=:realmStr and ee.link.sourceCode=:sourceCode and ";
				int attributeCodeIndex = 0;
				int valueIndex = 0;
				final List<String> attributeCodeList = new ArrayList<>();
				final List<String> valueList = new ArrayList<>();

				for (final Map.Entry<String, List<String>> entry : params.entrySet()) {
					if (entry.getKey().equals("pageStart") || entry.getKey().equals("pageSize")) { // ugly
						continue;
					}
					final List<String> qvalueList = entry.getValue();
					if (!qvalueList.isEmpty()) {
						// create the value or
						String valueQuery = "(";
						for (final String value : qvalueList) {
							valueQuery += "ea" + attributeCodeIndex + ".valueString=:valueString" + valueIndex + " or ";
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
						queryStr += " ea" + attributeCodeIndex + ".attributeCode=:attributeCode" + attributeCodeIndex
								+ " and " + valueQuery;
						log.debug(
								"findChildrenByAttributeLink Key : " + entry.getKey() + " Value : " + entry.getValue());
					}
					attributeCodeIndex++;

				}
				log.debug("findChildrenByAttributeLink KIDS + ATTRIBUTE Query=" + queryStr);
				final Query query = getEntityManager().createQuery(queryStr);
				int index = 0;
				for (final String attributeParm : attributeCodeList) {
					query.setParameter("attributeCode" + index, attributeParm);
					log.debug("attributeCode" + index + "=:" + attributeParm);
					index++;
				}
				index = 0;
				for (final String valueParm : valueList) {
					query.setParameter("valueString" + index, valueParm);
					log.debug("valueString" + index + "=:" + valueParm);
					index++;
				}
				query.setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode);

				query.setParameter("realmStr", getRealm());

				query.setFirstResult(pageStart).setMaxResults(pageSize);
				if (stakeholderCode != null) {
					query.setParameter("stakeholderCode", stakeholderCode);
				}
				eeResults = query.getResultList();
				log.debug("findChildrenByAttributeLink NULL THE ATTRIBUTES");
			}

		}
		Integer index = 0;
		for (BaseEntity be : eeResults) {
			be.setIndex(index++);
		}

		return eeResults;
	}

	public List<BaseEntity> findChildrenByLinkValue(@NotNull final String sourceCode, final String linkCode,
			final String linkValue, final boolean includeAttributes, final Integer pageStart, final Integer pageSize,
			final Integer level, final MultivaluedMap<String, String> params, final String stakeholderCode) {

		// Ignore this bit if stakeholder is null
		String stakeholderFilter1 = "";
		String stakeholderFilter2 = "";
		if (stakeholderCode != null) {
			stakeholderFilter1 = "EntityEntity ff JOIN be.baseEntityAttributes bff,";
			stakeholderFilter2 = " and ff.pk.targetCode=:stakeholderCode and ff.link.sourceCode=be.code ";
		}

		final List<BaseEntity> eeResults;
		new HashMap<String, BaseEntity>();
		final String userRealmStr = getRealm();

		log.debug("findChildrenByAttributeLink");
		if (includeAttributes) {
			log.debug("findChildrenByAttributeLink - includesAttributes");

			// ugly and insecure
			final Integer pairCount = params.size();
			if (pairCount.equals(0)) {
				log.debug("findChildrenByAttributeLink - PairCount==0");
				Query query = null;

				query = getEntityManager().createQuery("SELECT distinct be FROM BaseEntity be," + stakeholderFilter1
						+ "EntityEntity ee JOIN be.baseEntityAttributes bee where ee.link.targetCode=be.code and ee.link.attributeCode=:linkAttributeCode and ee.link.sourceCode=:sourceCode  and ee.pk.source.realm=:realmStr and ee.link.linkValue=:linkValue"
						+ stakeholderFilter2).setParameter("sourceCode", sourceCode)

						.setParameter("linkAttributeCode", linkCode).setParameter("realmStr", getRealm())

						.setParameter("linkValue", linkValue);

				if (stakeholderCode != null) {
					query.setParameter("stakeholderCode", stakeholderCode);
				}
				eeResults = query.setFirstResult(pageStart).setMaxResults(pageSize).getResultList();

			} else {
				log.debug("findChildrenByAttributeLink - PAIR COUNT IS  " + pairCount);
				String eaStrings = "";
				String eaStringsQ = "";
				if (pairCount > 0) {
					eaStringsQ = "(";
					for (int i = 0; i < pairCount; i++) {
						eaStrings += ",EntityAttribute ea" + i;
						eaStringsQ += "ea" + i + ".baseEntityCode=be.code or ";
					}
					eaStringsQ = eaStringsQ.substring(0, eaStringsQ.length() - 4);
					eaStringsQ += ") and ";
				}

				String queryStr = "SELECT distinct be FROM BaseEntity be," + stakeholderFilter1 + "EntityEntity ee"
						+ eaStrings + "  JOIN be.baseEntityAttributes bee where " + eaStringsQ
						+ "  ee.link.targetCode=be.code" + stakeholderFilter2
						+ " and ee.link.attributeCode=:linkAttributeCode and  be.realm=:realmStr and ee.link.linkValue=:linkValue and ee.link.sourceCode=:sourceCode and ";
				int attributeCodeIndex = 0;
				int valueIndex = 0;
				final List<String> attributeCodeList = new ArrayList<>();
				final List<String> valueList = new ArrayList<>();

				for (final Map.Entry<String, List<String>> entry : params.entrySet()) {
					if (entry.getKey().equals("pageStart") || entry.getKey().equals("pageSize")) { // ugly
						continue;
					}
					final List<String> qvalueList = entry.getValue();
					if (!qvalueList.isEmpty()) {
						// create the value or
						String valueQuery = "(";
						for (final String value : qvalueList) {
							valueQuery += "ea" + attributeCodeIndex + ".valueString=:valueString" + valueIndex + " or ";
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
						queryStr += " ea" + attributeCodeIndex + ".attributeCode=:attributeCode" + attributeCodeIndex
								+ " and " + valueQuery;
						log.debug(
								"findChildrenByAttributeLink Key : " + entry.getKey() + " Value : " + entry.getValue());
					}
					attributeCodeIndex++;

				}
				log.debug("findChildrenByAttributeLink KIDS + ATTRIBUTE Query=" + queryStr);
				final Query query = getEntityManager().createQuery(queryStr);
				int index = 0;
				for (final String attributeParm : attributeCodeList) {
					query.setParameter("attributeCode" + index, attributeParm);
					log.debug("findChildrenByAttributeLink attributeCode" + index + "=:" + attributeParm);
					index++;
				}
				index = 0;
				for (final String valueParm : valueList) {
					query.setParameter("valueString" + index, valueParm);
					log.debug("valueString" + index + "=:" + valueParm);
					index++;
				}
				query.setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode);

				query.setParameter("realmStr", getRealm());

				if (stakeholderCode != null) {
					query.setParameter("stakeholderCode", stakeholderCode);
				}
				query.setParameter("linkValue", linkValue);
				query.setFirstResult(pageStart).setMaxResults(pageSize);
				eeResults = query.getResultList();

			}
		} else {
			log.debug("**************** ENTITY ENTITY WITH NO ATTRIBUTES ****************");

			// ugly and insecure
			final Integer pairCount = params.size();
			if (pairCount.equals(0)) {

				Query query = getEntityManager().createQuery("SELECT distinct be FROM BaseEntity be,"
						+ stakeholderFilter1 + "EntityEntity ee  where ee.link.targetCode=be.code " + stakeholderFilter2
						+ " and ee.link.attributeCode=:linkAttributeCode and ee.link.sourceCode=:sourceCode   and be.realm=:realmStr  and ee.link.linkValue=:linkValue ")
						.setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode)

						.setParameter("linkValue", linkValue).setParameter("realmStr", getRealm())

						.setFirstResult(pageStart);
				if (stakeholderCode != null) {
					query.setParameter("stakeholderCode", stakeholderCode);
				}

				eeResults = query.setMaxResults(pageSize).getResultList();

			} else {
				log.debug("findChildrenByAttributeLink PAIR COUNT  " + pairCount);
				String eaStrings = "";
				String eaStringsQ = "";
				if (pairCount > 0) {
					eaStringsQ = "(";
					for (int i = 0; i < pairCount; i++) {
						eaStrings += ",EntityAttribute ea" + i;
						eaStringsQ += "ea" + i + ".baseEntityCode=be.code or ";
					}
					eaStringsQ = eaStringsQ.substring(0, eaStringsQ.length() - 4);
					eaStringsQ += ") and ";
				}

				String queryStr = "SELECT distinct be FROM BaseEntity be," + stakeholderFilter1 + " EntityEntity ee"
						+ eaStrings + "  where " + eaStringsQ + " ee.link.targetCode=be.code " + stakeholderFilter2
						+ " and ee.link.attributeCode=:linkAttributeCode and be.realm=:realmStr  and ee.link.linkValue=:linkValue and ee.link.sourceCode=:sourceCode and ";
				int attributeCodeIndex = 0;
				int valueIndex = 0;
				final List<String> attributeCodeList = new ArrayList<>();
				final List<String> valueList = new ArrayList<>();

				for (final Map.Entry<String, List<String>> entry : params.entrySet()) {
					if (entry.getKey().equals("pageStart") || entry.getKey().equals("pageSize")) { // ugly
						continue;
					}
					final List<String> qvalueList = entry.getValue();
					if (!qvalueList.isEmpty()) {
						// create the value or
						String valueQuery = "(";
						for (final String value : qvalueList) {
							valueQuery += "ea" + attributeCodeIndex + ".valueString=:valueString" + valueIndex + " or ";
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
						queryStr += " ea" + attributeCodeIndex + ".attributeCode=:attributeCode" + attributeCodeIndex
								+ " and " + valueQuery;
						log.debug(
								"findChildrenByAttributeLink Key : " + entry.getKey() + " Value : " + entry.getValue());
					}
					attributeCodeIndex++;

				}
				log.debug("findChildrenByAttributeLink KIDS + ATTRIBUTE Query=" + queryStr);
				final Query query = getEntityManager().createQuery(queryStr);
				int index = 0;
				for (final String attributeParm : attributeCodeList) {
					query.setParameter("attributeCode" + index, attributeParm);
					log.debug("attributeCode" + index + "=:" + attributeParm);
					index++;
				}
				index = 0;
				for (final String valueParm : valueList) {
					query.setParameter("valueString" + index, valueParm);
					log.debug("valueString" + index + "=:" + valueParm);
					index++;
				}
				query.setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode);

				query.setParameter("realmStr", getRealm());

				query.setFirstResult(pageStart).setMaxResults(pageSize);
				if (stakeholderCode != null) {
					query.setParameter("stakeholderCode", stakeholderCode);
				}
				query.setParameter("linkValue", linkValue);
				eeResults = query.getResultList();
				log.debug("findChildrenByAttributeLink NULL THE ATTRIBUTES");
			}

		}
		Integer index = 0;
		for (BaseEntity be : eeResults) {
			be.setIndex(index++);
		}

		return eeResults;
	}

	public Long findChildrenByAttributeLinkCount(@NotNull final String sourceCode, final String linkCode,
			final MultivaluedMap<String, String> params, final String stakeholderCode) {
		boolean includeAttributes = false;

		// Ignore this bit if stakeholder is null
		String stakeholderFilter1 = "";
		String stakeholderFilter2 = "";
		if (stakeholderCode != null) {
			stakeholderFilter1 = "EntityEntity ff JOIN be.baseEntityAttributes bff,";
			stakeholderFilter2 = " and ff.link.targetCode=:stakeholderCode and ff.link.sourceCode=be.code ";
		}

		new HashMap<String, BaseEntity>();
		final String userRealmStr = getRealm();

		Query query = null;
		// ugly and insecure
		final Integer pairCount = params.size();
		if (pairCount.equals(0)) {

			query = getEntityManager().createQuery("SELECT count(distinct be) FROM BaseEntity be," + stakeholderFilter1
					+ "EntityEntity ee  where ee.link.targetCode=be.code " + stakeholderFilter2
					+ " and ee.link.attributeCode=:linkAttributeCode and ee.link.sourceCode=:sourceCode   and be.realm=:realmStr")
					.setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode)

					.setParameter("realmStr", getRealm());

			if (stakeholderCode != null) {
				query.setParameter("stakeholderCode", stakeholderCode);
			}

		} else {
			log.debug("findChildrenByAttributeLink PAIR COUNT  " + pairCount);
			String eaStrings = ",";
			String eaStringsQ = "";
			if (pairCount > 0) {
				eaStringsQ = "(";
				for (int i = 0; i < pairCount; i++) {
					eaStrings += ",EntityAttribute ea" + i;
					eaStringsQ += "ea" + i + ".baseEntityCode=be.code or ";
				}
				eaStringsQ = eaStringsQ.substring(0, eaStringsQ.length() - 4);
				eaStringsQ += ") and ";
			}
			if (eaStrings.equals(",")) {
				eaStrings = "";
			}

			String queryStr = "SELECT count(distinct be) FROM BaseEntity be," + stakeholderFilter1 + " EntityEntity ee"
					+ eaStrings + "  where " + eaStringsQ + " ee.link.targetCode=be.code " + stakeholderFilter2
					+ " and ee.link.attributeCode=:linkAttributeCode and be.realm=:realmStr and ee.link.sourceCode=:sourceCode and ";
			int attributeCodeIndex = 0;
			int valueIndex = 0;
			final List<String> attributeCodeList = new ArrayList<>();
			final List<String> valueList = new ArrayList<>();

			for (final Map.Entry<String, List<String>> entry : params.entrySet()) {
				if (entry.getKey().equals("pageStart") || entry.getKey().equals("pageSize")) { // ugly
					continue;
				}
				final List<String> qvalueList = entry.getValue();
				if (!qvalueList.isEmpty()) {
					// create the value or
					String valueQuery = "(";
					for (final String value : qvalueList) {
						valueQuery += "ea" + attributeCodeIndex + ".valueString=:valueString" + valueIndex + " or ";
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
					queryStr += " ea" + attributeCodeIndex + ".attributeCode=:attributeCode" + attributeCodeIndex
							+ " and " + valueQuery;
					log.debug("findChildrenByAttributeLink Key : " + entry.getKey() + " Value : " + entry.getValue());
				}
				attributeCodeIndex++;

			}
			query = getEntityManager().createQuery(queryStr);
			int index = 0;
			for (final String attributeParm : attributeCodeList) {
				query.setParameter("attributeCode" + index, attributeParm);
				log.debug("attributeCode" + index + "=:" + attributeParm);
				index++;
			}
			index = 0;
			for (final String valueParm : valueList) {
				query.setParameter("valueString" + index, valueParm);
				log.debug("valueString" + index + "=:" + valueParm);
				index++;
			}
			query.setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode);

			query.setParameter("realmStr", getRealm());

			if (stakeholderCode != null) {
				query.setParameter("stakeholderCode", stakeholderCode);
			}
		}
		Long total = 0L;
		try {
			total = (Long) query.getSingleResult();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return total;
	}

	public Long findChildrenByLinkValueCount(@NotNull final String sourceCode, final String linkCode,
			final String linkValue, final MultivaluedMap<String, String> params) {
		final String userRealmStr = getRealm();

		Long total = 0L;
		final Integer pairCount = params.size();
		log.debug("findChildrenByAttributeLinkCount PAIR COUNT IS " + pairCount);
		String eaStrings = "";
		String eaStringsQ = "";
		if (pairCount > 0) {
			eaStringsQ = "(";
			for (int i = 0; i < pairCount; i++) {
				eaStrings += ",EntityAttribute ea" + i;
				eaStringsQ += "ea" + i + ".baseEntityCode=be.code or ";
			}
			eaStringsQ = eaStringsQ.substring(0, eaStringsQ.length() - 4);
			eaStringsQ += ") and ";
		}

		if (eaStrings.equals(",")) {
			eaStrings = "";
		}

		String queryStr = "SELECT count(distinct be) FROM BaseEntity be,EntityEntity ee" + eaStrings + "  where "
				+ eaStringsQ
				+ "  ee.link.targetCode=be.code and ee.link.attributeCode=:linkAttributeCode  and be.realm=:realmStr and ee.link.sourceCode=:sourceCode  and ee.link.linkValue=:linkValue ";
		int attributeCodeIndex = 0;
		int valueIndex = 0;
		final List<String> attributeCodeList = new ArrayList<>();
		final List<String> valueList = new ArrayList<>();

		for (final Map.Entry<String, List<String>> entry : params.entrySet()) {
			final List<String> qvalueList = entry.getValue();
			if (!qvalueList.isEmpty()) {
				// create the value or
				String valueQuery = "(";
				for (final String value : qvalueList) {
					valueQuery += "ea" + attributeCodeIndex + ".valueString=:valueString" + valueIndex + " or ";
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
				queryStr += " and  ea" + attributeCodeIndex + ".attributeCode=:attributeCode" + attributeCodeIndex
						+ " and " + valueQuery;
				log.debug("Key : " + entry.getKey() + " Value : " + entry.getValue());
			}
			attributeCodeIndex++;

		}
		log.debug("findChildrenByAttributeLinkCount KIDS + ATTRIBUTE Query=" + queryStr);
		final Query query = getEntityManager().createQuery(queryStr);
		int index = 0;
		for (final String attributeParm : attributeCodeList) {
			query.setParameter("attributeCode" + index, attributeParm);
			log.debug("attributeCode" + index + "=:" + attributeParm);
			index++;
		}
		index = 0;
		for (final String valueParm : valueList) {
			query.setParameter("valueString" + index, valueParm);
			log.debug("valueString" + index + "=:" + valueParm);
			index++;
		}
		query.setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode);

		query.setParameter("realmStr", getRealm());

		query.setParameter("linkValue", linkValue);
		try {
			total = (Long) query.getSingleResult();
		} catch (Exception e) {
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
		final List<Ask> results = getEntityManager()
				.createQuery("SELECT ea FROM Ask ea where ea.source.id=:baseEntityId").setParameter("baseEntityId", id)
				.getResultList();

		return results;

	}

	public List<Ask> findAsksBySourceBaseEntityCode(final String code) {
		final List<Ask> results = getEntityManager()

				.createQuery("SELECT ea FROM Ask ea where ea.source.code=:baseEntityCode and ea.realm=:realmStr")
				.setParameter("realmStr", getRealm()).setParameter("baseEntityCode", code).getResultList();

		return results;
	}

	public List<Ask> findAsksByAttribute(final Attribute attribute, final BaseEntity source, final BaseEntity target) {
		return findAsksByAttributeCode(attribute.getCode(), source.getCode(), target.getCode());
	}

	public List<Ask> findAsksByCode(final String attributeCode, String sourceCode, final String targetCode) {
		List<Ask> results = null;
		try {
			results = getEntityManager().createQuery(
					"SELECT ask FROM Ask ask where ask.attributeCode=:attributeCode and ask.sourceCode=:sourceCode and ask.targetCode=:targetCode")
					.setParameter("attributeCode", attributeCode).setParameter("sourceCode", sourceCode)

					.setParameter("realmStr", getRealm()).setParameter("targetCode", targetCode).getResultList();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return results;
	}

	public List<Ask> findAsksByQuestion(final Question question, final BaseEntity source, final BaseEntity target) {
		return findAsksByQuestionCode(question.getCode(), source.getCode(), target.getCode());
	}

	public List<Ask> findAsksByAttributeCode(final String attributeCode, String sourceCode, final String targetCode) {
		List<Ask> results = null;
		try {
			results = getEntityManager().createQuery(
					"SELECT ask FROM Ask ask where ask.attributeCode=:attributeCode and ask.sourceCode=:sourceCode and ask.targetCode=:targetCode")
					.setParameter("attributeCode", attributeCode).setParameter("sourceCode", sourceCode)

					.setParameter("realmStr", getRealm()).setParameter("targetCode", targetCode).getResultList();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return results;
	}

	public QuestionQuestion findQuestionQuestionByCode(final String sourceCode, final String targetCode)
			throws NoResultException {
		QuestionQuestion result = null;
		try {
			result = (QuestionQuestion) getEntityManager().createQuery(

					"SELECT qq FROM QuestionQuestion qq where qq.pk.sourceCode=:sourceCode and qq.pk.targetCode=:targetCode and qq.pk.source.realm=:realmStr")

					.setParameter("realmStr", getRealm()).setParameter("sourceCode", sourceCode)
					.setParameter("targetCode", targetCode).getSingleResult();

		} catch (Exception e) {
			throw new NoResultException("Cannot find QQ " + sourceCode + ":" + targetCode);
		}
		return result;
	}

	public List<Ask> findAsksByQuestionCode(final String questionCode, String sourceCode, final String targetCode) {
		List<Ask> results = null;
		final String userRealmStr = getRealm();

		try {
			results = getEntityManager().createQuery(
					"SELECT ask FROM Ask ask where ask.questionCode=:questionCode and ask.sourceCode=:sourceCode and ask.targetCode=:targetCode and ask.realm=:realmStr")
					.setParameter("questionCode", questionCode).setParameter("sourceCode", sourceCode)

					.setParameter("realmStr", getRealm()).setParameter("targetCode", targetCode).getResultList();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return results;
	}

	public List<Ask> findAsks(final Question rootQuestion, final BaseEntity source, final BaseEntity target) {
		return findAsks(rootQuestion, source, target, false, false, false, false);
	}

	/*
	 * Added boolean readonly value to the method which will support to add readonly
	 * parameter to the QuestionQuestions/Asks
	 */
	public List<Ask> findAsks(final Question rootQuestion, final BaseEntity source, final BaseEntity target,
			Boolean childQuestionIsMandatory, Boolean childQuestionIsReadOnly, Boolean childQuestionIsFormTrigger,
			Boolean childQuestionIsCreateOnTrigger) {
		List<Ask> asks = new ArrayList<>();

		if (rootQuestion.getAttributeCode().equals(Question.QUESTION_GROUP_ATTRIBUTE_CODE)) {
			// Recurse!
			List<QuestionQuestion> qqList = new ArrayList<>(rootQuestion.getChildQuestions());
			Collections.sort(qqList); // sort by priority
			for (QuestionQuestion qq : qqList) {
				String qCode = qq.getPk().getTargetCode();
				Question childQuestion = findQuestionByCode(qCode);
				asks.addAll(findAsks(childQuestion, source, target, qq.getMandatory(), qq.getReadonly(),
						qq.getFormTrigger(), qq.getCreateOnTrigger()));
			}
		} else {
			// This is an actual leaf question, so we can create an ask ...
			Ask ask = null;
			// check if this already exists?
			List<Ask> myAsks = findAsksByQuestion(rootQuestion, source, target);
			if (!(myAsks == null || myAsks.isEmpty())) {
				ask = myAsks.get(0);
				ask.setMandatory(rootQuestion.getMandatory() || childQuestionIsMandatory);
				ask.setReadonly(rootQuestion.getReadonly() || childQuestionIsReadOnly); // setting readonly to asks
				ask.setFormTrigger(childQuestionIsFormTrigger);
				ask.setCreateOnTrigger(childQuestionIsCreateOnTrigger);
			} else {
				// create one
				Boolean mandatory = rootQuestion.getMandatory() || childQuestionIsMandatory;
				Boolean readonly = rootQuestion.getReadonly() || childQuestionIsReadOnly;

				ask = new Ask(rootQuestion, source.getCode(), target.getCode(), mandatory, 0.0, false, false, readonly);
				ask.setCreateOnTrigger(childQuestionIsMandatory);
				ask.setFormTrigger(childQuestionIsFormTrigger);
				ask.setRealm(getRealm());

				ask = upsert(ask); // save
			}
			asks.add(ask);
		}

		return asks;
	}

	public List<Ask> findAsks2(final Question rootQuestion, final BaseEntity source, final BaseEntity target) {
		return findAsks2(rootQuestion, source, target, false, false, false, false);
	}

	public List<Ask> findAsks2(final Question rootQuestion, final BaseEntity source, final BaseEntity target,
			Boolean childQuestionIsMandatory, Boolean childQuestionIsReadonly, Boolean childQuestionIsFormTrigger,
			Boolean childQuestionIsCreateOnTrigger) {
		if (rootQuestion == null) {
			log.error(
					"rootQuestion for findAsks2 is null - source=" + source.getCode() + ": target " + target.getCode());
			return new ArrayList<Ask>();
		}
		List<Ask> asks = new ArrayList<>();
		Boolean mandatory = rootQuestion.getMandatory() || childQuestionIsMandatory;
		Boolean readonly = rootQuestion.getReadonly() || childQuestionIsReadonly;
		Ask ask = null;
		// check if this already exists?
		List<Ask> myAsks = findAsksByQuestion(rootQuestion, source, target);
		if (!(myAsks == null || myAsks.isEmpty())) {
			ask = myAsks.get(0);
			ask.setMandatory(mandatory);
			ask.setReadonly(readonly);
			ask.setFormTrigger(childQuestionIsFormTrigger);
			ask.setCreateOnTrigger(childQuestionIsCreateOnTrigger);

		} else {
			ask = new Ask(rootQuestion, source.getCode(), target.getCode(), mandatory, 1.0, false, false, readonly);
			ask.setCreateOnTrigger(childQuestionIsMandatory);
			ask.setFormTrigger(childQuestionIsFormTrigger);
			ask.setRealm(getRealm());

			// Now merge ask name if required
			ask = performMerge(ask);

			ask = upsert(ask);
		}
		// create one
		if (rootQuestion.getAttributeCode().startsWith(Question.QUESTION_GROUP_ATTRIBUTE_CODE)) {
			// Recurse!
			List<QuestionQuestion> qqList = new ArrayList<>(rootQuestion.getChildQuestions());
			Collections.sort(qqList); // sort by priority
			List<Ask> childAsks = new ArrayList<>();
			for (QuestionQuestion qq : qqList) {
				String qCode = qq.getPk().getTargetCode();
				log.info(qq.getPk().getSourceCode() + " -> Child Question -> " + qCode);
				Question childQuestion = findQuestionByCode(qCode);
				// Grab whatever icon the QuestionQuestion has set
				childQuestion.setIcon(qq.getIcon());
				if (childQuestion != null) {
					List<Ask> askChildren = null;
					try {
						askChildren = findAsks2(childQuestion, source, target, qq.getMandatory(), qq.getReadonly(),
								qq.getFormTrigger(), qq.getCreateOnTrigger());
						for (Ask child : askChildren) {
							child.setQuestion(childQuestion);
							child.setHidden(qq.getHidden());
							child.setDisabled(qq.getDisabled());
							child.setReadonly(qq.getReadonly());
						}
					} catch (Exception e) {
						log.error("Error with QuestionQuestion: " + rootQuestion.getCode());
						log.error("Problem Question: " + childQuestion.getCode());
						e.printStackTrace();
					}
					childAsks.addAll(askChildren);
				}
			}
			Ask[] asksArray = childAsks.toArray(new Ask[0]);
			ask.setChildAsks(asksArray);

			ask.setRealm(getRealm());
			// ask.setChildAsks(childAsks);

			ask = upsert(ask); // save
		}

		asks.add(ask);
		return asks;
	}

	private Ask performMerge(Ask ask) {
		if (ask.getName().contains("{{")) {
			// now merge in data
			String name = ask.getName();

			Map<String, Object> templateEntityMap = new HashMap<>();
			ContextList contexts = ask.getContextList();
			for (Context context : contexts.getContexts()) {
				BaseEntity be = context.getEntity();
				templateEntityMap.put(context.getName(), be);
			}
			String mergedName = MergeUtil.merge(name, templateEntityMap);
			ask.setName(mergedName);
		}
		return ask;

	}

	QuestionSourceTarget findQST(final String questionCode, final QuestionSourceTarget[] qstArray,
			final QuestionSourceTarget defaultQST) {
		for (QuestionSourceTarget qst : qstArray) {
			if (qst.getQuestionCode().equalsIgnoreCase(questionCode)) {
				return qst;
			}
		}
		return defaultQST;
	}

	public List<Ask> findAsksUsingQuestionSourceTarget(Question rootQuestion, QuestionSourceTarget[] qstArray,
			QuestionSourceTarget defaultQST) {
		// find the root QST from the qstArray
		QuestionSourceTarget qst = findQST(rootQuestion.getCode(), qstArray, defaultQST);

		return findAsksUsingQuestionSourceTarget(rootQuestion, qst, qstArray, false);
	}

	public List<Ask> findAsksUsingQuestionSourceTarget(final Question rootQuestion,
			final QuestionSourceTarget defaultQST, QuestionSourceTarget[] qstArray, Boolean childQuestionIsMandatory) {
		List<Ask> asks = new ArrayList<>();
		Boolean mandatory = rootQuestion.getMandatory() || childQuestionIsMandatory;
		Boolean readonly = rootQuestion.getReadonly();

		Ask ask = null;
		// check if this already exists?
		List<Ask> myAsks = findAsksByQuestion(rootQuestion, defaultQST.getSource(), defaultQST.getTarget());
		if (!(myAsks == null || myAsks.isEmpty())) {
			ask = myAsks.get(0);
			ask.setMandatory(mandatory);
			ask.setReadonly(readonly);
		} else {
			ask = new Ask(rootQuestion, defaultQST.getSourceCode(), defaultQST.getTargetCode(), mandatory);
			ask.setReadonly(readonly);

			ask.setRealm(getRealm());

			ask = upsert(ask);
		}
		// create one
		if (rootQuestion.getAttributeCode().startsWith(Question.QUESTION_GROUP_ATTRIBUTE_CODE)) {
			// Recurse!
			List<QuestionQuestion> qqList = new ArrayList<>(rootQuestion.getChildQuestions());
			Collections.sort(qqList); // sort by priority
			List<Ask> childAsks = new ArrayList<>();
			for (QuestionQuestion qq : qqList) {
				String qCode = qq.getPk().getTargetCode();
				log.info(qq.getPk().getSourceCode() + " -> Child Question -> " + qCode);
				Question childQuestion = findQuestionByCode(qCode);
				// now set defaultQST
				QuestionSourceTarget qst = findQST(qCode, qstArray, defaultQST);
				List<Ask> askChildren = null;
				try {
					askChildren = findAsksUsingQuestionSourceTarget(childQuestion, qst, qstArray, qq.getMandatory());
				} catch (Exception e) {
				}
				childAsks.addAll(askChildren);
			}
			Ask[] asksArray = childAsks.toArray(new Ask[0]);
			ask.setChildAsks(asksArray);

			ask.setRealm(getRealm());

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
		BaseEntity source = null;
		BaseEntity target = null;
		if ("PER_SOURCE".equals(sourceCode) && "PER_TARGET".equals(targetCode)) {
			source = new BaseEntity(sourceCode, "SourceCode");
			target = new BaseEntity(targetCode, "TargetCode");
		} else {
			source = findBaseEntityByCode(sourceCode);
			target = findBaseEntityByCode(targetCode);
		}
		return createAsksByQuestion2(rootQuestion, source, target);
	}

	public List<Ask> createAsksByQuestion2(final Question rootQuestion, final BaseEntity source,
			final BaseEntity target) {
		List<Ask> asks = findAsks2(rootQuestion, source, target);
		return asks;
	}

	public List<Ask> findAsksByTargetBaseEntityId(final Long id) {
		final List<Ask> results = getEntityManager()
				.createQuery("SELECT ea FROM Ask ea where ea.target.id=:baseEntityId").setParameter("baseEntityId", id)
				.getResultList();

		return results;

	}

	public List<Ask> findAsksByTargetBaseEntityCode(final String code) {
		final List<Ask> results = getEntityManager()

				.createQuery("SELECT ea FROM Ask ea where ea.target.code=:baseEntityCode and ea.realm=:realmStr")
				.setParameter("realmStr", getRealm()).setParameter("baseEntityCode", code).getResultList();

		return results;

	}

	public List<Ask> findAsksByRawAsk(final Ask ask) {
		final List<Ask> results = getEntityManager().createQuery(
				"SELECT ea FROM Ask ea where ea.targetCode=:targetCode and  ea.sourceCode=:sourceCode and ea.attributeCode=:attributeCode  and ea.realm=:realmStr")
				.setParameter("targetCode", ask.getTargetCode()).setParameter("sourceCode", ask.getSourceCode())
				.setParameter("attributeCode", ask.getAttributeCode()).setParameter("realmStr", getRealm())
				.getResultList();

		return results;

	}

	public List<Answer> findAnswersByRawAnswer(final Answer answer) {
		final List<Answer> results = getEntityManager().createQuery(
				"SELECT ea FROM Answer ea where ea.targetCode=:targetCode and  ea.sourceCode=:sourceCode and ea.attributeCode=:attributeCode  and ea.realm=:realmStr")
				.setParameter("targetCode", answer.getTargetCode()).setParameter("sourceCode", answer.getSourceCode())
				.setParameter("attributeCode", answer.getAttributeCode()).setParameter("realmStr", getRealm())
				.getResultList();

		return results;

	}

	public List<GPS> findGPSByTargetBaseEntityId(final Long id) {
		final List<GPS> results = getEntityManager()
				.createQuery("SELECT ea FROM GPS ea where ea.targetId=:baseEntityId").setParameter("baseEntityId", id)
				.getResultList();

		return results;

	}

	public List<GPS> findGPSByTargetBaseEntityCode(final String targetCode) {
		final List<GPS> results = getEntityManager()
				.createQuery("SELECT ea FROM GPS ea where ea.targetCode=:baseEntityCode  and ea.realm=:realmStr")
				.setParameter("realmStr", getRealm()).setParameter("baseEntityCode", targetCode).getResultList();

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
				.createQuery("SELECT ea FROM AnswerLink ea where ea.targetCode=:baseEntityCode  and ea.realm=:realmStr")
				.setParameter("realmStr", getRealm()).setParameter("baseEntityCode", targetCode).getResultList();

		return results;

	}

	public List<AnswerLink> findAnswersBySourceBaseEntityCode(final String sourceCode) {
		final List<AnswerLink> results = getEntityManager()
				.createQuery(
						"SELECT ea FROM AnswerLink ea where ea.pk.source.code=:baseEntityCode  and ea.realm=:realmStr")
				.setParameter("realmStr", getRealm()).setParameter("baseEntityCode", sourceCode).getResultList();

		return results;

	}

	public List<AnswerLink> findAnswersByTargetOrSourceBaseEntityCode(final String baseEntityCode) {
		final List<AnswerLink> results = getEntityManager().createQuery(
				"SELECT ea FROM AnswerLink ea where ea.sourceCode=:baseEntityCode or ea.targetCode=:baseEntityCode  and ea.realm=:realmStr")
				.setParameter("realmStr", getRealm()).setParameter("baseEntityCode", baseEntityCode).getResultList();

		return results;
	}

	public List<EntityEntity> findLinksBySourceOrTargetBaseEntityCode(final String baseEntityCode) {
		final List<EntityEntity> results = getEntityManager().createQuery(
				"SELECT ea FROM EntityEntity ea where ea.link.sourceCode=:baseEntityCode or ea.pk.targetCode=:baseEntityCode  and ea.realm=:realmStr")
				.setParameter("realmStr", getRealm()).setParameter("baseEntityCode", baseEntityCode).getResultList();

		return results;

	}

	public List<Question> findQuestions() throws NoResultException {

		final List<Question> results = getEntityManager()
				.createQuery("SELECT a FROM Question a where a.realm=:realmStr").setParameter("realmStr", getRealm())
				.getResultList();

		return results;
	}

	public List<Ask> findAsks() throws NoResultException {

		final List<Ask> results = getEntityManager().createQuery("SELECT a FROM Ask a where a.realm=:realmStr")
				.setParameter("realmStr", getRealm()).getResultList();

		return results;
	}

	public List<Ask> findAsksWithQuestions() throws NoResultException {

		// log.info("find asks Realm = " + securityService.getRealm());

		final List<Ask> results = getEntityManager()
				.createQuery("SELECT a FROM Ask a JOIN a.question q where a.realm=:realmStr and q.realm=:realmStr")
				.setParameter("realmStr", getRealm()).getResultList();

		return results;
	}

	public List<Attribute> findAttributes() throws NoResultException {

		List<Attribute> results = getEntityManager()
				.createQuery("SELECT a FROM Attribute a where a.realm=:realmStr and a.name not like 'App\\_%'")
				.setParameter("realmStr", getRealm())
				.getResultList();

		// Replace Validation in Attribute.DataType.validationList
		if(results.size() > 0) {
			// Get all validations, normally not many
			List<Validation> validations =  getEntityManager()
					.createQuery("SELECT v FROM Validation v where v.realm=:realmStr")
					.setParameter("realmStr", getRealm())
					.getResultList();
			HashMap<String , Validation> validationMapping = new HashMap<>();
			for (Validation validation : validations) {
				validationMapping.put(validation.getCode(), validation);
			}

			// Replace validation in Attribute
			for (Attribute attribute: results) {
				List<Validation> validationList = attribute.getDataType().getValidationList();
				// same definition as ValidationListConverter.convertToEntityAttribute
				List<Validation> newValidationList =  new CopyOnWriteArrayList<>();
				for (Validation validation : validationList) {
					Validation validationFromDB = validationMapping.getOrDefault(validation.getCode(), null);
					if(validationFromDB != null)  {
						newValidationList.add(validationFromDB);
					} else {
						// should not reach to here
						log.error("Can't find linked Validation:" + validation.getCode() + " for Attribute:" + attribute.getCode());
					}
				}
				attribute.getDataType().setValidationList(newValidationList);
			}
		}
		return results;
	}

	public List<Rule> findRules() throws NoResultException {

		final List<Rule> results = getEntityManager().createQuery("SELECT a FROM Rule a").getResultList();

		return results;
	}

	public List<AnswerLink> findAnswerLinks() throws NoResultException {

		final List<AnswerLink> results = getEntityManager().createQuery("SELECT a FROM AnswerLink a").getResultList();

		return results;
	}

	public List<EntityAttribute> findAttributesByBaseEntityCode(final String code) throws NoResultException {

		final List<EntityAttribute> ret = new ArrayList<>();
		BaseEntity be = this.findBaseEntityByCode(code);
		List<Object[]> results = getEntityManager().createQuery(

				"SELECT ea.pk.attribute,ea.privacyFlag,ea.weight,ea.inferred,ea.valueString,ea.valueBoolean,ea.valueDate, ea.valueDateTime,ea.valueDouble, ea.valueInteger,ea.valueLong FROM EntityAttribute ea where ea.pk.baseEntity.code=:baseEntityCode and ea.pk.baseEntity.realm=:realmStr")

				.setParameter("realmStr", getRealm()).setParameter("baseEntityCode", code).getResultList();

		// VERY UGLY (ACC)
		for (Object[] objectArray : results) {
			Attribute attribute = (Attribute) objectArray[0];
			Double weight = (Double) objectArray[2];
			Boolean privacyFlag = (Boolean) objectArray[1];
			Boolean inferred = (Boolean) objectArray[3];
			Object value = null;

			for (int i = 4; i < 11; i++) {
				if (objectArray[i] == null) {
					continue;
				}
				value = objectArray[i];
				break;
			}
			if (inRole("admin") && privacyFlag || !privacyFlag) {

				EntityAttribute ea = new EntityAttribute(be, attribute, weight, value);
				ea.setInferred(inferred);
				ea.setPrivacyFlag(privacyFlag);
				ret.add(ea);
			}
		}
		return ret;
	}

	public List<BaseEntity> findBaseEntitysByAttributeValues(final MultivaluedMap<String, String> params,
			final boolean includeAttributes, final Integer pageStart, final Integer pageSize) {

		final List<BaseEntity> eeResults;
		new HashMap<String, BaseEntity>();
		String realmStr = this.getRealm();

		if (includeAttributes) {

			// ugly and insecure
			final Integer pairCount = params.size();
			if (pairCount.equals(0)) {
				eeResults = getEntityManager().createQuery(
						"SELECT distinct be FROM BaseEntity be JOIN be.baseEntityAttributes bee and be.realm=:realmStr ") // add
						// company
						// limiter

						.setParameter("realmStr", getRealm()).setFirstResult(pageStart).setMaxResults(pageSize)

						.getResultList();

			} else {
				log.debug("PAIR COUNT IS NOT ZERO " + pairCount);
				String eaStrings = "";
				String eaStringsQ = "(";
				for (int i = 0; i < pairCount; i++) {

					eaStrings += ",EntityAttribute ea" + i;
					eaStringsQ += "ea" + i + ".baseEntityCode=be.code or ";
				}
				eaStringsQ = eaStringsQ.substring(0, eaStringsQ.length() - 4);
				eaStringsQ += ")";

				String queryStr = "SELECT distinct be FROM BaseEntity be" + eaStrings
						+ "  JOIN be.baseEntityAttributes bee where be.realm=:realmStr and " + eaStringsQ + " and  ";
				int attributeCodeIndex = 0;
				int valueIndex = 0;
				final List<String> attributeCodeList = new ArrayList<>();
				final List<String> valueList = new ArrayList<>();

				for (final Map.Entry<String, List<String>> entry : params.entrySet()) {
					if (entry.getKey().equals("pageStart") || entry.getKey().equals("pageSize")) { // ugly
						continue;
					}
					final List<String> qvalueList = entry.getValue();
					if (!qvalueList.isEmpty()) {
						// create the value or
						String valueQuery = "(";
						for (final String value : qvalueList) {
							valueQuery += "ea" + attributeCodeIndex + ".valueString=:valueString" + valueIndex + " or ";
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
						queryStr += " ea" + attributeCodeIndex + ".attributeCode=:attributeCode" + attributeCodeIndex
								+ " and " + valueQuery;
						log.debug("Key : " + entry.getKey() + " Value : " + entry.getValue());
					}
					attributeCodeIndex++;

				}
				final Query query = getEntityManager().createQuery(queryStr);
				int index = 0;
				for (final String attributeParm : attributeCodeList) {
					query.setParameter("attributeCode" + index, attributeParm);
					log.debug("attributeCode" + index + "=:" + attributeParm);
					index++;
				}
				index = 0;
				for (final String valueParm : valueList) {
					query.setParameter("valueString" + index, valueParm);
					log.debug("valueString" + index + "=:" + valueParm);
					index++;
				}

				query.setParameter("realmStr", getRealm());

				query.setFirstResult(pageStart).setMaxResults(pageSize);
				eeResults = query.getResultList();

			}
		} else {
			Log.info("**************** ENTITY ENTITY WITH NO ATTRIBUTES ****************");

			eeResults = getEntityManager().createQuery("SELECT be FROM BaseEntity be where be.realm=:realmStr")

					.setFirstResult(pageStart).setParameter("realmStr", getRealm()).setMaxResults(pageSize)

					.getResultList();

		}
		// TODO: improve

		return eeResults;
	}

	public Long findBaseEntitysByAttributeValuesCount(final MultivaluedMap<String, String> params) {

		final Long result;
		new HashMap<String, BaseEntity>();

		Log.info("**************** COUNT BE SEARCH WITH ATTRIBUTE VALUE WITH ATTRIBUTES!!  ****************");

		// ugly and insecure
		final Integer pairCount = params.size();
		if (pairCount.equals(0)) {
			result = (Long) getEntityManager().createQuery(
					"SELECT count(be.code) FROM BaseEntity be JOIN be.baseEntityAttributes bee and be.realm=:realmStr")

					.setParameter("realmStr", getRealm()).getSingleResult();

		} else {
			String queryStr = "SELECT count(be.code) FROM BaseEntity be JOIN be.baseEntityAttributes bee where be.realm=:realmStr and ";
			int attributeCodeIndex = 0;
			int valueIndex = 0;
			final List<String> attributeCodeList = new ArrayList<>();
			final List<String> valueList = new ArrayList<>();

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
					queryStr += " bee.attributeCode=:attributeCode" + attributeCodeIndex + " and " + valueQuery;
					log.debug("Key : " + entry.getKey() + " Value : " + entry.getValue());
				}
				attributeCodeIndex++;

			}
			log.debug("Query=" + queryStr);
			final Query query = getEntityManager().createQuery(queryStr);
			int index = 0;
			for (final String attributeParm : attributeCodeList) {
				query.setParameter("attributeCode" + index, attributeParm);
				log.debug("attributeCode" + index + "=:" + attributeParm);
				index++;
			}
			index = 0;
			for (final String valueParm : valueList) {
				query.setParameter("valueString" + index, valueParm);
				log.debug("valueString" + index + "=:" + valueParm);
				index++;
			}

			query.setParameter("realmStr", getRealm());

			result = (Long) query.getSingleResult();

		}

		return result;

	}

	public List<BaseEntity> findDescendantsByAttributeLink(@NotNull final String sourceCode, final String linkCode,
			final boolean includeAttributes, final Integer pageStart, final Integer pageSize) {

		final List<BaseEntity> eeResults;
		final Map<String, BaseEntity> beMap = new HashMap<>();

		if (includeAttributes) {
			Log.info("**************** ENTITY ENTITY DESCENDANTS WITH ATTRIBUTES!! pageStart = " + pageStart
					+ " pageSize=" + pageSize + " ****************");

			eeResults = getEntityManager().createQuery(
					"SELECT be FROM BaseEntity be,EntityEntity ee JOIN be.baseEntityAttributes bee where ee.pk.targetCode=be.code and ee.pk.attribute.code=:linkAttributeCode and ee.pk.source.code=:sourceCode")
					.setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode)

					.setParameter("realmStr", getRealm()).setFirstResult(pageStart).setMaxResults(pageSize)
					.getResultList();

			if (eeResults.isEmpty()) {
				log.debug("EEE IS EMPTY");
			} else {
				log.debug("EEE Count" + eeResults.size());
				log.debug("EEE" + eeResults);
			}
			return eeResults;

		} else {
			Log.info("**************** ENTITY ENTITY WITH NO ATTRIBUTES ****************");

			eeResults = getEntityManager().createQuery(
					"SELECT be FROM BaseEntity be,EntityEntity ee where ee.pk.targetCode=be.code and ee.pk.attribute.code=:linkAttributeCode and ee.pk.source.code=:sourceCode")
					.setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode)

					.setParameter("realmStr", getRealm()).setFirstResult(pageStart).setMaxResults(pageSize)
					.getResultList();

			for (final BaseEntity be : eeResults) {
				beMap.put(be.getCode(), be);
				Log.info("BECODE:" + be.getCode());
			}
		}

		return beMap.values().stream().collect(Collectors.toList());
	}

	public List<Link> findLinks(@NotNull final String targetCode) {

		final List<Link> eeResults;
		eeResults = getEntityManager().createQuery(

				"SELECT ee.link FROM EntityEntity ee where  ee.pk.targetCode=:targetCode and ee.pk.source.realm=:realmStr")
				.setParameter("targetCode", targetCode)
				.setParameter("realmStr", getRealm()).getResultList();

		return eeResults;
	}

	public Long findLinksCount(@NotNull final String targetCode) {

		Query query = getEntityManager().createQuery(

				"SELECT count(ee.link) FROM EntityEntity ee where  ee.pk.targetCode=:targetCode and ee.pk.source.realm=:realmStr")
				.setParameter("targetCode", targetCode)
				.setParameter("realmStr", getRealm());

		Long count = (Long) query.getSingleResult();
		return count;
	}

	public List<Link> findLinks(@NotNull final String targetCode, final String linkCode) {

		final List<Link> eeResults;
		eeResults = getEntityManager().createQuery(

				"SELECT ee.link FROM EntityEntity ee where  ee.pk.targetCode=:targetCode and ee.pk.attribute.code=:linkAttributeCode and ee.pk.source.realm=:realmStr")
				.setParameter("targetCode", targetCode).setParameter("linkAttributeCode", linkCode)
				.setParameter("realmStr", getRealm()).getResultList();

		return eeResults;
	}

	public Long findLinksCount(@NotNull final String targetCode, final String linkCode) {

		Query query = getEntityManager().createQuery(

				"SELECT count(ee.link) FROM EntityEntity ee where  ee.pk.targetCode=:targetCode and ee.pk.attribute.code=:linkAttributeCode  and ee.pk.source.realm=:realmStr")
				.setParameter("targetCode", targetCode).setParameter("linkAttributeCode", linkCode)
				.setParameter("realmStr", getRealm());

		Long count = (Long) query.getSingleResult();
		return count;
	}

	public List<Link> findParentLinks(@NotNull final String targetCode) {

		final List<Link> eeResults;
		eeResults = getEntityManager().createQuery(

				"SELECT ee.link FROM EntityEntity ee where  ee.pk.targetCode=:targetCode and ee.pk.source.realm=:realmStr")
				.setParameter("targetCode", targetCode)
				.setParameter("realmStr", getRealm()).getResultList();

		return eeResults;
	}

	public Long findParentLinksCount(@NotNull final String targetCode) {

		Long eeResults;
		eeResults = (Long) getEntityManager().createQuery(

				"SELECT count(ee.link) FROM EntityEntity ee where  ee.pk.targetCode=:targetCode and ee.pk.source.realm=:realmStr")
				.setParameter("targetCode", targetCode)
				.setParameter("realmStr", getRealm()).getSingleResult();

		return eeResults;
	}

	public List<Link> findParentLinks(@NotNull final String targetCode, final String linkCode) {

		final List<Link> eeResults;
		eeResults = getEntityManager().createQuery(

				"SELECT ee.link FROM EntityEntity ee where  ee.pk.targetCode=:targetCode and ee.pk.attribute.code=:linkAttributeCode and ee.pk.source.realm=:realmStr")
				.setParameter("targetCode", targetCode).setParameter("linkAttributeCode", linkCode)
				.setParameter("realmStr", getRealm()).getResultList();

		return eeResults;
	}

	public Long findParentLinksCount(@NotNull final String targetCode, final String linkCode) {

		Long eeResults;
		eeResults = (Long) getEntityManager().createQuery(

				"SELECT count(ee.link) FROM EntityEntity ee where  ee.pk.targetCode=:targetCode and ee.pk.attribute.code=:linkAttributeCode and ee.pk.source.realm=:realmStr")
				.setParameter("targetCode", targetCode).setParameter("linkAttributeCode", linkCode)
				.setParameter("realmStr", getRealm()).getSingleResult();

		return eeResults;
	}

	public List<Link> findParentLinks(@NotNull final String targetCode, final String linkCode, final String value) {

		final List<Link> eeResults;
		eeResults = getEntityManager().createQuery(
				"SELECT ee.link FROM EntityEntity ee where  ee.link.targetCode=:targetCode and ee.link.linkValue=:linkValue and ee.link.attributeCode=:linkAttributeCode and ee.pk.source.realm=:realmStr")
				.setParameter("targetCode", targetCode).setParameter("linkAttributeCode", linkCode)

				.setParameter("linkValue", value).setParameter("realmStr", getRealm()).getResultList();

		return eeResults;
	}

	public Long findParentLinksCount(@NotNull final String targetCode, final String linkCode, final String value) {

		final Long eeResults;
		eeResults = (Long) getEntityManager().createQuery(
				"SELECT count(ee.link) FROM EntityEntity ee where  ee.link.targetCode=:targetCode and ee.link.linkValue=:linkValue and ee.link.attributeCode=:linkAttributeCode and ee.pk.source.realm=:realmStr")
				.setParameter("targetCode", targetCode).setParameter("linkAttributeCode", linkCode)

				.setParameter("linkValue", value).setParameter("realmStr", getRealm()).getSingleResult();

		return eeResults;
	}

	public List<Link> findChildLinks(@NotNull final String sourceCode, final String linkCode, final String value) {

		final List<Link> eeResults;
		eeResults = getEntityManager().createQuery(
				"SELECT ee.link FROM EntityEntity ee where  ee.link.sourceCode=:sourceCode and ee.link.linkValue=:linkValue and ee.link.attributeCode=:linkAttributeCode and ee.pk.source.realm=:realmStr")
				.setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode)

				.setParameter("linkValue", value).setParameter("realmStr", getRealm()).getResultList();

		return eeResults;
	}

	public Long findChildLinksCount(@NotNull final String sourceCode, final String linkCode, final String value) {

		final Long eeResults;
		eeResults = (Long) getEntityManager().createQuery(
				"SELECT count(ee.link) FROM EntityEntity ee where  ee.link.sourceCode=:targetCode and ee.link.linkValue=:linkValue and ee.link.attributeCode=:linkAttributeCode and ee.pk.source.realm=:realmStr")
				.setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode)

				.setParameter("linkValue", value).setParameter("realmStr", getRealm()).getSingleResult();

		return eeResults;
	}

	public List<Link> findChildLinks(@NotNull final String sourceCode, final String linkCode) {

		final List<Link> eeResults;
		eeResults = getEntityManager().createQuery(

				"SELECT ee.link FROM EntityEntity ee where  ee.pk.source.code=:sourceCode and ee.pk.attribute.code=:linkAttributeCode and ee.pk.source.realm=:realmStr")
				.setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode)
				.setParameter("realmStr", getRealm()).getResultList();

		return eeResults;
	}

	public Long findChildLinksCount(@NotNull final String sourceCode, final String linkCode) {

		final Long eeResults;
		eeResults = (Long) getEntityManager().createQuery(

				"SELECT count(ee.link) FROM EntityEntity ee where  ee.pk.source.code=:sourceCode and ee.pk.attribute.code=:linkAttributeCode and ee.pk.source.realm=:realmStr")
				.setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode)
				.setParameter("realmStr", getRealm()).getSingleResult();

		return eeResults;
	}

	public List<Link> findChildLinks(@NotNull final String sourceCode) {

		final List<Link> eeResults;
		eeResults = getEntityManager().createQuery(

				"SELECT ee.link FROM EntityEntity ee where  ee.pk.source.code=:sourceCode and ee.pk.source.realm=:realmStr")
				.setParameter("sourceCode", sourceCode)
				.setParameter("realmStr", getRealm()).getResultList();

		return eeResults;
	}

	public Long findChildLinksCount(@NotNull final String sourceCode) {

		final Long eeResults;
		eeResults = (Long) getEntityManager().createQuery(

				"SELECT count(ee.link) FROM EntityEntity ee where  ee.pk.source.code=:sourceCode and ee.pk.source.realm=:realmStr")
				.setParameter("sourceCode", sourceCode)
				.setParameter("realmStr", getRealm()).getSingleResult();

		return eeResults;
	}

	public Link findLink(final String sourceCode, final String targetCode, final String linkCode)
			throws NoResultException {
		Link ee = null;

		try {
			ee = (Link) getEntityManager().createQuery(
					"SELECT ee.link FROM EntityEntity ee where ee.link.targetCode=:targetCode and ee.link.attributeCode=:linkAttributeCode and ee.link.sourceCode=:sourceCode and ee.realm=:realmStr")
					.setParameter("sourceCode", sourceCode).setParameter("linkAttributeCode", linkCode)

					.setParameter("targetCode", targetCode).setParameter("realmStr", getRealm()).getSingleResult();

		} catch (Exception e) {
			throw new NoResultException("Link " + sourceCode + ":" + targetCode + ":" + linkCode + " not found");
		}
		return ee;
	}

	public Integer updateEntityEntity(final Link link) throws NoResultException {
		Integer result = 0;

		Link oldLink = findLink(link.getSourceCode(), link.getTargetCode(), link.getAttributeCode());

		try {
			result = getEntityManager().createQuery(
					"update EntityEntity  set linkValue =:linkValue, parentColor=:parentColor, childColor=:childColor, rule=:rule, weight=:weight, link.weight=:weight, link.parentColor=:parentColor, link.childColor=:childColor, link.rule=:rule, link.linkValue=:linkValue  where link.targetCode=:targetCode and link.attributeCode=:linkAttributeCode and link.sourceCode=:sourceCode")
					.setParameter("sourceCode", link.getSourceCode())
					.setParameter("linkAttributeCode", link.getAttributeCode())
					.setParameter("targetCode", link.getTargetCode()).setParameter("linkValue", link.getLinkValue())
					.setParameter("parentColor", link.getParentColor()).setParameter("childColor", link.getChildColor())

					.setParameter("realmStr", getRealm()).setParameter("rule", link.getRule())
					.setParameter("weight", link.getWeight()).executeUpdate();

			QEventLinkChangeMessage msg = new QEventLinkChangeMessage(link, oldLink, getCurrentToken());

			sendQEventLinkChangeMessage(msg);
			log.debug("Sent Event Link Change Msg " + msg);

		} catch (Exception e) {
			throw new NoResultException("EntityEntity " + link + " not found");
		}
		return result;
	}

	public Integer updateEntityEntity(final EntityEntity ee) {
		Integer result = 0;

		try {
			String sql = "update EntityEntity ee set ee.weight=:weight, ee.valueString=:valueString, ee.link.weight=:weight, ee.link.linkValue=:valueString where ee.pk.targetCode=:targetCode and ee.link.attributeCode=:linkAttributeCode and ee.link.sourceCode=:sourceCode";
			result = getEntityManager().createQuery(sql).setParameter("sourceCode", ee.getPk().getSource().getCode())
					.setParameter("linkAttributeCode", ee.getLink().getAttributeCode())
					.setParameter("targetCode", ee.getPk().getTargetCode()).setParameter("weight", ee.getWeight())
					.setParameter("valueString", ee.getValueString()).executeUpdate();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public EntityEntity findEntityEntity(final String sourceCode, final String targetCode, final String linkCode)
			throws NoResultException {

		// find the BaseEntity
		BaseEntity source = this.findBaseEntityByCode(sourceCode);

		// now loop through this baseentity to find the actual ee (avoid the direct look
		// up loop)
		for (EntityEntity ee : source.getLinks()) {
			if (ee.getLink().getAttributeCode().equals(linkCode) && ee.getLink().getTargetCode().equals(targetCode)) {
				return ee;
			}
		}

		throw new NoResultException("EntityEntity " + sourceCode + ":" + targetCode + ":" + linkCode + " not found");
	}

	public EntityAttribute findEntityAttribute(final String baseEntityCode, final String attributeCode) {

		// find the BaseEntity
		BaseEntity source = this.findBaseEntityByCode(baseEntityCode);

		for (EntityAttribute ea : source.getBaseEntityAttributes()) {
			if (ea.getAttributeCode().equals(attributeCode) && ea.getBaseEntityCode().equals(baseEntityCode)) {
				return ea;
			}
		}
		log.error("EntityAttribute " + baseEntityCode + ":" + attributeCode + " not found");
		return null;
	}

	// @Transactional
	public void removeEntityEntity(final String code, final EntityEntity ee) {
		try {
			Link oldLink = ee.getLink();
			BaseEntity source = findBaseEntityByCode(code);
			source.getLinks().remove(ee);
			getEntityManager().merge(source);
			this.writeToDDT(source);
			getEntityManager().remove(ee);
			QEventLinkChangeMessage msg = new QEventLinkChangeMessage(null, oldLink, getCurrentToken());
			sendQEventLinkChangeMessage(msg);
			log.debug("Sent Event Link Change Msg " + msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// @Transactional
	public void removeAnswerLink(final String code, final AnswerLink answerLink) {
		try {
			BaseEntity source = findBaseEntityByCode(code);
			source.getAnswers().remove(answerLink);
			getEntityManager().merge(source);
			this.writeToDDT(source);
			getEntityManager().remove(answerLink);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	// @Transactional
	public void removeQuestionQuestion(final String parentCode, final String targetCode) {
		QuestionQuestion qq = null;
		try {
			qq = findQuestionQuestionByCode(parentCode, targetCode);
			removeQuestionQuestion(qq);
		} catch (Exception e) {
			log.error("QuestionQuestion " + parentCode + ":" + targetCode + " not found");
		}
	}

	// @Transactional
	public void removeQuestionQuestion(final QuestionQuestion qq) {
		try {
			Question question = findQuestionByCode(qq.getPk().getSourceCode());
			question.getChildQuestions().remove(qq);
			getEntityManager().merge(question);
			getEntityManager().remove(qq);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// @Transactional
	public void removeEntityAttribute(final String baseEntityCode, final String attributeCode) {
		EntityAttribute ea = null;
		ea = findEntityAttribute(baseEntityCode, attributeCode);
	    if( ea != null) removeEntityAttribute(ea);
	}

	// @Transactional
	public void removeEntityAttribute(final EntityAttribute ea) {
		try {
			BaseEntity source = findBaseEntityByCode(ea.getBaseEntityCode());
			source.getBaseEntityAttributes().remove(ea);
			getEntityManager().merge(source);
			this.writeToDDT(source);
			getEntityManager().remove(ea);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// @Transactional
	public void removeEntityAttributes(final String attributeCode) {
		try {
			String hql ="delete from EntityAttribute ea where ea.attributeCode = :attributeCode and ea.realm = :realmStr";

			Query query = getEntityManager().createQuery(hql);

			query.setParameter("attributeCode", attributeCode);

			query.setParameter("realmStr", getRealm());

			int total = query.executeUpdate();
			log.info(String.format("DEBUG, Removed %d records from table EntityAttribute for Attribute code:%s, realm:%s.",
						total, attributeCode, getRealm()));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public EntityEntity addLink(final String sourceCode, final String targetCode, final String linkCode, Object value,
			Double weight) throws IllegalArgumentException, BadDataException {

		return addLink(sourceCode, targetCode, linkCode, value, weight, true);
	}

	// @Transactional
	public EntityEntity addLink(final String sourceCode, final String targetCode, final String linkCode, Object value,
			Double weight, final boolean changeEvent) throws IllegalArgumentException, BadDataException {
		EntityEntity ee = null;

		try {
			Link link = findLink(sourceCode, targetCode, linkCode);
			BaseEntity source = findBaseEntityByCode(link.getSourceCode());
			BaseEntity target = findBaseEntityByCode(link.getTargetCode());
			Attribute attribute = findAttributeByCode(link.getAttributeCode());
			ee = new EntityEntity(source, target, attribute, weight, value);
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
				throw new IllegalArgumentException("targetCode " + targetCode + " not found");
			}

			try {
				linkAttribute = findAttributeLinkByCode(linkCode);
			} catch (NoResultException es) {
				throw new IllegalArgumentException("linkCode" + linkCode + " not found");
			}

			ee = beSource.addTarget(beTarget, linkAttribute, weight, value);
			beSource = getEntityManager().merge(beSource);
			this.writeToDDT(beSource); // This is to ensure the BE in cache has its links updated

			if (changeEvent) {
				QEventLinkChangeMessage msg = new QEventLinkChangeMessage(ee.getLink(), null, getCurrentToken());

				sendQEventLinkChangeMessage(msg);
				log.debug("Sent Event Link Change Msg " + msg);
			}

		}
		return ee;
	}

	// @Transactional
	public void removeLink(final Link link) {
		EntityEntity ee = null;

		try {
			ee = findEntityEntity(link.getSourceCode(), link.getTargetCode(), link.getAttributeCode());
			removeEntityEntity(link.getSourceCode(), ee);
			QEventLinkChangeMessage msg = new QEventLinkChangeMessage(null, ee.getLink(), getCurrentToken());

			sendQEventLinkChangeMessage(msg);
			log.debug("Sent Event Link Change Msg " + msg);

		} catch (Exception e) {
			log.error("EntityEntity " + link + " not found");
		}
	}

	// @Transactional
	public void removeLink(final String sourceCode, final String targetCode, final String linkCode) {
		EntityEntity ee = null;

		try {
			ee = findEntityEntity(sourceCode, targetCode, linkCode);
			removeEntityEntity(sourceCode, ee);

		} catch (Exception e) {
			log.error("EntityEntity " + sourceCode + ":" + targetCode + ":" + linkCode + " not found");
		}
	}

	// @Transactional
	public EntityEntity updateLink(final Link link) throws IllegalArgumentException, BadDataException {
		EntityEntity ee = null;

		ee = findEntityEntity(link.getSourceCode(), link.getTargetCode(), link.getAttributeCode());

		ee.setValue(link.getLinkValue());
		ee.setWeight(link.getWeight());
		ee.getLink().setChildColor(link.getChildColor());
		ee.getLink().setParentColor(link.getParentColor());
		ee.getLink().setRule(link.getRule());

		ee = getEntityManager().merge(ee);
		return ee;
	}

	public void importBaseEntitys(final InputStream in, final String filename) {
		// import csv
		String line = "";
		final String cvsSplitBy = ",";
		boolean headerLine = true;
		final Map<Integer, Attribute> attributes = new HashMap<>();

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

							attribute.setRealm(getRealm());

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

					entity.setRealm(getRealm());

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
							final Optional<EntityAttribute> firstname = entity.findEntityAttribute("PRI_FIRSTNAME");
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

	public Long importKeycloakUsers(final String keycloakUrl, final String realm, final String username,
			final String password, final String clientId, final Integer maxReturned, final String parentGroupCodes) {
		Long count = 0L;

		AttributeLink linkAttribute = this.findAttributeLinkByCode("LNK_CORE");
		List<BaseEntity> parentGroupList = new ArrayList<>();

		String[] parentCodes = parentGroupCodes.split(",");
		for (String parentCode : parentCodes) {
			BaseEntity group = null;

			try {
				group = this.findBaseEntityByCode(parentCode); // careful as GRPUSERS needs to
				parentGroupList.add(group);
			} catch (NoResultException e) {
				log.debug("Group Code does not exist :" + parentCode);
			}

		}

		KeycloakService ks;
		final Map<String, Map<String, Object>> usersMap = new HashMap<>();

		try {
			ks = new KeycloakService(keycloakUrl, realm, username, password, clientId);
			final List<LinkedHashMap> users = ks.fetchKeycloakUsers(maxReturned);
			for (final Object user : users) {
				final LinkedHashMap map = (LinkedHashMap) user;
				final Map<String, Object> userMap = new HashMap<>();
				for (final Object key : map.keySet()) {
					// log.debug(key + ":" + map.get(key));
					userMap.put((String) key, map.get(key));

				}
				usersMap.put((String) userMap.get("username"), userMap);

			}

			log.debug("finished");
		} catch (final IOException e) {
			e.printStackTrace();
		}

		for (final String kcusername : usersMap.keySet()) {
			final MultivaluedMap<String, String> params = new MultivaluedMapImpl<>();
			params.add("PRI_USERNAME", kcusername);
			final Map<String, Object> userMap = usersMap.get(kcusername);

			final List<BaseEntity> users = findBaseEntitysByAttributeValues(params, true, 0, 1);
			if (users.isEmpty()) {
				final String code = "PER_CH40_"
						+ kcusername.toUpperCase().replaceAll("\\ ", "").replaceAll("\\.", "").replaceAll("\\&", "");
				log.debug("New User Code = " + code);
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

					user.setRealm(getRealm());

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
					log.debug("BE:" + user);
				} catch (final BadDataException e) {
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
			log.debug("Null Keycloak Context");
			return;
		}

		final BaseEntity be = new BaseEntity("Test BaseEntity");

		be.setRealm(getRealm());

		be.setCode(BaseEntity.getDefaultCodePrefix() + "TEST");
		getEntityManager().persist(be);

		Person edison = new Person("Thomas Edison");

		edison.setRealm(getRealm());

		edison.setCode(Person.getDefaultCodePrefix() + "EDISON");
		getEntityManager().persist(edison);

		final Person tesla = new Person("Nikola Tesla");

		tesla.setRealm(getRealm());

		tesla.setCode(Person.getDefaultCodePrefix() + "TESLA");
		getEntityManager().persist(tesla);

		final Company crowtech = new Company("crowtech", "Crowtech Pty Ltd");

		crowtech.setRealm(getRealm());

		crowtech.setCode(Company.getDefaultCodePrefix() + "CROWTECH");
		getEntityManager().persist(crowtech);

		final Company spacex = new Company("spacex", "SpaceX");

		spacex.setRealm(getRealm());

		spacex.setCode(Company.getDefaultCodePrefix() + "SPACEX");
		getEntityManager().persist(spacex);

		final Product bmw316i = new Product("bmw316i", "BMW 316i");

		bmw316i.setRealm(getRealm());

		bmw316i.setCode(Product.getDefaultCodePrefix() + "BMW316I");
		getEntityManager().persist(bmw316i);

		final Product mazdaCX5 = new Product("maxdacx5", "Mazda CX-5");

		mazdaCX5.setRealm(getRealm());

		mazdaCX5.setCode(Product.getDefaultCodePrefix() + "MAXDACX5");
		getEntityManager().persist(mazdaCX5);

		final AttributeText attributeText1 = new AttributeText(Attribute.getDefaultCodePrefix() + "TEST1", "Test 1");
		getEntityManager().persist(attributeText1);
		final AttributeText attributeText2 = new AttributeText(Attribute.getDefaultCodePrefix() + "TEST2", "Test 2");
		getEntityManager().persist(attributeText2);
		final AttributeText attributeText3 = new AttributeText(Attribute.getDefaultCodePrefix() + "TEST3", "Test 3");
		getEntityManager().persist(attributeText3);

		Person person = new Person("Barry Allen");
		person.setCode(Person.getDefaultCodePrefix() + "FLASH");
		getEntityManager().persist(person);

		try {
			person.addAttribute(attributeText1, 1.0);
			person.addAttribute(attributeText2, 0.8);
			person.addAttribute(attributeText3, 0.6, 3147);

			// Link some BaseEntities
			final AttributeText link1 = new AttributeText(Attribute.getDefaultCodePrefix() + "LINK1", "Link1");
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
			e.printStackTrace();
		}
	}

	public void importKeycloakUsers(final List<Group> parentGroupList, final AttributeLink linkAttribute,
			final Integer maxReturned) throws IOException, BadDataException {

		final Map<String, String> envParams = System.getenv();
		String keycloakUrl = envParams.get("KEYCLOAKURL");
		log.debug("Keycloak URL=[" + keycloakUrl + "]");
		keycloakUrl = keycloakUrl.replaceAll("'", "");
		final String realm = envParams.get("KEYCLOAK_getRealm()");
		final String username = envParams.get("KEYCLOAK_USERNAME");
		final String password = envParams.get("KEYCLOAK_PASSWORD");
		final String clientid = envParams.get("KEYCLOAK_CLIENTID");
		final String secret = envParams.get("KEYCLOAK_SECRET");

		log.debug("Realm is :[" + realm + "]");

		final KeycloakService kcs = new KeycloakService(keycloakUrl, realm, username, password, clientid, secret);
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
			log.debug("Code=" + newUser.getCode());
			;
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
		log.debug(users);
	}

	public Link moveLink(final String originalSourceCode, final String targetCode, final String linkCode,
			final String destinationSourceCode) throws IllegalArgumentException {
		Link ee = null;

		try {
			Link oldLink = findLink(originalSourceCode, targetCode, linkCode);
			// add new link
			EntityEntity eee = addLink(destinationSourceCode, targetCode, linkCode, oldLink.getLinkValue(),
					oldLink.getWeight());
			removeLink(oldLink);
			ee = eee.getLink();
			QEventLinkChangeMessage msg = new QEventLinkChangeMessage(ee, oldLink, getCurrentToken());

			sendQEventLinkChangeMessage(msg);
			log.debug("Sent Event Link Change Msg " + msg);

		} catch (Exception e) {
			log.error("linkCode" + linkCode + " not found");
		}
		return ee;
	}

	public BaseEntity getUser() {
		return null;
	}

	// @Transactional
	public Long update(final QBaseMSGMessageTemplate template) {

		template.setRealm(getRealm());

		QBaseMSGMessageTemplate temp = getEntityManager().merge(template);
		log.debug("klnsnfklsdjfjsdfjklsfsdf " + temp);
		return template.getId();
	}

	// @Transactional
	public Long insert(final QBaseMSGMessageTemplate template) {

		template.setRealm(getRealm());

		try {
			getEntityManager().persist(template);

		} catch (final EntityExistsException e) {
			e.printStackTrace();

		}
		return template.getId();
	}

	public QBaseMSGMessageTemplate findTemplateByCode(@NotNull final String templateCode) throws NoResultException {

		QBaseMSGMessageTemplate result = null;

		result = (QBaseMSGMessageTemplate) getEntityManager().createQuery(
				"SELECT temp FROM QBaseMSGMessageTemplate temp where temp.code=:templateCode and temp.realm=:realmStr")
				.setParameter("realmStr", getRealm()).setParameter("templateCode", templateCode.toUpperCase())
				.getSingleResult();

		return result;

	}

	public QBaseMSGMessageTemplate findTemplateByCode(@NotNull final String templateCode, @NotNull final String realm)
			throws NoResultException {
		QBaseMSGMessageTemplate result = null;
		try {
			result = (QBaseMSGMessageTemplate) getEntityManager().createQuery(
					"SELECT temp FROM QBaseMSGMessageTemplate temp where temp.code=:templateCode and temp.realm=:realmStr")
					.setParameter("realmStr", realm).setParameter("templateCode", templateCode.toUpperCase())
					.getSingleResult();
		} catch (Exception e) {
			return null;
		}

		return result;
	}

	public Boolean inRole(final String role) {
		return true; // allow for qwanda-services
	}

	public void writeToDDT(final String key, final String value) {
		ddtCacheMock.put(key, value);
	}

	public void writeToDDT(final BaseEntity be) {
		ddtCacheMock.put(be.getCode(), JsonUtils.toJson(be));
	}

	public void writeToDDT(final Question q) {
		ddtCacheMock.put(q.getCode(), JsonUtils.toJson(q));
	}

	public void updateDDT(final String key, final String value) {
		log.info("Update DDT " + key);
	}

	public String readFromDDT(final String key) {

		return ddtCacheMock.get(key);
	}

	public void pushAttributes() {
		log.info("Pushing attributes to DDT");
	}

	public void writeToDDT(final List<BaseEntity> bes) {
		log.info("Pushing baseentitys to DDT cache");
		for (BaseEntity be : bes) {
			writeToDDT(be);
		}
	}

	public void writeQuestionsToDDT(final List<Question> qs) {
		log.info("Pushing Questions to DDT cache");
		for (Question q : qs) {
			writeToDDT(q);
		}
	}

	public String getToken() {
		return "DUMMY";
	}

	public BaseEntity addAttributes(BaseEntity be) {
		for (EntityAttribute entity : be.getBaseEntityAttributes()) {
			Attribute attribute = null;
			if (entity.getAttribute() == null) {

				try {
					attribute = findAttributeByCode(entity.getAttributeCode());
				} catch (NoResultException e) {
					// Create it (ideally if user is admin)
					if (entity.getAttributeCode().startsWith("PRI_IS_")) {
						attribute = new AttributeBoolean(entity.getAttributeCode(),
								StringUtils.capitalize(entity.getAttributeCode().substring(4).toLowerCase()));
					} else {

						if (entity.getValueInteger() != null) {

							attribute = new AttributeInteger(entity.getAttributeCode(),
									StringUtils.capitalize(entity.getAttributeCode().substring(4).toLowerCase()));
						} else if (entity.getValueDateTime() != null) {

							attribute = new AttributeDateTime(entity.getAttributeCode(),
									StringUtils.capitalize(entity.getAttributeCode().substring(4).toLowerCase()));
						}
						if (entity.getValueLong() != null) {

							attribute = new AttributeLong(entity.getAttributeCode(),
									StringUtils.capitalize(entity.getAttributeCode().substring(4).toLowerCase()));
						}
						if (entity.getValueTime() != null) {

							attribute = new AttributeTime(entity.getAttributeCode(),
									StringUtils.capitalize(entity.getAttributeCode().substring(4).toLowerCase()));
						}
						if (entity.getValueMoney() != null) {

							attribute = new AttributeMoney(entity.getAttributeCode(),
									StringUtils.capitalize(entity.getAttributeCode().substring(4).toLowerCase()));
						}
						if (entity.getValueDouble() != null) {

							attribute = new AttributeDouble(entity.getAttributeCode(),
									StringUtils.capitalize(entity.getAttributeCode().substring(4).toLowerCase()));
						}
						if (entity.getValueBoolean() != null) {

							attribute = new AttributeBoolean(entity.getAttributeCode(),
									StringUtils.capitalize(entity.getAttributeCode().substring(4).toLowerCase()));
						}
						if (entity.getValueDate() != null) {

							attribute = new AttributeDate(entity.getAttributeCode(),
									StringUtils.capitalize(entity.getAttributeCode().substring(4).toLowerCase()));
						} else {
							attribute = new AttributeText(entity.getAttributeCode(),
									StringUtils.capitalize(entity.getAttributeCode().substring(4).toLowerCase()));
						}
					}
					insert(attribute);
					attribute = findAttributeByCode(entity.getAttributeCode());
				}
				entity.setAttribute(attribute);
			}
		}
		return be;
	}

	/**
	 * fetches V1 Layouts from db
	 * 
	 * @param table
	 * @return response of the synchronization
	 */

	public QDataSubLayoutMessage fetchSubLayoutsFromDb(final String realm, final String gitrealm, final String branch) {

		QDataSubLayoutMessage ret = null;

		SearchEntity searchBE = new SearchEntity("Sublayouts", "Sublayouts")
				.addSort("PRI_CREATED", "Created", SearchEntity.Sort.DESC)
				.addFilter("PRI_CODE", SearchEntity.StringFilter.LIKE, "LAY_%")
				// .addFilter("PRI_VERSION",SearchEntity.StringFilter.EQUAL,"V1")
				// .addFilter("PRI_BRANCH",SearchEntity.StringFilter.EQUAL,branch)
				// .addFilter("PRI_GITgetRealm()",SearchEntity.StringFilter.EQUAL,gitrealm)
				.setPageStart(0).setPageSize(10000);

		List<BaseEntity> layouts = findBySearchBE(searchBE);

		Layout[] layoutArray = new Layout[layouts.size()];

		// Convert BaseEntity to Layout
		int index = 0;
		for (BaseEntity be : layouts) {
			try {
				layoutArray[index++] = new Layout(be.getValue("PRI_LAYOUT_NAME").get().toString(),
						be.getValue("PRI_LAYOUT_DATA").get().toString(), be.getValue("PRI_LAYOUT_URL").get().toString(),
						be.getValue("PRI_LAYOUT_URI").get().toString(),
						be.getValue("PRI_LAYOUT_MODIFIED_DATE").get().toString());
			} catch (Exception e) {
			}
		}

		ret = new QDataSubLayoutMessage(layoutArray, getToken());

		return ret;
	}

	public void saveLayouts(final List<BaseEntity> layouts, final String gitrealm, final String version,
			final String branch) {

		Attribute layoutDataAttribute = findAttributeByCode("PRI_LAYOUT_DATA");
		Attribute layoutURLAttribute = findAttributeByCode("PRI_LAYOUT_URL");
		Attribute layoutURIAttribute = findAttributeByCode("PRI_LAYOUT_URI");
		Attribute layoutNameAttribute = findAttributeByCode("PRI_LAYOUT_NAME");
		Attribute layoutModifiedDateAttribute = findAttributeByCode("PRI_LAYOUT_MODIFIED_DATE");
		Attribute layoutVersionAttribute = findAttributeByCode("PRI_VERSION");
		Attribute layoutBranchAttribute = findAttributeByCode("PRI_BRANCH");
		Attribute layoutGitRealmAttribute = findAttributeByCode("PRI_GITgetRealm()");

		for (BaseEntity layout : layouts) {
			// so save the layout
			BaseEntity newLayout = null;
			try {
				newLayout = findBaseEntityByCode(layout.getCode());
			} catch (NoResultException e) {
				log.info("New Layout detected");
			}
			if (newLayout == null) {
				newLayout = new BaseEntity(layout.getCode(), layout.getName());
			} else {
				int newData = layout.getValue("PRI_LAYOUT_DATA").get().toString().hashCode();
				int oldData = newLayout.findEntityAttribute(layoutDataAttribute).getAsString().hashCode();
				if (newData == oldData) {
					continue;
				}
			}
			newLayout.setRealm(getRealm());
			newLayout.setUpdated(layout.getUpdated());

			// upsert(newLayout);

			try {
				newLayout.addAttribute(layoutDataAttribute, 0.0, layout.getValue("PRI_LAYOUT_DATA").get().toString());
				newLayout.addAttribute(layoutURLAttribute, 0.0, layout.getValue("PRI_LAYOUT_URL").get().toString());
				newLayout.addAttribute(layoutURIAttribute, 0.0, layout.getValue("PRI_LAYOUT_URI").get().toString());
				newLayout.addAttribute(layoutNameAttribute, 0.0, layout.getValue("PRI_LAYOUT_NAME").get().toString());
				newLayout.addAttribute(layoutModifiedDateAttribute, 0.0,
						layout.getValue("PRI_LAYOUT_MODIFIED_DATE").get().toString());
				newLayout.addAttribute(layoutBranchAttribute, 0.0, branch);
				newLayout.addAttribute(layoutVersionAttribute, 0.0, version);
				newLayout.addAttribute(layoutGitRealmAttribute, 0.0, gitrealm);

			} catch (final BadDataException e) {
				e.printStackTrace();
			}
			try {
				updateWithAttributes(newLayout);
				addLink("GRP_LAYOUTS", newLayout.getCode(), "LNK_CORE", "LAYOUT", 1.0, false); // don't send change
																								// event
			} catch (IllegalArgumentException | BadDataException e) {
				log.error("Could not write layout - " + e.getLocalizedMessage());
			}

		}

	}

	public List<String> getRealms() {
		// fetch all projects from db
		String sqlCode = "SELECT distinct be.realm FROM BaseEntity be,EntityAttribute ea where ea.baseEntityCode=be.code and  be.code LIKE 'PRJ_%' and ea.attributeCode='ENV_DISABLE' and ea.valueString = 'FALSE' and be.realm <> 'genny' and be.realm <> 'hidden'";
		final List<String> realms = (List<String>) getEntityManager().createQuery(sqlCode).getResultList();

		return realms;
	}

}
