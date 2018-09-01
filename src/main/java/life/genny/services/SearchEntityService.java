package life.genny.services;

import static java.lang.System.out;
import static test.generated.Tables.BASEENTITY;
import static test.generated.Tables.BASEENTITY_ATTRIBUTE;
import static test.generated.Tables.BASEENTITY_BASEENTITY;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.Query;
import javax.sql.DataSource;
import javax.validation.constraints.NotNull;

import org.apache.logging.log4j.Logger;
import org.hibernate.Filter;
import org.hibernate.Session;
import org.hibernate.internal.SessionImpl;
import org.hibernate.tool.schema.spi.ExceptionHandler;
import org.javamoney.moneta.Money;
import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.JoinType;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.SelectQuery;
import org.jooq.Table;
import org.jooq.conf.RenderNameStyle;
import org.jooq.conf.Settings;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

import life.genny.qwanda.entity.EntityEntity;
import life.genny.qwanda.converter.JOOQMoneyConverter;
import life.genny.qwanda.Link;
import life.genny.qwanda.attribute.Attribute;
import life.genny.qwanda.attribute.EntityAttribute;
import life.genny.qwanda.entity.BaseEntity;
import test.generated.tables.Baseentity;
import test.generated.tables.BaseentityAttribute;
import test.generated.tables.BaseentityBaseentity;
import test.generated.tables.*;

public class SearchEntityService {
	private static final String DEFAULT_REALM = "genny";
	
	EntityManager em;
	
	BaseentityAttribute bea = BASEENTITY_ATTRIBUTE.as("bea");
	BaseentityBaseentity ee = BASEENTITY_BASEENTITY.as("ee");
	BaseentityBaseentity ff = BASEENTITY_BASEENTITY.as("ff");
	BaseentityBaseentity gg = BASEENTITY_BASEENTITY.as("gg");
	Baseentity be = BASEENTITY.as("be");
	
	protected static final Logger log = org.apache.logging.log4j.LogManager
			.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());
	
	List<String> allowedConditions = Arrays.asList("=", "<", ">", "<=", ">=", "LIKE", "!=", "<>", "&+", "&0");
	List<String> allowedLinkWeightConditions = Arrays.asList("=", "<", ">", "<=", ">=");

	private DataSource ds;
	
	protected String getRealm() {
		return DEFAULT_REALM;
	}
	
	public BaseEntity getUser() {
		return null;
	}
	
	protected EntityManager getEntityManager() {
		return em;
	}
	protected SearchEntityService() {
	}

	public List<BaseEntity> findBySearchBE(@NotNull final BaseEntity searchBE) {
		
		
		final DSLContext dslContext = getDSLContext();
		
		List<BaseEntity> results = null;
		
		Integer pageStart = searchBE.getValue("SCH_PAGE_START", 0);
		Integer pageSize = searchBE.getValue("SCH_PAGE_SIZE", 100);
		String linkWeightFilter = searchBE.getValue("SCH_LINK_FILTER", ">");

		// Check for bad linkWeight filtering
		if (!allowedLinkWeightConditions.stream().anyMatch(str -> str.trim().equals(linkWeightFilter))) {
			log.error("Error! Illegal link Weight condition!(" + linkWeightFilter + ") for user " + getUser());
			return results;
		}

		int filterIndex = 0;
		final HashMap<String, Field> attributeCodeMap = new HashMap<>();
		final List<Order> orderList = new ArrayList<>(); // attributeCode , ASC/DESC
		Set<String> attributeCodes = new HashSet<>();
		
		SelectQuery<Record> beSearchQuery = createSearchSQL(dslContext, searchBE);
		
		for (EntityAttribute ea : searchBE.getBaseEntityAttributes()) {
			BaseentityAttribute beaTemp = BASEENTITY_ATTRIBUTE.as("bea" + filterIndex);
			test.generated.tables.Attribute aTemp = test.generated.tables.Attribute.ATTRIBUTE.as("a" + filterIndex);
			if (ea.getAttributeCode().startsWith("SCH_")) {
				continue;
			} else if (ea.getAttributeCode().startsWith("SRT_")) {
				String sortAttributeCode = ea.getAttributeCode().substring("SRT_".length());
				if (ea.getWeight() == null) {
					ea.setWeight(1000.0); // If no weight is given setting it to be in the last of the sort
				}
				orderList.add(new Order(sortAttributeCode, ea.getValueString().toUpperCase(), ea.getWeight())); // weight

				if (!"PRI_CODE".equalsIgnoreCase(sortAttributeCode) && !"PRI_CREATED".equalsIgnoreCase(sortAttributeCode) && 
						!"PRI_UPDATED".equalsIgnoreCase(sortAttributeCode) && !"PRI_ID".equalsIgnoreCase(sortAttributeCode) && 
						!"PRI_NAME".equalsIgnoreCase(sortAttributeCode)) {

					beSearchQuery.addJoin(beaTemp, JoinType.CROSS_JOIN);
					beSearchQuery.addConditions(beaTemp.BASEENTITY_ID.eq(bea.BASEENTITY_ID));
					beSearchQuery.addJoin(aTemp, JoinType.CROSS_JOIN);
					beSearchQuery.addConditions(beaTemp.ATTRIBUTE_ID.eq(aTemp.ID));
					beSearchQuery.addConditions(aTemp.CODE.eq(sortAttributeCode));
					
					String attributeCodeEA = "bea" + filterIndex;
					if (ea.getPk() == null || ea.getPk().getAttribute() == null) {
						Attribute attribute = findAttributeByCode(sortAttributeCode);
						ea.getPk().setAttribute(attribute);
					}
					String sortType;
					sortType = getSortType(ea);
					attributeCodeMap.put(sortAttributeCode, beaTemp.field(attributeCodeEA + "." + sortType));
					filterIndex++;
				}
			} else if (ea.getAttributeCode().startsWith("COL_")) {
				String columnAttributeCode = ea.getAttributeCode().substring("COL_".length());
				if (!"code".equalsIgnoreCase(columnAttributeCode)) {
					attributeCodes.add(columnAttributeCode);
				}
			} else {
				String priAttributeCode = ea.getAttributeCode();
				String condition = ea.getAttributeName();
				if (condition != null) {
					final String conditionTest = condition.trim();
					if (!allowedConditions.stream().anyMatch(str -> str.trim().equals(conditionTest))) {
						log.error("Error! Illegal condition!(" + conditionTest + ") [" + ea.getAttributeCode()
								+ "] for user " + getUser());
						return results;
					}
				} 
				if(condition == null || condition.isEmpty()) {
					condition="=";
				}
				
				String valueString = ea.getValueString();
				if (valueString != null && valueString.contains(";")) {
					log.error("Error! Illegal condition!(" + valueString + ") [" + ea.getAttributeCode()
							+ "] for user " + getUser());
					return results;
				}
				if ("PRI_CODE".equalsIgnoreCase(priAttributeCode)) {
						addQueryCondition(beSearchQuery, be.CODE, condition, ea.getValueString());
				} else if ("PRI_CREATED".equalsIgnoreCase(priAttributeCode)) {
						addQueryCondition(beSearchQuery, be.CREATED, condition, ea.getValueDateTime());
				} else {
					beSearchQuery.addJoin(beaTemp, JoinType.CROSS_JOIN);
					beSearchQuery.addConditions(beaTemp.BASEENTITY_ID.eq(bea.BASEENTITY_ID));
					beSearchQuery.addJoin(aTemp, JoinType.CROSS_JOIN);
					beSearchQuery.addConditions(beaTemp.ATTRIBUTE_ID.eq(aTemp.ID));
					beSearchQuery.addConditions(aTemp.CODE.eq(priAttributeCode));

					if (ea.getPk() == null || ea.getPk().getAttribute() == null) {
						Attribute attribute = findAttributeByCode(priAttributeCode);
						ea.getPk().setAttribute(attribute);
					}
					switch (ea.getPk().getAttribute().getDataType().getClassName()) {
					case "java.lang.Integer":
					case "Integer":
						addQueryCondition(beSearchQuery, beaTemp.VALUEINTEGER, condition, ea.getValueInteger());
						attributeCodeMap.put(priAttributeCode, beaTemp.VALUEINTEGER);
						break;
					case "java.lang.Long":
					case "Long":
						addQueryCondition(beSearchQuery, beaTemp.VALUELONG, condition, ea.getValueLong());
						attributeCodeMap.put(priAttributeCode, beaTemp.VALUELONG);
						break;
					case "java.lang.Double":
					case "Double":
						addQueryCondition(beSearchQuery, beaTemp.VALUEDOUBLE, condition, ea.getValueDouble());
						attributeCodeMap.put(priAttributeCode, beaTemp.VALUEDOUBLE);
						break;
					case "range.LocalDate":
						addDateRangeCondition(beSearchQuery, beaTemp, ea.getValueDateRange());
						attributeCodeMap.put(priAttributeCode, beaTemp.VALUEDATE);
						break;
					case "java.lang.Boolean":
					case "Boolean":
						addQueryCondition(beSearchQuery, beaTemp.VALUEBOOLEAN, condition, ea.getValueBoolean());
						attributeCodeMap.put(priAttributeCode, beaTemp.VALUEBOOLEAN);
						break;
					case "java.time.LocalDate":
					case "LocalDate":
						addQueryCondition(beSearchQuery, beaTemp.VALUEDATE, condition, ea.getValueDate());
						attributeCodeMap.put(priAttributeCode, beaTemp.VALUEDATE);
						break;
					case "java.time.LocalDateTime":
					case "LocalDateTime":
						addQueryCondition(beSearchQuery, beaTemp.VALUEDATETIME, condition, ea.getValueDateTime());
						attributeCodeMap.put(priAttributeCode, beaTemp.VALUEDATETIME);
						break;
					case "java.time.LocalTime":
					case "LocalTime":
						addQueryCondition(beSearchQuery, beaTemp.VALUETIME, condition, ea.getValueTime());
						attributeCodeMap.put(priAttributeCode, beaTemp.VALUETIME);
						break;
					case "java.lang.String":
					case "String":
					default:
						addQueryCondition(beSearchQuery, beaTemp.VALUESTRING, condition, ea.getValueString());
						attributeCodeMap.put(priAttributeCode, beaTemp.VALUESTRING);
					}
				}
				filterIndex++;
			}
		}
		if(!orderList.isEmpty()) {
			createOrderString(beSearchQuery, bea, attributeCodeMap, orderList);
		}
		
		// Limit column attributes returned using Hibernate filter
		// If empty then return everything
		if (!attributeCodes.isEmpty()) {
			Filter filter = getEntityManager().unwrap(Session.class).enableFilter("filterAttribute");
			filter.setParameterList("attributeCodes", attributeCodes);
		}
		
		beSearchQuery.addSelect(be.ID,be.DTYPE,be.CREATED,be.NAME,be.REALM,be.UPDATED,be.CODE);
		beSearchQuery.setDistinct(true);
		
		beSearchQuery.addLimit(pageStart, pageSize);
		
		out.println("JOOQ QUERY: " + beSearchQuery.getSQL());
		try {
			
			results = beSearchQuery.fetchInto(BaseEntity.class);
			if(results != null) {
				out.println("BE SEARCH RESULT COUNT: " + results.size());
			}
			// Set simple sort index for frontend to use
			int index = 0;
			for (BaseEntity be : results) {
				be.setIndex(index++);
				DataType<Money> moneyType = SQLDataType.VARCHAR.asConvertedDataType(new JOOQMoneyConverter());
				// EntityEntity Query:
				SelectQuery<Record> eeQuery = getDSLContext().selectQuery();
				test.generated.tables.Attribute attribute = test.generated.tables.Attribute.ATTRIBUTE.as("attribute");
				eeQuery.addFrom(ee);
				eeQuery.addJoin(attribute, ee.ATTRIBUTE_ID.eq(attribute.ID));
				eeQuery.addConditions(ee.SOURCE_ID.eq(be.getId()));
				DataType<Money> type = SQLDataType.VARCHAR.asConvertedDataType(new JOOQMoneyConverter());
				eeQuery.addSelect(ee.TARGETCODE, ee.CREATED, ee.UPDATED, ee.VALUEBOOLEAN, ee.VALUEDATE, ee.VALUEDATETIME, ee.VALUEDOUBLE, ee.VALUEINTEGER,
						ee.VALUELONG, DSL.field("ee.MONEY", type), ee.VALUESTRING, ee.VALUETIME, ee.VERSION, ee.WEIGHT, ee.ATTRIBUTE_ID, ee.SOURCE_ID);
				out.println("ENTITYENTITY QUERY: " + eeQuery.getSQL());
				List<EntityEntity> entityEntityResults = eeQuery.fetchInto(EntityEntity.class);
				for(EntityEntity entityEntity : entityEntityResults) {
					//Link Query:
					SelectQuery<Record> linkQuery = getDSLContext().selectQuery();
					linkQuery.addFrom(ee);
					linkQuery.addJoin(attribute, ee.ATTRIBUTE_ID.eq(attribute.ID));
					linkQuery.addConditions(ee.SOURCE_ID.eq(be.getId()));
					linkQuery.addSelect(ee.LINK_CODE, ee.CHILDCOLOR, ee.LINKVALUE, ee.PARENTCOLOR, ee.RULE, ee.SOURCE_CODE,
							ee.TARGET_CODE, ee.LINK_WEIGHT);
					out.println("LINK QUERY: " + linkQuery.getSQL());
					Link link = linkQuery.fetchOne().into(Link.class);
					entityEntity.setLink(link);
				}
				be.setLinks(new HashSet<EntityEntity>(entityEntityResults));
				//Attribute Query:
				SelectQuery<Record> attributeQuery = getDSLContext().selectQuery();
				attributeQuery.addFrom(bea);
				attributeQuery.addJoin(attribute, bea.ATTRIBUTE_ID.eq(attribute.ID));
				attributeQuery.addConditions(bea.BASEENTITY_ID.eq(be.getId()));

				attributeQuery.addSelect(bea.ATTRIBUTECODE, bea.BASEENTITYCODE, bea.CREATED, bea.INFERRED, bea.PRIVACYFLAG,
						bea.READONLY, bea.UPDATED, bea.VALUEBOOLEAN, bea.VALUEDATE, bea.VALUEDATERANGE, bea.VALUEDATETIME,
						bea.VALUEDOUBLE, bea.VALUEINTEGER, bea.VALUELONG, DSL.field("bea.MONEY", moneyType), bea.VALUESTRING, 
						bea.VALUETIME, bea.WEIGHT, bea.ATTRIBUTE_ID, bea.BASEENTITY_ID);
				out.println("ATTRIBUTE QUERY: " + attributeQuery.getSQL());
				List<EntityAttribute> attrResults = attributeQuery.fetchInto(EntityAttribute.class);
				be.setBaseEntityAttributes(new HashSet<EntityAttribute>(attrResults));
				out.println("ATTRIBUTE SIZE: " + be.getBaseEntityAttributes().size());
			}
			
		} catch (Exception e) {
			out.println("CAN'T EXECUTE JOOQ QUERY :( " + e);
		} 
		return results;
	}
	
	private SelectQuery createSearchSQL(DSLContext dslContext, BaseEntity searchBE) {
		final String userRealmStr = getRealm();

		String stakeholderCode = searchBE.getValue("SCH_STAKEHOLDER_CODE", null);
		String sourceStakeholderCode = searchBE.getValue("SCH_SOURCE_STAKEHOLDER_CODE", null);
		String linkCode = searchBE.getValue("SCH_LINK_CODE", null);
		String linkValue = searchBE.getValue("SCH_LINK_VALUE", null);
		Double linkWeight = searchBE.getValue("SCH_LINK_WEIGHT", 0.0);
		String linkWeightFilter = searchBE.getValue("SCH_LINK_FILTER", ">");
		String sourceCode = searchBE.getValue("SCH_SOURCE_CODE", null);
		String targetCode = searchBE.getValue("SCH_TARGET_CODE", null);
		
		Set<String> realms = new HashSet<>();
		realms.add(userRealmStr);
		realms.add("genny");
		
		SelectQuery<Record> beSearchQuery = dslContext.selectQuery();
		beSearchQuery.addFrom(bea);
		beSearchQuery.addJoin(be, bea.BASEENTITY_ID.eq(be.ID));
		beSearchQuery.addConditions(be.REALM.in(realms));
		
		if (sourceCode != null || targetCode != null || linkCode != null || linkValue != null) {
			Baseentity be3 = BASEENTITY.as("be3");
			beSearchQuery.addFrom(ee);
			beSearchQuery.addJoin(be3, JoinType.CROSS_JOIN);
			beSearchQuery.addConditions(ee.SOURCE_ID.eq(be3.ID));
			if(sourceCode != null) {
				beSearchQuery.addConditions(be3.CODE.eq(sourceCode).and(ee.TARGETCODE.equal(be.CODE)));
			} 
			if(targetCode != null) {
				beSearchQuery.addConditions(ee.TARGETCODE.eq(targetCode).and(be3.CODE.equal(be.CODE)));
			}
			if(linkCode != null) {
				beSearchQuery.addConditions(ee.LINK_CODE.eq(linkCode));
			}
			if(linkValue != null) {
				beSearchQuery.addConditions(ee.LINKVALUE.eq(linkValue));
			}
			if(linkWeight > 0.0) {
				addQueryCondition(beSearchQuery, ee.LINK_WEIGHT, linkWeightFilter, linkWeight);
			}
		}
		if(stakeholderCode != null) {
			Baseentity be1 = BASEENTITY.as("be1");
			beSearchQuery.addFrom(ff);
			beSearchQuery.addJoin(be1, JoinType.CROSS_JOIN);
			beSearchQuery.addConditions(ff.SOURCE_ID.eq(be1.ID));
			beSearchQuery.addConditions(ff.TARGETCODE.eq(stakeholderCode).and(be1.CODE.equal(be.CODE))
					.or(be1.CODE.eq(stakeholderCode).and(ff.TARGETCODE.equal(be.CODE))));
		}
		if(sourceStakeholderCode != null) {
			Baseentity be2 = BASEENTITY.as("be2");
			beSearchQuery.addFrom(gg);
			beSearchQuery.addJoin(be2, JoinType.CROSS_JOIN);
			beSearchQuery.addConditions(gg.SOURCE_ID.eq(be2.ID));
			beSearchQuery.addConditions(gg.TARGETCODE.eq(sourceStakeholderCode).and(be2.CODE.equal(be.CODE))
					.or(be2.CODE.eq(sourceStakeholderCode).and(gg.TARGETCODE.equal(be.CODE))));
		}
		return beSearchQuery;
	}
	
	protected DataSource getDataSource() {
		return ds;
	}

	public DSLContext getDSLContext() {
		Settings settings = new Settings();
		settings.setExecuteLogging(true);
		settings.withRenderSchema(false);
		settings.withRenderNameStyle(RenderNameStyle.AS_IS);
		return DSL.using(getDataSource(), SQLDialect.MYSQL, settings);
	}
	
	public <T> void addQueryCondition(SelectQuery query, Field fieldToCompare, String condition, T fieldValue) {
		switch (condition) {
		//Allowed conditions : "=", "<", ">", "<=", ">=", "LIKE", "!=", "<>", "&+", "&0"
			case "=":
				query.addConditions(fieldToCompare.eq(fieldValue));
				break;
			case "<":
				query.addConditions(fieldToCompare.lt(fieldValue));
				break;
			case ">":
				query.addConditions(fieldToCompare.gt(fieldValue));
				break;
			case "<=":
				query.addConditions(fieldToCompare.le(fieldValue));
				break;
			case ">=":
				query.addConditions(fieldToCompare.ge(fieldValue));
				break;
			case "LIKE":
				String stringFieldValue = (String) fieldValue;
				query.addConditions(fieldToCompare.like(stringFieldValue));
				break;
			case "!=":
			case "<>":
				query.addConditions(fieldToCompare.ne(fieldValue));
				break;
			case "&+":
				query.addConditions(fieldToCompare.ne(0));
				break;
			case "&0":
				query.addConditions(fieldToCompare.eq(0));
				break;
		}
	}
	
	private void addDateRangeCondition(SelectQuery query, BaseentityAttribute bea, Range rangeValue) {
		//CHECK HERE...
		if (rangeValue.hasLowerBound() && rangeValue.hasUpperBound()) {
			if (BoundType.CLOSED.equals(rangeValue.lowerBoundType())) {
				addQueryCondition(query, bea.VALUEDATE, ">=", rangeValue.lowerEndpoint());
				addQueryCondition(query, bea.VALUEDATE, "<=", rangeValue.upperEndpoint());
			} else if (BoundType.CLOSED.equals(rangeValue.lowerBoundType())
					&& BoundType.OPEN.equals(rangeValue.lowerBoundType())) {
				addQueryCondition(query, bea.VALUEDATE, ">=", rangeValue.lowerEndpoint());
				addQueryCondition(query, bea.VALUEDATE, "<", rangeValue.upperEndpoint());
			} else if (BoundType.OPEN.equals(rangeValue.lowerBoundType())
					&& BoundType.CLOSED.equals(rangeValue.lowerBoundType())) {
				addQueryCondition(query, bea.VALUEDATE, ">", rangeValue.lowerEndpoint());
				addQueryCondition(query, bea.VALUEDATE, "<=", rangeValue.upperEndpoint());
			} else if (BoundType.OPEN.equals(rangeValue.lowerBoundType())) {
				addQueryCondition(query, bea.VALUEDATE, ">", rangeValue.lowerEndpoint());
				addQueryCondition(query, bea.VALUEDATE, "<", rangeValue.upperEndpoint());
			}
		} else if (rangeValue.hasLowerBound() && !rangeValue.hasUpperBound()) {
			//CHECK HERE...
			if (BoundType.CLOSED.equals(rangeValue.lowerBoundType())) {
			}
			addQueryCondition(query,bea.VALUEDATE,">=", rangeValue.lowerEndpoint());
		} else if (rangeValue.hasUpperBound() && !rangeValue.hasLowerBound()) {
			if (BoundType.CLOSED.equals(rangeValue.upperBoundType())) {
				addQueryCondition(query,bea.VALUEDATE,"<=", rangeValue.upperEndpoint());
			} else {
				addQueryCondition(query,bea.VALUEDATE,"<", rangeValue.upperEndpoint());
			}
		}
	}
	
	public void createOrderString(SelectQuery query, BaseentityAttribute bea, HashMap<String, Field> attributeCodeMap, List<Order> orderListParam) {
		Collections.sort(orderListParam, new OrderCompare());
		List<Field> orderList = new ArrayList<>();
		for (Order order : orderListParam) {
			if("ASC".equalsIgnoreCase(order.getAscdesc())) {
				orderList.add((Field) attributeCodeMap.get(order.getFieldName()).asc());
			} else {
				orderList.add((Field) attributeCodeMap.get(order.getFieldName()).desc());
			}
		}
		query.addOrderBy(orderList);
	}
	
	public Attribute findAttributeByCode(@NotNull final String code) throws NoResultException {
		final String userRealmStr = getRealm();
		return (Attribute) getEntityManager()
				.createQuery("SELECT a FROM Attribute a where a.code=:code and a.realm=:realmStr")
				.setParameter("code", code.toUpperCase()).setParameter("realmStr", userRealmStr).getSingleResult();
	}
	
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
	
	private String getRealmsStr(Set<String> realms) {
		String ret = "";
		for (String realm : realms) {
			ret += "'" + realm + "',";
		}
		return ret.substring(0, ret.length() - 1);
	}
	
	class Column implements Comparable<Column> {
		private String fieldName;
		private String fieldCode;
		private Double weight;

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
}