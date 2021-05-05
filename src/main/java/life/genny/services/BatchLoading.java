package life.genny.services;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import java.util.Map.Entry;

import java.util.Optional;
import java.util.Set;

import javax.persistence.NoResultException;
import javax.transaction.Transactional;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import life.genny.qwanda.Ask;
import life.genny.qwanda.Question;
import life.genny.qwanda.QuestionQuestion;
import life.genny.qwanda.attribute.Attribute;
import life.genny.qwanda.attribute.AttributeLink;
import life.genny.qwanda.attribute.EntityAttribute;
import life.genny.qwanda.datatype.DataType;
import life.genny.qwanda.entity.BaseEntity;
import life.genny.qwanda.entity.EntityEntity;
import life.genny.qwanda.exception.BadDataException;
import life.genny.qwanda.message.QBaseMSGMessageTemplate;
import life.genny.qwanda.validation.Validation;
import life.genny.qwanda.validation.ValidationList;
import life.genny.qwandautils.GennySettings;
import life.genny.qwandautils.GennySheets;

/**
 * @author helios
 *
 */

public class BatchLoading {
	/**
	 * Stores logger object.
	 */
	protected static final Logger log = org.apache.logging.log4j.LogManager
			.getLogger(MethodHandles.lookup().lookupClass().getCanonicalName());

	private BaseEntityService2 service;

	public static int id = 1;

	private static boolean isSynchronise;

	private static String table;

	private String mainRealm = GennySettings.mainrealm;
	private Map project = null;

	private final String secret = System.getenv("GOOGLE_CLIENT_SECRET");
	private final String hostingSheetId = System.getenv("GOOGLE_HOSTING_SHEET_ID");
	File credentialPath = new File(System.getProperty("user.home"), ".genny/sheets.googleapis.com-java-quickstart");
	public GennySheets sheets = new GennySheets(secret, hostingSheetId, credentialPath);

	public static Map<String, Object> savedProjectData;

	public BatchLoading(BaseEntityService2 service) {
		this.service = service;
		sheets.setRealm(this.service.realmsStr);
	}

	public BatchLoading(Map project, BaseEntityService2 service) {
		this.service = service;
		this.mainRealm = (String) project.get("code");
		this.project = project;
		sheets.setRealm(this.mainRealm);
	}

	/**
	 * Upsert Validation to database
	 * 
	 * @param project
	 */
	public void validations(Map<String, Object> project) {
		if (project.get("validations") == null) {
			return;
		}
		ValidatorFactory factory = javax.validation.Validation.buildDefaultValidatorFactory();
		Validator validator = factory.getValidator();
		((HashMap<String, HashMap>) project.get("validations")).entrySet().stream().forEach(data -> {
			Map<String, Object> validations = data.getValue();
			String regex = null;

			regex = (String) validations.get("regex");
			if (regex != null) {
				regex = regex.replaceAll("^\"|\"$", "");
			}
			String code = ((String) validations.get("code")).replaceAll("^\"|\"$", "");
			;
			String name = ((String) validations.get("name")).replaceAll("^\"|\"$", "");
			;
			String recursiveStr = (String) validations.get("recursive");
			String multiAllowedStr = (String) validations.get("multi_allowed");
			String groupCodesStr = (String) validations.get("group_codes");
			Boolean recursive = getBooleanFromString(recursiveStr);
			Boolean multiAllowed = getBooleanFromString(multiAllowedStr);

			Validation val = null;

			if (code.startsWith(Validation.getDefaultCodePrefix() + "SELECT_")) {
				val = new Validation(code, name, groupCodesStr, recursive, multiAllowed);
			} else {
				val = new Validation(code, name, regex);

			}

			val.setRealm(this.mainRealm);
			log.info("[" + val.getRealm() + "] code " + code + ",name:" + name + ",val:" + val + ", grp="

					+ (groupCodesStr != null ? groupCodesStr : "X"));

			Set<ConstraintViolation<Validation>> constraints = validator.validate(val);
			for (ConstraintViolation<Validation> constraint : constraints) {
				log.error(constraint.getPropertyPath() + " " + constraint.getMessage());
			}
			if (constraints.isEmpty()) {
				service.upsert(val);
			}
		});
	}

	/**
	 * Upsert Attribute to database
	 * 
	 * @param project
	 * @param dataTypeMap
	 */
	public void attributes(Map<String, Object> project, Map<String, DataType> dataTypeMap) {
		if (project.get("attributes") == null) {
			return;
		}
		ValidatorFactory factory = javax.validation.Validation.buildDefaultValidatorFactory();
		Validator validator = factory.getValidator();

		((HashMap<String, HashMap>) project.get("attributes")).entrySet().stream().forEach(data -> {
			try {
				Map<String, Object> attributes = data.getValue();
				String code = ((String) attributes.get("code")).replaceAll("^\"|\"$", "");
				;
				String dataType = null;
				try {
					dataType = ((String) attributes.get("dataType")).replaceAll("^\"|\"$", "");
					;
				} catch (NullPointerException npe) {
					log.error("[" + this.mainRealm + "] DataType for " + code + " cannot be null");
					throw new Exception("Bad DataType given for code " + code);
				}
				String name = ((String) attributes.get("name")).replaceAll("^\"|\"$", "");
				;
				DataType dataTypeRecord = dataTypeMap.get(dataType);
				((HashMap<String, HashMap>) project.get("dataType")).get(dataType);
				String privacyStr = (String) attributes.get("privacy");
				if (privacyStr != null) {
					privacyStr = privacyStr.toUpperCase();
				}
				Boolean privacy = "TRUE".equalsIgnoreCase(privacyStr);
				if (privacy) {
					log.info("[" + this.mainRealm + "] Attribute " + code + " has default privacy");
				}
				String descriptionStr = (String) attributes.get("description");
				String helpStr = (String) attributes.get("help");
				String placeholderStr = (String) attributes.get("placeholder");
				String defaultValueStr = (String) attributes.get("defaultValue");
				Attribute attr = new Attribute(code, name, dataTypeRecord);
				attr.setDefaultPrivacyFlag(privacy);
				attr.setDescription(descriptionStr);
				attr.setHelp(helpStr);
				attr.setPlaceholder(placeholderStr);
				attr.setDefaultValue(defaultValueStr);
				attr.setRealm(mainRealm);
				Set<ConstraintViolation<Attribute>> constraints = validator.validate(attr);
				for (ConstraintViolation<Attribute> constraint : constraints) {
					log.info(constraint.getPropertyPath() + " " + constraint.getMessage());
				}
				if (constraints.isEmpty()) {
					service.upsert(attr);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}

	/**
	 * Initialized Map of DataTypes
	 * 
	 * @param project
	 * @return
	 */
	public Map<String, DataType> dataType(Map<String, Object> project) {
		if (project.get("dataType") == null) {
			return null;
		}
		final Map<String, DataType> dataTypeMap = new HashMap<>();
		((HashMap<String, HashMap>) project.get("dataType")).entrySet().stream().forEach(data -> {
			Map<String, Object> dataType = data.getValue();
			String validations = (String) dataType.get("validations");
			String code = ((String) dataType.get("code")).replaceAll("^\"|\"$", "");
			String className = ((String)dataType.get("classname")).replaceAll("^\"|\"$", "");
			String name = ((String) dataType.get("name")).replaceAll("^\"|\"$", "");
			String inputmask = (String) dataType.get("inputmask");
			String component = (String) dataType.get("component");
			final ValidationList validationList = new ValidationList();
			validationList.setValidationList(new ArrayList<Validation>());
			if (validations != null) {
				final String[] validationListStr = validations.split(",");
				for (final String validationCode : validationListStr) {
					try {
						Validation validation = service.findValidationByCode(validationCode);
						validationList.getValidationList().add(validation);
					} catch (NoResultException e) {
						log.error("Could not load Validation " + validationCode);
					}
				}
			}
			if (!dataTypeMap.containsKey(code)) {
				DataType dataTypeRecord;
				if (component == null) {
					dataTypeRecord = new DataType(name, validationList, name, inputmask);
				} else {
					dataTypeRecord = new DataType(name, validationList, name, inputmask, component);
				}
				dataTypeMap.put(code, dataTypeRecord);
			}
		});
		return dataTypeMap;
	}

	/**
	 * Upsert BaseEntity to Database
	 * 
	 * @param project
	 */
	public void baseEntitys(Map<String, Object> project) {
		if (project.get("baseEntitys") == null) {
			return;
		}
		ValidatorFactory factory = javax.validation.Validation.buildDefaultValidatorFactory();
		Validator validator = factory.getValidator();

		((HashMap<String, HashMap>) project.get("baseEntitys")).entrySet().stream().forEach(data -> {
			Map<String, Object> baseEntitys = data.getValue();
			String code = ((String) baseEntitys.get("code")).replaceAll("^\"|\"$", "");
			;
			String name = getNameFromMap(baseEntitys, "name", code);
			BaseEntity be = new BaseEntity(code, name);

			be.setRealm(mainRealm);

			Set<ConstraintViolation<BaseEntity>> constraints = validator.validate(be);
			for (ConstraintViolation<BaseEntity> constraint : constraints) {
				log.info(constraint.getPropertyPath() + " " + constraint.getMessage());
			}

			if (constraints.isEmpty()) {
				service.upsert(be);
			}
		});
	}

	private String getNameFromMap(Map<String, Object> baseEntitys, String key, String defaultString) {
		String ret = defaultString;
		if (baseEntitys.containsKey(key)) {
			if (baseEntitys.get("name") != null) {
				ret = ((String) baseEntitys.get("name")).replaceAll("^\"|\"$", "");
				;
			}
		}
		return ret;
	}

	/**
	 * Upsert BaseEntities with Attributes
	 * 
	 * @param project
	 */
	public void baseEntityAttributes(Map<String, Object> project) {
		if (project.get("attibutesEntity") == null) {
			return;
		}
		((HashMap<String, HashMap>) project.get("attibutesEntity")).entrySet().stream().forEach(data -> {
			Map<String, Object> baseEntityAttr = data.getValue();
			String attributeCode = null;
			try {
				attributeCode = ((String) baseEntityAttr.get("attributeCode")).replaceAll("^\"|\"$", "");
			} catch (Exception e2) {
				log.error("[" + this.mainRealm + "] AttributeCode not found [" + baseEntityAttr + "]");
			}
			String valueString = (String) baseEntityAttr.get("valueString");
			if (valueString != null) {
				valueString = valueString.replaceAll("^\"|\"$", "");
			}
			String baseEntityCode = null;

			try {
				baseEntityCode = ((String) baseEntityAttr.get("baseEntityCode")).replaceAll("^\"|\"$", "");
				String weight = (String) baseEntityAttr.get("weight");
				String privacyStr = (String) baseEntityAttr.get("privacy");
				Boolean privacy = "TRUE".equalsIgnoreCase(privacyStr);
				String confirmationStr= (String) baseEntityAttr.get("confirmation");
				Boolean confirmation = "TRUE".equalsIgnoreCase(confirmationStr);
				Attribute attribute = null;
				BaseEntity be = null;
				try {
					attribute = service.findAttributeByCode(attributeCode);
					if (attribute == null) {
						log.error("[" + this.mainRealm + "] BASE ENTITY CODE: " + baseEntityCode);
						log.error("[" + this.mainRealm + "] " + attributeCode + " is not in the Attribute Table!!!");
					} else {
						be = service.findBaseEntityByCode(baseEntityCode);
						Double weightField = null;
						try {
							weightField = Double.valueOf(weight);
						} catch (java.lang.NumberFormatException ee) {
							weightField = 0.0;
						}
						try {
							EntityAttribute ea = be.addAttribute(attribute, weightField, valueString);
							if (privacy || attribute.getDefaultPrivacyFlag()) {
								ea.setPrivacyFlag(true);
							}

							if (confirmation) {
								ea.setConfirmationFlag(true);
							}
						} catch (final BadDataException e) {
							e.printStackTrace();
						}
						be.setRealm(mainRealm);
						service.updateWithAttributes(be);
					}
				} catch (final NoResultException e) {
				}

			} catch (Exception e1) {
				String beCode = "BAD BE CODE";
				if (baseEntityAttr != null) {
					beCode = (String) baseEntityAttr.get("baseEntityCode");
				}
				log.error("[" + this.mainRealm + "] Error in getting baseEntityAttr  for AttributeCode " + attributeCode
						+ " and beCode=" + beCode);
			}

		});
	}

	/**
	 * Upsert EntityEntity
	 * 
	 * @param project
	 */
	public void entityEntitys(Map<String, Object> project) {
		if (project.get("basebase") == null) {
			return;
		}
		((HashMap<String, HashMap>) project.get("basebase")).entrySet().stream().forEach(data -> {
			Map<String, Object> entEnts = data.getValue();
			String linkCode = (String) entEnts.get("linkCode");
			String parentCode = (String) entEnts.get("parentCode");
			String targetCode = (String) entEnts.get("targetCode");
			String weightStr = (String) entEnts.get("weight");
			String valueString = (String) entEnts.get("valueString");
			final Double weight = Double.valueOf(weightStr);
			BaseEntity sbe = null;
			BaseEntity tbe = null;
			Attribute linkAttribute = service.findAttributeByCode(linkCode);
			try {
				sbe = service.findBaseEntityByCode(parentCode);
				tbe = service.findBaseEntityByCode(targetCode);
				if (!((sbe == null) || (tbe == null))) {
					if (isSynchronise) {
						try {
							EntityEntity ee = service.findEntityEntity(parentCode, targetCode, linkCode);
							ee.setWeight(weight);
							ee.setValueString(valueString);
							service.updateEntityEntity(ee);
						} catch (final NoResultException e) {
							EntityEntity ee = new EntityEntity(sbe, tbe, linkAttribute, weight);
							ee.setValueString(valueString);
							service.insertEntityEntity(ee);
						}
						return;
					}
					sbe.addTarget(tbe, linkAttribute, weight, valueString);
					service.updateWithAttributes(sbe);
				}
			} catch (final NoResultException e) {
				log.warn("[" + this.mainRealm + "] CODE NOT PRESENT IN LINKING: " + parentCode + " : " + targetCode
						+ " : " + linkAttribute);
			} catch (final BadDataException e) {
				e.printStackTrace();
			} catch (final NullPointerException e) {
				e.printStackTrace();
			}
		});
	}

	/**
	 * Upsert QuestionQuestion
	 * 
	 * @param project
	 */

	public void questionQuestions(Map<String, Object> project) {
		if (project.get("questionQuestions") == null) {
			return;
		}
		((HashMap<String, HashMap>) project.get("questionQuestions")).entrySet().stream().forEach(data -> {
			Map<String, Object> queQues = data.getValue();
			String parentCode = (String) queQues.get("parentCode");
			String targetCode = (String) queQues.get("targetCode");
			String weightStr = (String) queQues.get("weight");
			String mandatoryStr = (String) queQues.get("mandatory");
			String readonlyStr = (String) queQues.get("readonly");
			Boolean readonly = readonlyStr == null ? false : "TRUE".equalsIgnoreCase(readonlyStr);

			Double weight = 0.0;
			try {
				weight = Double.valueOf(weightStr);
			} catch (NumberFormatException e1) {
				weight = 0.0;
			}
			Boolean mandatory = "TRUE".equalsIgnoreCase(mandatoryStr);

			Question sbe = null;
			Question tbe = null;

			try {
				sbe = service.findQuestionByCode(parentCode);
				tbe = service.findQuestionByCode(targetCode);
				try {
					String oneshotStr = (String) queQues.get("oneshot");
					Boolean oneshot = false;
					if (oneshotStr == null) {
						// Set the oneshot to be that of the targetquestion
						oneshot = tbe.getOneshot();
					} else {
						oneshot = "TRUE".equalsIgnoreCase(oneshotStr);
					}

					QuestionQuestion qq = sbe.addChildQuestion(tbe.getCode(), weight, mandatory);
					qq.setOneshot(oneshot);
					qq.setReadonly(readonly);

					qq.setRealm(mainRealm);

					QuestionQuestion existing = null;
					try {
						existing = service.findQuestionQuestionByCode(parentCode, targetCode);
						if (existing == null) {
							qq = service.upsert(qq);
						} else {
							service.upsert(qq);
						}
					} catch (NoResultException e1) {
						qq = service.upsert(qq);
					} catch (Exception e) {
						existing.setMandatory(qq.getMandatory());
						existing.setOneshot(qq.getOneshot());
						existing.setWeight(qq.getWeight());
						existing.setReadonly(qq.getReadonly());

						existing.setRealm(mainRealm);

						qq = service.upsert(existing);
					}

				} catch (NullPointerException e) {
					log.error("[" + this.mainRealm + "] Cannot find QuestionQuestion targetCode:" + targetCode
							+ ":parentCode:" + parentCode);

				}
			} catch (final BadDataException e) {
				e.printStackTrace();
			}
		});
	}

	/**
	 * Upsert LinkAttribute to database
	 * 
	 * @param project
	 */
	public void attributeLinks(Map<String, Object> project, Map<String, DataType> dataTypeMap) {
		((HashMap<String, HashMap>) project.get("attributeLink")).entrySet().stream().forEach(data -> {
			Map<String, Object> attributeLink = data.getValue();

			String code = ((String) attributeLink.get("code")).replaceAll("^\"|\"$", "");
			;
			String dataType = null;
			AttributeLink linkAttribute = null;

			try {
				dataType = ((String) attributeLink.get("dataType")).replaceAll("^\"|\"$", "");
				;
				String name = ((String) attributeLink.get("name")).replaceAll("^\"|\"$", "");
				;
				DataType dataTypeRecord = dataTypeMap.get(dataType);
				((HashMap<String, HashMap>) project.get("dataType")).get(dataType);
				String privacyStr = (String) attributeLink.get("privacy");
				Boolean privacy = "TRUE".equalsIgnoreCase(privacyStr);

				linkAttribute = new AttributeLink(code, name);
				linkAttribute.setDefaultPrivacyFlag(privacy);
				linkAttribute.setDataType(dataTypeRecord);
				linkAttribute.setRealm(mainRealm);
				service.upsert(linkAttribute);
			} catch (Exception e) {
				String name = ((String) attributeLink.get("name")).replaceAll("^\"|\"$", "");
				;
				String privacyStr = (String) attributeLink.get("privacy");
				Boolean privacy = "TRUE".equalsIgnoreCase(privacyStr);

				linkAttribute = new AttributeLink(code, name);
				linkAttribute.setDefaultPrivacyFlag(privacy);
				linkAttribute.setRealm(mainRealm);
			}

			service.upsert(linkAttribute);

		});
	}

	/**
	 * Insert Questions to Database
	 * 
	 * @param project
	 */
	public void questions(Map<String, Object> project) {
		((HashMap<String, HashMap>) project.get("questions")).entrySet().stream().forEach(data -> {
			Map<String, Object> questions = data.getValue();
			String code = (String) questions.get("code");
			String name = (String) questions.get("name");
			String attrCode = (String) questions.get("attribute_code");
			String html = (String) questions.get("html");
			String oneshotStr = (String) questions.get("oneshot");
			String readonlyStr = (String) questions.get("readonly");
			String hiddenStr = (String) questions.get("hidden");
			String mandatoryStr = (String) questions.get("mandatory");
			String helper = (String) questions.get("helper");

			Boolean oneshot = getBooleanFromString(oneshotStr);
			Boolean readonly = getBooleanFromString(readonlyStr);
			Boolean mandatory = getBooleanFromString(mandatoryStr);
			Attribute attr;
			attr = service.findAttributeByCode(attrCode);
			if (attr != null) {
			Question q = new Question(code, name, attr);
			q.setOneshot(oneshot);
			q.setHtml(html);
			q.setReadonly(readonly);
			q.setMandatory(mandatory);
			q.setHelper(helper);

			q.setRealm(mainRealm);

			Question existing = service.findQuestionByCode(code);
			if (existing == null) {
				if (isSynchronise()) {
					Question val = service.findQuestionByCode(q.getCode(), mainRealm);
					if (val != null) {

						val.setRealm(mainRealm);

						service.updateRealm(val);
						return;
					}
				}
				service.insert(q);
			} else {
				existing.setName(name);
				existing.setHtml(html);
				existing.setHelper(helper);
				existing.setOneshot(oneshot);
				existing.setReadonly(readonly);
				existing.setMandatory(mandatory);
				service.upsert(existing);
			}
			} else {
				log.error("Cannot import Question "+code+" due to missing attribute :["+attrCode+"]");
			}
		});
	}

	/**
	 * Insert Ask to database
	 * 
	 * @param project
	 */
	public void asks(Map<String, Object> project) {
		((HashMap<String, HashMap>) project.get("ask")).entrySet().stream().forEach(data -> {
			Map<String, Object> asks = data.getValue();
			String attributeCode = (String) asks.get("attributeCode");
			String sourceCode = (String) asks.get("sourceCode");
			String expired = (String) asks.get("expired");
			String refused = (String) asks.get("refused");
			String targetCode = (String) asks.get("targetCode");
			String qCode = (String) asks.get("question_code");
			String name = (String) asks.get("name");
			String expectedId = (String) asks.get("expectedId");
			String weightStr = (String) asks.get("weight");
			String mandatoryStr = (String) asks.get("mandatory");
			String readonlyStr = (String) asks.get("readonly");
			String hiddenStr = (String) asks.get("hidden");
			final Double weight = Double.valueOf(weightStr);
			Boolean mandatory = "TRUE".equalsIgnoreCase(mandatoryStr);
			Boolean readonly = "TRUE".equalsIgnoreCase(readonlyStr);
			Boolean hidden = "TRUE".equalsIgnoreCase(hiddenStr);
			Question question = service.findQuestionByCode(qCode);
			final Ask ask = new Ask(question, sourceCode, targetCode, mandatory, weight);
			ask.setName(name);
			ask.setHidden(hidden);
			ask.setReadonly(readonly);

			ask.setRealm(mainRealm);

			service.insert(ask);
		});
	}

	/**
	 * Get the Project named on the last row inheriting or updating records from
	 * previous projects names in the Hosting Sheet
	 * 
	 * @return
	 */
	public Map<String, Object> getProject() {
		Map<String, Object> lastProject = null;
		List<Map<String, Object>> projects = getProjects();
		if (projects.size() <= 1) {
			log.info("[" + this.mainRealm + "] is single project");
			return projects.get(0);
		} else {
			for (int count = 0; count < projects.size(); count++) {
				int subsequentIndex = count + 1;
				if (subsequentIndex == projects.size()) {
					break;
				}

				if (lastProject == null) {
					lastProject = upsertProjectMapProps(projects.get(count), projects.get(subsequentIndex));
				} else {
					lastProject = upsertProjectMapProps(lastProject, projects.get(subsequentIndex));
				}
			}
		}
		return lastProject;
	}

	/**
	 * Call functions named after the classes
	 */
	public Map<String, Object> persistProject(boolean isSynchronise, String table, boolean isDelete) {
		log.info("[" + this.mainRealm + "] Persisting Project in BatchLoading");
		BatchLoading.isSynchronise = isSynchronise;
		BatchLoading.table = table;
		if (isSynchronise) {
			log.info("Table to synchronise: " + table);
			Map<String, Object> finalProject = project == null ? getProject() : project;
			if (!isDelete) {
				switch (table) {
				case "validation":
					validations(finalProject);
					savedProjectData.put("validations", finalProject.get("validations"));
					break;
				case "attribute":
					Map<String, DataType> dataTypes = dataType(finalProject);
					attributes(finalProject, dataTypes);
					savedProjectData.put("attributes", finalProject.get("attributes"));
					break;
				case "baseentity":
					baseEntitys(finalProject);
					savedProjectData.put("baseEntitys", finalProject.get("baseEntitys"));
					break;
				case "entityattribute":
					baseEntityAttributes(finalProject);
					savedProjectData.put("attibutesEntity", finalProject.get("attibutesEntity"));
					break;
				case "attributelink":
					Map<String, DataType> linkDataTypes = dataType(finalProject);
					attributeLinks(finalProject, linkDataTypes);
					savedProjectData.put("attributeLink", finalProject.get("attributeLink"));
					break;
				case "entityentity":
					entityEntitys(finalProject);
					savedProjectData.put("basebase", finalProject.get("basebase"));
					break;
				case "question":
					questions(finalProject);
					savedProjectData.put("questions", finalProject.get("questions"));
					break;
				case "questionquestion":
					questionQuestions(finalProject);
					savedProjectData.put("questionQuestions", finalProject.get("questionQuestions"));
					break;
				case "message":
					messageTemplates(finalProject);
					savedProjectData.put("messages", finalProject.get("messages"));
					break;
				default:
					log.info("Error in table name. Please check.");
				}
				log.info("########## SYNCHRONISED GOOGLE SHEET #############");
			}
			return finalProject;
		}
		Map<String, Object> lastProject = getProject();
		savedProjectData = lastProject;
		log.info("+++++++++ AbouDSDSDSDSDSDSDSDSDSDSSDSDSDt to load Questions +++++++++++++");
		validations(lastProject);
		Map<String, DataType> dataTypes = dataType(lastProject);
		attributes(lastProject, dataTypes);
		baseEntitys(lastProject);
		baseEntityAttributes(lastProject);
		attributeLinks(lastProject, dataTypes);
		entityEntitys(lastProject);
		log.info("+++++++++ About to load Questions +++++++++++++");
		questions(lastProject);
		log.info("+++++++++ About to load QuestionQuestions +++++++++++++");
		questionQuestions(lastProject);
		log.info("+++++++++ Finished loading QuestionQuestions +++++++++++++");
		asks(lastProject);
		log.info("+++++++++ About to load Message Templates +++++++++++++");
		messageTemplates(lastProject);
		log.info("########## LOADED ALL GOOGLE DOC DATA FOR REALM " + mainRealm.toUpperCase() + " #############");
		return lastProject;
	}

	/**
	 * List of Project Maps
	 * 
	 * @return
	 */
	public List<Map<String, Object>> getModules() {
		List<Map> projectsConfig = sheets.projectsImport();
		return projectsConfig.stream().map(data -> {
			String sheetID = (String) data.get("sheetID");
			String name = (String) data.get("name");
			String module = (String) data.get("module");
			Boolean disable = Boolean.valueOf((String) data.get("disable"));

			log.info("Hellooooooooooooooooooooooooooooooooooooooooooooooo" + disable);
			if (disable)
				log.info("heyheyheyhey this is project is disabled" + name);
			final List<Map<String, Object>> map = new ArrayList<>();
			System.out.printf("%-80s%s%n", "[" + this.mainRealm + "] Loading Project \033[31;1m" + name
					+ "\033[0m and module \033[31;1m" + module + "\033[0m please wait...", "\uD83D\uDE31\t");
			Map<String, Object> fields = project(sheetID);
			System.out.printf("%-80s%s%n", "[" + this.mainRealm + "] Project \033[31;1m" + name
					+ "\033[0m and module \033[31;1m" + module + "\033[0m uploaded ",
					"\uD83D\uDC4F  \uD83D\uDC4F  \uD83D\uDC4F");
			map.add(fields);
			return map;
		}).reduce((ac, acc) -> {
			ac.addAll(acc);
			return ac;
		}).get();
	}

	public List<Map<String, Object>> getProjects() {
		List<Map> projectsConfig = null;
		Integer countDown = 10;
		while (countDown > 0) {
			try {
				projectsConfig = sheets.hostingImport();
				break;
			} catch (Exception ee) { // java.util.NoSuchElementException e |
										// java.net.SocketTimeoutException ee
				log.error("[" + this.mainRealm + "] Load from Google Doc failed, trying again in 3 sec");
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e1) {

				}
				countDown--;
			}
		}
		return projectsConfig.stream().peek(d -> log.info((String) d.get("disable")))
				.filter(prj -> !Boolean.valueOf((String) prj.get("disable"))).map(data -> {
					String sheetID = (String) data.get("sheetID");
					if ("1tgefqD-33yFAn4PXlQa0UJOzFnKVK9ehT47nLxmqoXU".equals(sheetID)) {
						log.info("compliance docs");
					}
					sheets.setSheetId(sheetID);
					List<Map<String, Object>> listModuleProject = getModules();
					return listModuleProject;
				}).reduce((ac, acc) -> {
					ac.addAll(acc);
					return ac;
				}).get();
	}

	/**
	 * Import records from google sheets
	 * 
	 * @param projectType
	 * @return
	 */
	public Map<String, Object> project(final String projectType) {
		final Map<String, Object> genny = new HashMap<>();
		sheets.setSheetId(projectType);
		Integer numOfTries = 3;
		while (numOfTries > 0) {
			try {
				if (isSynchronise) {
					switch (table) {
					case "validation":
						Map<String, Map> validations = sheets.newGetVal();
						genny.put("validations", validations);
						break;
					case "attribute":
						Map<String, Map> dataTypes = sheets.newGetDType();
						genny.put("dataType", dataTypes);
						Map<String, Map> attrs = sheets.newGetAttr();
						genny.put("attributes", attrs);
						break;
					case "baseentity":
						Map<String, Map> bes = sheets.newGetBase();
						genny.put("baseEntitys", bes);
						break;
					case "entityattribute":
						Map<String, Map> attr2Bes = sheets.newGetEntAttr();
						genny.put("attibutesEntity", attr2Bes);
						break;
					case "attributelink":
						Map<String, Map> attrLink = sheets.newGetAttrLink();
						genny.put("attributeLink", attrLink);
						break;
					case "entityentity":
						Map<String, Map> bes2Bes = sheets.newGetEntEnt();
						genny.put("basebase", bes2Bes);
						break;
					case "question":
						Map<String, Map> gQuestions = sheets.newGetQtn();
						genny.put("questions", gQuestions);
						break;
					case "questionquestion":
						Map<String, Map> que2Que = sheets.newGetQueQue();
						genny.put("questionQuestions", que2Que);
						break;
					case "message":
						Map<String, Map> messages = sheets.getMessageTemplates();
						genny.put("messages", messages);
						break;
					default:
						log.info("Error in table name. Please check.");
					}
					return genny;
				}

				log.info("[" + this.mainRealm + "] validations");
				Map<String, Map> validations = sheets.newGetVal();
				genny.put("validations", validations);
				log.info("[" + this.mainRealm + "] datatypes");
				Map<String, Map> dataTypes = sheets.newGetDType();
				genny.put("dataType", dataTypes);
				log.info("[" + this.mainRealm + "] Attributes");
				Map<String, Map> attrs = sheets.newGetAttr();
				genny.put("attributes", attrs);
				log.info("[" + this.mainRealm + "] BaseEntitys");
				Map<String, Map> bes = sheets.newGetBase();
				genny.put("baseEntitys", bes);
				log.info("[" + this.mainRealm + "] EntityAttributes");
				Map<String, Map> attr2Bes = sheets.newGetEntAttr();
				genny.put("attibutesEntity", attr2Bes);
				log.info("[" + this.mainRealm + "] attr link");
				Map<String, Map> attrLink = sheets.newGetAttrLink();
				genny.put("attributeLink", attrLink);
				log.info("[" + this.mainRealm + "] EntotyEntitys");
				Map<String, Map> bes2Bes = sheets.newGetEntEnt();
				genny.put("basebase", bes2Bes);
				log.info("[" + this.mainRealm + "] Questions");
				Map<String, Map> gQuestions = sheets.newGetQtn();
				genny.put("questions", gQuestions);
				log.info("[" + this.mainRealm + "] Question Groups");
				Map<String, Map> que2Que = sheets.newGetQueQue();
				genny.put("questionQuestions", que2Que);
				log.info("[" + this.mainRealm + "] Asks");
				Map<String, Map> asks = sheets.newGetAsk();
				genny.put("ask", asks);
				log.info("[" + this.mainRealm + "] templates");
				Map<String, Map> messages = sheets.getMessageTemplates();
				genny.put("messages", messages);
				break;
			} catch (Exception e) {
				log.error("[" + this.mainRealm
						+ "] Failed to download Google Docs Configuration ... , will retry , trys left=" + numOfTries);
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e1) {
					log.error("[" + this.mainRealm + "] sleep exception..");
				} // sleep for 10 secs
			}

			numOfTries--;
		}

		if (numOfTries <= 0) {
			log.error("[" + this.mainRealm + "] Failed to download Google Docs Configuration ... given up ...");
		}

		return genny;
	}

	/**
	 * Override or update fields with non-null fields from Subprojects to
	 * Superprojects
	 * 
	 * @param superProject
	 * @param subProject
	 * @return
	 */
	static int count = 0;

	@SuppressWarnings({ "unchecked", "unused" })
	public Map<String, Object> upsertProjectMapProps(Map<String, Object> superProject, Map<String, Object> subProject) {
		superProject.entrySet().stream().forEach(map -> {
			if (subProject.get(map.getKey()) == null && superProject.get(map.getKey()) != null) {
				subProject.put(map.getKey(), superProject.get(map.getKey()));
			}
		});
		subProject.entrySet().stream().forEach(map -> {
			if (superProject.get(map.getKey()) == null && subProject.get(map.getKey()) != null) {
				superProject.put(map.getKey(), subProject.get(map.getKey()));
			}
		});
		subProject.entrySet().stream().forEach(map -> {
			final Map<String, Object> objects = (Map<String, Object>) subProject.get(map.getKey());
			if (objects != null) {
				objects.entrySet().stream().forEach(obj -> {
					if (((Map<String, Object>) superProject.get(map.getKey()))
							.<HashMap<String, Object>>get(obj.getKey()) != null) {
						Map<String, Object> mapp = (Map<String, Object>) obj.getValue();
						Map<String, Object> mapp2 = ((Map<String, HashMap>) superProject.get(map.getKey()))
								.<HashMap<String, Object>>get(obj.getKey());
						mapp.entrySet().stream().forEach(data -> {
							if (data.getValue() != null) {
								mapp2.put(data.getKey(), data.getValue());
							}
						});
					} else {
						((Map<String, Object>) superProject.get(map.getKey()))
								.<HashMap<String, Object>>put(obj.getKey(), obj.getValue());
					}
				});
			}
		});
		return superProject;
	}

	public void messageTemplates(Map<String, Object> project) {

		if (project.get("messages") == null) {
			log.info("[" + this.mainRealm + "] project.get(messages) is null");
			return;
		}

		((HashMap<String, HashMap>) project.get("messages")).entrySet().stream().forEach(data -> {

			log.info("[" + this.mainRealm + "] messages, data ::" + data);
			Map<String, Object> template = data.getValue();
			String code = (String) template.get("code");
			String name = (String) template.get("name");
			String description = (String) template.get("description");
			String subject = (String) template.get("subject");
			String emailTemplateDocId = (String) template.get("email");
			String smsTemplate = (String) template.get("sms");
			String toastTemplate = (String) template.get("toast");
			if (StringUtils.isBlank(toastTemplate)) {
				toastTemplate = "-";
				log.error("[" + this.mainRealm + "] toastTemplate is empty! for " + code);
			}
			if (StringUtils.isBlank(smsTemplate)) {
				smsTemplate = "-";
				log.error("[" + this.mainRealm + "] smsTemplate is empty! for " + code);
			}

			final QBaseMSGMessageTemplate templateObj = new QBaseMSGMessageTemplate();
			templateObj.setCode(code);
			templateObj.setName(name);
			templateObj.setCreated(LocalDateTime.now());
			templateObj.setDescription(description);
			templateObj.setEmail_templateId(emailTemplateDocId);
			templateObj.setSms_template(smsTemplate);
			templateObj.setSubject(subject);
			templateObj.setToast_template(toastTemplate);

			if (StringUtils.isBlank(name)) {
				log.error("[" + this.mainRealm + "] Empty Name");
			} else {
				try {
					QBaseMSGMessageTemplate msg = service.findTemplateByCode(code);
					try {
						if (msg != null) {
							msg.setName(name);
							msg.setDescription(description);
							msg.setEmail_templateId(emailTemplateDocId);
							msg.setSms_template(smsTemplate);
							msg.setSubject(subject);
							msg.setToast_template(toastTemplate);
							Long id = service.update(msg);
							log.info("[" + this.mainRealm + "] updated message id ::" + id);
						} else {
							Long id = service.insert(templateObj);
							log.info("[" + this.mainRealm + "] message id ::" + id);
						}

					} catch (Exception e) {
						log.error("[" + this.mainRealm + "] Cannot update QDataMSGMessage " + code);
					}
				} catch (NoResultException e1) {
					try {
						if (BatchLoading.isSynchronise()) {
							QBaseMSGMessageTemplate val = service.findTemplateByCode(templateObj.getCode(), "hidden");
							if (val != null) {
								val.setRealm("genny");
								service.updateRealm(val);
								return;
							}
						}
						Long id = service.insert(templateObj);
						log.info("[" + this.mainRealm + "] message id ::" + id);
					} catch (javax.validation.ConstraintViolationException ce) {
						log.error("[" + this.mainRealm + "] Error in saving message due to constraint issue:"
								+ templateObj + " :" + ce.getLocalizedMessage());
						log.info("[" + this.mainRealm + "] Trying to update realm from hidden to genny");
						templateObj.setRealm("genny");
						service.updateRealm(templateObj);
					}

				} catch (Exception e) {
					log.error("[" + this.mainRealm + "] Cannot add MessageTemplate");

				}
			}
		});
	}

	private Boolean getBooleanFromString(final String booleanString) {
		if (booleanString == null) {
			return false;
		}

		if ("TRUE".equalsIgnoreCase(booleanString.toUpperCase()) || "YES".equalsIgnoreCase(booleanString.toUpperCase())
				|| "T".equalsIgnoreCase(booleanString.toUpperCase())
				|| "Y".equalsIgnoreCase(booleanString.toUpperCase()) || "1".equalsIgnoreCase(booleanString)) {
			return true;
		}
		return false;

	}

	public static boolean isSynchronise() {
		return isSynchronise;
	}

	public String constructKeycloakJson() {
		final String PROJECT_CODE = "PRJ_" + this.mainRealm.toUpperCase();
		final String CURRENT_ENV = System.getenv("CURRENT_ENV");
		String keycloakUrl = null;
		String keycloakSecret = null;
		String keycloakJson = null;

		if (savedProjectData != null && savedProjectData.get("attibutesEntity") != null) {
			final Iterator iter = ((HashMap<String, HashMap>) savedProjectData.get("attibutesEntity")).entrySet()
					.iterator();

			while (iter.hasNext()) {
				final Entry entry = (Entry) iter.next();
				final String key = (String) entry.getKey();
				if ((PROJECT_CODE + "ENV_KEYCLOAK_AUTHURL_" + CURRENT_ENV).equalsIgnoreCase(key)) {
					HashMap<String, Object> baseEntityAttr = (HashMap<String, Object>) entry.getValue();
					keycloakUrl = baseEntityAttr.get("valueString").toString();
				} else if ((PROJECT_CODE + "ENV_KEYCLOAK_SECRET_" + CURRENT_ENV).equalsIgnoreCase(key)) {
					HashMap<String, Object> baseEntityAttr = (HashMap<String, Object>) entry.getValue();
					keycloakSecret = baseEntityAttr.get("valueString").toString();
				}
			}

			if (project != null) {
				keycloakUrl = (String) project.get("keycloakUrl");
				keycloakSecret = (String) project.get("clientSecret");
			}

			keycloakJson = "{\n" + "  \"realm\": \"" + this.mainRealm + "\",\n" + "  \"auth-server-url\": \""
					+ keycloakUrl + "\",\n" + "  \"ssl-required\": \"none\",\n" + "  \"resource\": \"" + this.mainRealm
					+ "\",\n" + "  \"credentials\": {\n" + "    \"secret\": \"" + keycloakSecret + "\" \n" + "  },\n"
					+ "  \"policy-enforcer\": {}\n" + "}";

			log.info("[" + this.mainRealm + "] Loaded keycloak.json... " + keycloakJson);
			return keycloakJson;

		} else {
			log.warn(
					"Could not construct keycloak json as savedProjectData is null due to google doc loading might be skipped...");
			BaseEntity project = service.findBaseEntityByCode(PROJECT_CODE);
			if (project == null) {
				log.error("[" + this.mainRealm + "] Error: no Project Setting for " + PROJECT_CODE);
				return null;
			}
			Optional<EntityAttribute> entityAttribute1 = project.findEntityAttribute("ENV_KEYCLOAK_JSON");
			if (entityAttribute1.isPresent()) {

				keycloakJson = entityAttribute1.get().getValueString();
				return keycloakJson;

			} else {
				log.error("[" + this.mainRealm + "] Error: no Project Setting for ENV_KEYCLOAK_JSON ensure PRJ_"
						+ this.mainRealm.toUpperCase() + " has entityAttribute value for ENV_KEYCLOAK_JSON");
				return null;
			}
		}
	}

	public String constructKeycloakJson(final Map project) {
		final String PROJECT_CODE = "PRJ_" + this.mainRealm.toUpperCase();
		String keycloakUrl = null;
		String keycloakSecret = null;
		String keycloakJson = null;

		if (project != null) {
			keycloakUrl = (String) project.get("keycloakUrl");
			keycloakSecret = (String) project.get("clientSecret");
		}

		keycloakJson = "{\n" + "  \"realm\": \"" + this.mainRealm + "\",\n" + "  \"auth-server-url\": \"" + keycloakUrl
				+ "/auth\",\n" + "  \"ssl-required\": \"external\",\n" + "  \"resource\": \"" + this.mainRealm + "\",\n"
				+ "  \"credentials\": {\n" + "    \"secret\": \"" + keycloakSecret + "\" \n" + "  },\n"
				+ "  \"policy-enforcer\": {}\n" + "}";

		log.info("[" + this.mainRealm + "] Loaded keycloak.json... " + keycloakJson);
		return keycloakJson;

	}

	public void upsertKeycloakJson(String keycloakJson) {
		final String PROJECT_CODE = "PRJ_" + this.mainRealm.toUpperCase();
		BaseEntity be = service.findBaseEntityByCode(PROJECT_CODE);

		ValidatorFactory factory = javax.validation.Validation.buildDefaultValidatorFactory();
		Validator validator = factory.getValidator();
		Attribute attr = service.findAttributeByCode("ENV_KEYCLOAK_JSON");
		if (attr == null) {
			attr = new Attribute("ENV_KEYCLOAK_JSON", "Keycloak Json", new DataType("DTT_TEXT"));
			attr.setRealm(mainRealm);
			Set<ConstraintViolation<Attribute>> constraints = validator.validate(attr);
			for (ConstraintViolation<Attribute> constraint : constraints) {
				log.info("[" + this.mainRealm + "] " + constraint.getPropertyPath() + " " + constraint.getMessage());
			}
			service.upsert(attr);
		}
		try {
			EntityAttribute ea = be.addAttribute(attr, 0.0, keycloakJson);
		} catch (BadDataException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		service.updateWithAttributes(be);

	}

	public void upsertProjectUrls(String urlList) {
		final String PROJECT_CODE = "PRJ_" + this.mainRealm.toUpperCase();
		BaseEntity be = service.findBaseEntityByCode(PROJECT_CODE);

		ValidatorFactory factory = javax.validation.Validation.buildDefaultValidatorFactory();
		Validator validator = factory.getValidator();
		Attribute attr = service.findAttributeByCode("ENV_URL_LIST");
		attr.setRealm(mainRealm);
		if (attr == null) {
			attr = new Attribute("ENV_URL_LIST", "Url List", new DataType("DTT_TEXT"));
			Set<ConstraintViolation<Attribute>> constraints = validator.validate(attr);
			for (ConstraintViolation<Attribute> constraint : constraints) {
				log.info("[" + this.mainRealm + "]" + constraint.getPropertyPath() + " " + constraint.getMessage());
			}
			service.upsert(attr);
		}
		try {
			EntityAttribute ea = be.addAttribute(attr, 0.0, urlList);
		} catch (BadDataException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		service.updateWithAttributes(be);

	}

}
