package life.genny.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.vavr.Tuple2;
import life.genny.services.BaseEntityService2.Column;
import life.genny.services.BaseEntityService2.Order;

public class SearchSettings {
	
	public String filterStrings = "";
	public String filterStringsQ = "";
	public String orderString = "";
	public String codeFilter = "";

	public Integer filterIndex = 0;
	public HashMap<String, String> attributeCodeMap = new HashMap<String, String>();
	public List<Tuple2<String, Object>> valueList = new ArrayList<Tuple2<String, Object>>();
	public List<Order> orderList = new ArrayList<Order>(); // attributeCode , ASC/DESC
	public List<Column> columnList = new ArrayList<Column>(); // column to be searched for and returned

	public Set<String> attributeCodes = new HashSet<String>();

	/**
	 * @return the filterStringsQ
	 */
	public String getFilterStringsQ() {
		return filterStringsQ;
	}

	/**
	 * @param filterStringsQ the filterStringsQ to set
	 */
	public void setFilterStringsQ(String filterStringsQ) {
		this.filterStringsQ = filterStringsQ;
	}

	/**
	 * @return the orderString
	 */
	public String getOrderString() {
		return orderString;
	}

	/**
	 * @param orderString the orderString to set
	 */
	public void setOrderString(String orderString) {
		this.orderString = orderString;
	}

	/**
	 * @return the codeFilter
	 */
	public String getCodeFilter() {
		return codeFilter;
	}

	/**
	 * @param codeFilter the codeFilter to set
	 */
	public void setCodeFilter(String codeFilter) {
		this.codeFilter = codeFilter;
	}

	/**
	 * @return the filterIndex
	 */
	public Integer getFilterIndex() {
		return filterIndex;
	}

	/**
	 * @param filterIndex the filterIndex to set
	 */
	public void setFilterIndex(Integer filterIndex) {
		this.filterIndex = filterIndex;
	}

	/**
	 * @return the attributeCodeMap
	 */
	public HashMap<String, String> getAttributeCodeMap() {
		return attributeCodeMap;
	}

	/**
	 * @param attributeCodeMap the attributeCodeMap to set
	 */
	public void setAttributeCodeMap(HashMap<String, String> attributeCodeMap) {
		this.attributeCodeMap = attributeCodeMap;
	}

	/**
	 * @return the valueList
	 */
	public List<Tuple2<String, Object>> getValueList() {
		return valueList;
	}

	/**
	 * @param valueList the valueList to set
	 */
	public void setValueList(List<Tuple2<String, Object>> valueList) {
		this.valueList = valueList;
	}

	/**
	 * @return the orderList
	 */
	public List<Order> getOrderList() {
		return orderList;
	}

	/**
	 * @param orderList the orderList to set
	 */
	public void setOrderList(List<Order> orderList) {
		this.orderList = orderList;
	}

	/**
	 * @return the columnList
	 */
	public List<Column> getColumnList() {
		return columnList;
	}

	/**
	 * @param columnList the columnList to set
	 */
	public void setColumnList(List<Column> columnList) {
		this.columnList = columnList;
	}

	/**
	 * @return the attributeCodes
	 */
	public Set<String> getAttributeCodes() {
		return attributeCodes;
	}

	/**
	 * @param attributeCodes the attributeCodes to set
	 */
	public void setAttributeCodes(Set<String> attributeCodes) {
		this.attributeCodes = attributeCodes;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "SearchSettings [" + (filterStrings != null ? "filterStrings=" + filterStrings + ", " : "")
				+ (filterStringsQ != null ? "filterStringsQ=" + filterStringsQ + ", " : "")
				+ (orderString != null ? "orderString=" + orderString + ", " : "")
				+ (codeFilter != null ? "codeFilter=" + codeFilter + ", " : "")
				+ (filterIndex != null ? "filterIndex=" + filterIndex + ", " : "")
				+ (attributeCodeMap != null ? "attributeCodeMap=" + attributeCodeMap + ", " : "")
				+ (valueList != null ? "valueList=" + valueList + ", " : "")
				+ (orderList != null ? "orderList=" + orderList + ", " : "")
				+ (columnList != null ? "columnList=" + columnList + ", " : "")
				+ (attributeCodes != null ? "attributeCodes=" + attributeCodes : "") + "]";
	}

	
	
}
