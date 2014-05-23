package webreduce.data;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

public class Dataset {

	protected static final Gson gson = new Gson();
	protected static Charset utf8 = Charsets.UTF_8;

	/* the actual relation extracted, always column oriented*/
	public String[][] relation = null;
	public String title = ""; // content of a <caption> tag of the table, if it existed, else the <title> tag of the original page
	public String url = "";
	public Boolean hasHeader = null; // true if the original HTML had <hr> tags

	// metadata used to identify and locate a table in the CC corpus
	public int tableNum = -1; // index of the table in the list of tables on the original page
	public String s3Link = ""; // link into S3
	public long recordEndOffset = -1; // offsets into the CC file
	@SerializedName("recordOfffset") // the dataset sadly contains a typo ;-)
	public long recordOffset = -1;
	public String terms = null; // top-terms extracted from the source page as one whitespace separated string

	/* the following attributes are not set in the raw data, but are set by the example preprocessing
	step of the example indexer (see webreduce.tools.Indexer) */
	public String[] columnTypes;
	public String[] termSet;
	public String[] urlTermSet;
	public String[] titleTermSet;
	public String domain; // extracted from the URL using Guava's InternetDomainName class

	public Dataset() {
	}

	public int getNumCols() {
		return this.relation.length;
	}

	public String[] getAttributes() {
		String[] attrs = new String[getNumCols()];
		for (int i = 0; i < getNumCols(); i++) {
			attrs[i] = relation[i][0];
		}
		return attrs;
	}

	public String[][] getRelation() {
		return relation;
	}

	public void setRelation(String[][] relation) {
		this.relation = relation;
	}

	public String getTerms() {
		return terms;
	}

	public void setTerms(String terms) {
		this.terms = terms;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public Boolean getHasHeader() {
		return hasHeader;
	}

	public void setHasHeader(Boolean hasHeader) {
		this.hasHeader = hasHeader;
	}

	public String getS3Link() {
		return s3Link;
	}

	public void setS3Link(String s3Link) {
		this.s3Link = s3Link;
	}

	public int getTableNum() {
		return tableNum;
	}

	public void setTableNum(int tableNum) {
		this.tableNum = tableNum;
	}

	public long getRecordEndOffset() {
		return recordEndOffset;
	}

	public void setRecordEndOffset(long recordEndOffset) {
		this.recordEndOffset = recordEndOffset;
	}

	public String[] getColumnTypes() {
		return columnTypes;
	}

	public void setColumnTypes(String[] columnTypes) {
		this.columnTypes = columnTypes;
	}

	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public String[] getTermSet() {
		return termSet;
	}

	public void setTermSet(String[] termSet) {
		this.termSet = termSet;
	}

	public String[] getUrlTermSet() {
		return urlTermSet;
	}

	public void setUrlTermSet(String[] urlTermSet) {
		this.urlTermSet = urlTermSet;
	}

	public String[] getTitleTermSet() {
		return titleTermSet;
	}

	public void setTitleTermSet(String[] titleTermSet) {
		this.titleTermSet = titleTermSet;
	}

	public static Dataset fromJson(String json) {
		return gson.fromJson(json, Dataset.class);
	}

	public static Dataset fromJson(InputStream jsonIn) {
		return gson
				.fromJson(new InputStreamReader(jsonIn, utf8), Dataset.class);
	}

	public String toJson() {
		return gson.toJson(this);
	}

}
