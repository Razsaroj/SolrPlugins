package solrplugin.solrplugin;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatterPlugin extends SearchComponent{

	private static final Logger LOG = LoggerFactory.getLogger(PatterPlugin.class);
	volatile long numRequests;
	volatile long numErrors;
	volatile long totalRequestsTime;
	volatile String lastnewSearcher;
	volatile String lastOptimizerEvent;
	protected String defaultField;
	private List<String> words;
	
	  @Override
	  public void init( NamedList args )
	  {
	    super.init(args);
	    defaultField = (String) args.get("field");
	    
	    if(defaultField == null) {
	    	throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Need to specify the default for analysis" );
	    }
	    
	    words = ((NamedList)args.get("words")).getAll("word");
	    
	    if(words.isEmpty()) {
	    	throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Need to specify atleast one word in searchComponent config");
	    }
	    
	  }
	
	@Override
	public void prepare(ResponseBuilder rb) throws IOException {
		// TODO Auto-generated method stub
		LOG.info("prepare method of PatterPlugin");
		
	}

	@Override
	public void process(ResponseBuilder rb) throws IOException {
		// TODO Auto-generated method stub
		LOG.info("pocess method of PatterPlugin");
		numRequests++;
		
		SolrParams params = rb.req.getParams();
		long lstartTime = System.currentTimeMillis();
		SolrIndexSearcher searcher = rb.req.getSearcher();
		
		NamedList response = new SimpleOrderedMap();
		
		String queryField = params.get("field");
		String field= null;
		
		if(defaultField!=null) {
			field=defaultField;
			LOG.info("default field: "+ field);
		}
		
		if(queryField != null) {
			field = queryField;
			LOG.info("queryField field: ", field);
		}
		
		if(field == null) {
			LOG.error("fields aren't defiend , not performing counting");
			return;
		}
		
		DocList docs = rb.getResults().docList;
		if(docs == null || docs.size() == 0) {
		LOG.debug("NO results");
		}
		
		LOG.debug("Doing This many docs:\t" + docs.size());
		
		Set<String> fieldSet = new HashSet<String>();
		
		SchemaField keyField = rb.req.getCore().getLatestSchema().getUniqueKeyField();
		
		if(null != keyField) {
			fieldSet.add(keyField.getName());
		}
		fieldSet.add(field);
		
		
		
		DocIterator iterator = docs.iterator();
		
		for (int i=0; i< docs.size(); i++) {
			try {
				
				int docId = iterator.nextDoc();
				HashMap<String, Double> counts = new HashMap<String, Double>();
				Document doc = searcher.doc(docId, fieldSet);
				IndexableField[] muiltfield = doc.getFields(field);
				
				for(IndexableField single: muiltfield) {
					for(String string: single.stringValue().split(" ")) {
						if(words.contains(string)) {
							Double oldcount = counts.containsKey(string)?counts.get(string):0;
							counts.put(string, oldcount + 1);
						}
					}
				}
				
				String id = doc.getField(keyField.getName()).stringValue();
				
				NamedList<Double> docresults = new NamedList<Double>();
				for(String word: words) {
					docresults.add(word, counts.get(word));
				}
				response.add(id, docresults);
			}catch(IOException ex) {
				LOG.error(ex.toString());
			}
			
		}
		
		rb.rsp.add("PatterPluginComponent", response);
		totalRequestsTime += System.currentTimeMillis() - lstartTime;
				
	}

	@Override
	public String getDescription() {
		// TODO Auto-generated method stub
		return "PatterPlugin";
	}
	
	
}
