package net.sf.jsi;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import net.sf.jsi.rtree.RTree;
import gnu.trove.procedure.TIntProcedure;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


//by Lei Jiang (lei.jiang@tapjoy.com)

public class ShpTxtReadTest {

	private static final Logger log = LoggerFactory.getLogger(ShpTxtReadTest.class);
	
	private ArrayList<SpatialIndex> msi = null;
	private HashMap<String, Integer> statemap = null; 
	private ArrayList<ArrayList<Rectangle>> mrect = null;
	
	private ArrayList<Point> testpoints = null;
	private ArrayList<String> teststates = null;
	private ArrayList<String> testnames = null;
	
	abstract class Operation {
	    private final int count[] = new int[1];
	    private String description;

	    public Operation(String description) {
	      this.description = description;
	    }

	    protected TIntProcedure countProc = new TIntProcedure() {
	      public boolean execute(int value) {
	        count[0]++;
	        return true;
	      }
	    };

	    public int callbackCount() {
	      return count[0];
	    }

	    public String getDescription() {
	      return description;
	    }

	    abstract void execute(ArrayList<SpatialIndex> si, ArrayList<ArrayList<Rectangle>> rect, HashMap<String, Integer> m, Rectangle qrect, String state);
	  }

    private void benchmark(Operation o, int testcount, int repetitions) {
	    long duration = 0;
	    long startTime = System.nanoTime();
	    
	    Point qp = testpoints.get(testcount);
	    float qx = qp.x;
	    float qy = qp.y;
	    String qstate = teststates.get(testcount);
	    String qname = testnames.get(testcount);
	    
	    if(!statemap.containsKey(qstate))
	    {
	    	log.info("query state "+qstate+" not indexed. failed to query...");
	    	return;
	    }
	    
	    Rectangle qrect = new Rectangle();
	    qrect.minX = qx;
	    qrect.maxX = qx;
	    qrect.minY = qy;
	    qrect.maxY = qy;
	    
	    for (int j = 0; j < repetitions; j++) o.execute(msi, mrect, statemap, qrect, qstate);
	    duration += (System.nanoTime() - startTime);

	    log.info(qname+","+qstate+ " : "+o.getDescription() + ", " +
	            "avg callbacks = " + ((float) o.callbackCount() / repetitions) + ", " +
	            "avg time = " + (duration / repetitions) + " ns");
	  }
    
    //@Test
    public void testReadRect()
    {
    	readRectFromShpTxt("/Users/ljiang/Projects/misc/income_shp2txt/de-income-geo2.txt");
    }
    
    public SpatialIndex buildTreeFromRect(ArrayList<Rectangle> inrect)
    {
    	SpatialIndex ss = new RTree();
    	Properties p = new Properties();
    	p.setProperty("MinNodeEntries", "400");
    	p.setProperty("MaxNodeEntries", "25000");
    	   
    	ss.init(p);
    	
    	int ind = 1;
    	for(Rectangle rr: inrect)
    	{
    		ss.add(rr, ind);
    		ind++;
    	} 	
    	return ss;
    }
    
    public ArrayList<Rectangle> readRectFromShpTxt(String filename)
    {
    	ArrayList<Rectangle> res = new ArrayList<Rectangle>();
    	FileReader fr = null;
    	BufferedReader inputStream = null;
    	try {
			fr = new FileReader(filename);
			inputStream = new BufferedReader(fr);
			
			String line = null;
			while((line = inputStream.readLine()) != null)
			{
				String[] sp = line.split("\t");
				if(sp.length < 3)
				{
					log.error("Illegal line: fewer than 3 elements separated by tab !!!\n"+line);
				}
				String[] sp2 = sp[2].split("#");
				Rectangle rec = new Rectangle();
				if(sp2.length < 4)
				{
					log.error("Illegal line: fewer than 4 elements separated by tab !!!\n" + line);
				}
				rec.minX = Float.parseFloat(sp2[0]);
				rec.minY = Float.parseFloat(sp2[1]);
				rec.maxX = Float.parseFloat(sp2[2]);
				rec.maxY = Float.parseFloat(sp2[3]);
				res.add(rec);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	finally
    	{
    		if(inputStream != null)
    		{
    			try {
					inputStream.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		}
    		if(fr != null)
    		{
    			try {
					fr.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		}
    	}   
    	log.info(res.size()+ " rectangles read from "+filename);
    	return res;
    }
    
    public void buildTreeByState(String dirpath)
    {
    	File folder = new File(dirpath);
    	if(!folder.isDirectory())
    	{
    		log.error("Input is not a directory: "+dirpath);
    		return;
    	}
    	
    	msi = new ArrayList<SpatialIndex>();
    	statemap = new HashMap<String, Integer>();
    	mrect = new ArrayList<ArrayList<Rectangle>>();
    	File[] files = folder.listFiles();
    	
    	int ind = 0;
    	for(File elem: files)
    	{
    		String tmpname = elem.getName();
    		String tmpfilename = dirpath+"/"+tmpname;
    		ArrayList<Rectangle> tmprect = readRectFromShpTxt(tmpfilename);
    		mrect.add(tmprect);
    		msi.add(buildTreeFromRect(tmprect));
    		statemap.put(tmpname.substring(0,2).toUpperCase(), ind);
    		ind++;
    	}	
    }
    
    public void readTestFile(String filename)
    {
    	testpoints = new ArrayList<Point>();
    	teststates = new ArrayList<String>();
    	testnames = new ArrayList<String>();
    	
    	FileReader fr = null;
    	BufferedReader inputStream = null;
    	try {
			fr = new FileReader(filename);
			inputStream = new BufferedReader(fr);
			
			String line = inputStream.readLine();  //skip first line
			while((line = inputStream.readLine()) != null)
			{
				line = line.trim();
				String[] sp = line.split(" ");
				ArrayList<String> shrunk = new ArrayList<String>();
				for(String elem: sp)
				{
					elem = elem.trim();
					if(elem.length() > 0)
						shrunk.add(elem);
				}
				if(shrunk.size() != 4)
					continue;
				float tmpx = -Float.parseFloat(shrunk.get(2));
				float tmpy = Float.parseFloat(shrunk.get(1));
				String tmpst = shrunk.get(3).split(",")[1].trim();
				String tmpname = shrunk.get(0).trim();
				
				if(statemap.containsKey(tmpst))
				{
					testpoints.add(new Point(tmpx, tmpy));
					teststates.add(tmpst);
					testnames.add(tmpname);
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	finally
    	{
    		if(inputStream != null)
    		{
    			try {
					inputStream.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		}
    		if(fr != null)
    		{
    			try {
					fr.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		}
    	}   
    }
    
    @Test
	public void readShpTxtTest()
	{
    	buildTreeByState("/Users/ljiang/Projects/misc/income_shp2txt/");
    	
    	final Operation linearop = new Operation("linear_query")
    	{
    		void execute(ArrayList<SpatialIndex> si, ArrayList<ArrayList<Rectangle>> rect, HashMap<String, Integer> m, Rectangle qrect, String qstate)
    		{
    			ArrayList<Integer> containids = new ArrayList<Integer>();
    			int ind = m.get(qstate);
    			int count = 0;
    			for(Rectangle rr: rect.get(ind))
    			{
    				if(rr.contains(qrect))
    				{
    					if(!countProc.execute(count))
    						return;
    				}
    				count++;				
    			}
    		}
    	};
    	
    	final Operation treeop = new Operation("r_tree_query")
    	{
    		void execute(ArrayList<SpatialIndex> si, ArrayList<ArrayList<Rectangle>> rect, HashMap<String, Integer> m, Rectangle qrect, String qstate)
    		{
    			int ind = m.get(qstate);
				si.get(ind).contains(qrect, countProc);
    		}
    	};
    	
    	readTestFile("/Users/ljiang/Projects/misc/uscitieslatlng.txt");
    	ExecutorService exec = Executors.newFixedThreadPool(1);
    	try
    	{
    		log.info("total #cities for testing: "+new Integer(testnames.size()).toString());
    	    for (int i = 0; i < 100; i++) {
    	    		exec.submit(new Runnable() {
    	    			
    	    			Random rr = new Random();
    	    			int testcount = rr.nextInt() % 145;
    	    			
    	    			public void run() {
    	    				benchmark(linearop, testcount, 1000);
    	    				benchmark(treeop, testcount, 1000);
    	    			}
    	    		});
    	    		//exec.submit(new Runnable() {
    	    		//	public void run() {
    	    		//		benchmark(treeop, 1000);
    	    		//		}
    	    		//	});
    	    		//testcount = ran.nextInt() % testnames.size(); 
    	    }
    	    try { exec.awaitTermination(1, TimeUnit.MINUTES); } catch (Exception e) {}
    	 }
    	finally
    	{
    		exec.shutdownNow();
    	}
	}
}
