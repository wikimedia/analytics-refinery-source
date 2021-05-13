package org.wikimedia.analytics.refinery.hive;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

import com.google.common.base.Joiner;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.lt.LithuanianAnalyzer;
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StemmerUDF extends GenericUDF {
    private Converter[] converters = new Converter[2];
    private PrimitiveCategory[] inputTypes = new PrimitiveCategory[2];

    private final transient Map<String, Analyzer> analyzersCache = new HashMap<String, Analyzer>();

    private static final String EMPTY = "";

    private transient Analyzer defaultAnalyzer;

    /**
     * The initialize method is called only once during the lifetime of the UDF.
     * Method checks for the validity (number, type, etc) of the arguments being
     * passed to the UDF and preinstantiates all analyzers and stores them on a
     * local map for easy retrieval
     *
     * It also sets the return type of the result of the UDF, in this case
     * ObjectInspector equivalent of string
     *
     * @param arguments
     *
     * @return ObjectInspector Map<String,String>
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        checkArgsSize(arguments, 1, 2);

        for (int i = 0; i < arguments.length; i++) {
            checkArgPrimitive(arguments, i);
            checkArgGroups(arguments, i, inputTypes, STRING_GROUP);
            obtainStringConverter(arguments, i, inputTypes, converters);
        }

        initAnalyzersCache();

        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }
     /**
     * Spark and Hive act differently when initializing UDFs
     *
     * Spark doesn't look to be calling initialize when it should
     * we are forced to include this additional method so UDF
     * is initialized as expected.
     *
     * @param context
     **/
    @Override
    public void configure(MapredContext context) {
        initAnalyzersCache();
    }

    /**
     * We assume this method is not called in a multi threaded context
     * as it is the case if it is called through initialize or configure
     */
    public void initAnalyzersCache() {

        if (!this.analyzersCache.isEmpty()) {
            // map is already initialized
            return;
        }
        // pre-generate analyzers per language
        this.defaultAnalyzer = new StandardAnalyzer();

        analyzersCache.put(EMPTY, this.defaultAnalyzer);
        analyzersCache.put(null, this.defaultAnalyzer);
        analyzersCache.put("ar", new ArabicAnalyzer());
        analyzersCache.put("bg", new BulgarianAnalyzer());
        analyzersCache.put("br", new BrazilianAnalyzer());
        analyzersCache.put("ca", new CatalanAnalyzer());
        // wikis use cs for Czech, Lucene uses cz;
        // support both since cz is not used for anything else
        analyzersCache.put("cs", new CzechAnalyzer());
        analyzersCache.put("cz", new CzechAnalyzer());
        analyzersCache.put("da", new DanishAnalyzer());
        analyzersCache.put("de", new GermanAnalyzer());
        analyzersCache.put("el", new GreekAnalyzer());
        analyzersCache.put("en", new EnglishAnalyzer());
        analyzersCache.put("es", new SpanishAnalyzer());
        analyzersCache.put("eu", new BasqueAnalyzer());
        analyzersCache.put("fa", new PersianAnalyzer());
        analyzersCache.put("fi", new FinnishAnalyzer());
        analyzersCache.put("fr", new FrenchAnalyzer());
        analyzersCache.put("ga", new IrishAnalyzer());
        analyzersCache.put("gl", new GalicianAnalyzer());
        analyzersCache.put("hi", new HindiAnalyzer());
        analyzersCache.put("hu", new HungarianAnalyzer());
        analyzersCache.put("hy", new ArmenianAnalyzer());
        analyzersCache.put("id", new IndonesianAnalyzer());
        analyzersCache.put("it", new ItalianAnalyzer());
        analyzersCache.put("lt", new LithuanianAnalyzer());
        analyzersCache.put("lv", new LatvianAnalyzer());
        analyzersCache.put("nl", new DutchAnalyzer());
        analyzersCache.put("no", new NorwegianAnalyzer());
        analyzersCache.put("pt", new PortugueseAnalyzer());
        analyzersCache.put("ro", new RomanianAnalyzer());
        analyzersCache.put("ru", new RussianAnalyzer());
        analyzersCache.put("sv", new SwedishAnalyzer());
        analyzersCache.put("th", new ThaiAnalyzer());
        analyzersCache.put("tr", new TurkishAnalyzer());
    }

    /**
     * Takes the actual arguments and returns the result.
     *
     * The input is accessed using the ObjectInspectors that were saved into
     * global variables in the call to initialize() While only one instance of
     * this class exists per jvm this method is called once for every row of
     * data being processed. UDFs are called during the map phase of the
     * MapReduce job. This means that we have no control over the order in which
     * the records get sent to the UDF.
     *
     * @param arguments
     *
     * @return
     * @throws HiveException
     */
    @Override
    public String evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments.length > 0) {
            String text = getStringValue(arguments, 0, converters);
            if (text == null) {
                return EMPTY;
            }

            String lang = "en";
            if (arguments.length > 1) {
                lang = getStringValue(arguments, 1, converters);
            }

            // happy case
            List<String> words = new ArrayList<>();

            Analyzer analyzer = analyzersCache.get(lang);

            // unsupported languages get default choice
            if (analyzer == null) {
                analyzer = defaultAnalyzer;
            }

            try (TokenStream ts = analyzer.tokenStream(EMPTY, new StringReader(text));) {
                ts.reset();
                CharTermAttribute cattr = ts.getAttribute(CharTermAttribute.class);
                while (ts.incrementToken()) {
                    words.add(cattr.toString());
                }
            } catch (IOException e) {
                return text;
            }

            return Joiner.on(" ").join(words);

        }

        return EMPTY;
    }

    @Override
    public String getDisplayString(String[] arguments) {
        return "Stemmer(" + arguments[0] + ")";
    }
}
