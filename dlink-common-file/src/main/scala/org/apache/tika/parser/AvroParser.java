package org.apache.tika.parser;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.InputStream;
import java.util.Collections;
import java.util.Set;

/**
 * Apache Tika extension for Avro type
 * <p>
 * {@link <a href="https://tika.apache.org/1.23/parser_guide.html#Add_your_MIME-Type"></a>}
 * <p>
 * Dependencies:
 * {@link <a href="resources/META-INF/services/org.apache.tika.parser.Parser"></a>}
 * {@link <a href="resources/org/apache/tika/mime/custom-mimetypes.xml"></a>}
 * <p>
 * it depends maven-shade-plugin's `org.apache.maven.plugins.shade.resource.ServicesResourceTransformer`
 * to merge `resources/META-INF/services/org.apache.tika.parser.Parser` from different jars
 *
 * @author zhangqiang
 */
public class AvroParser extends AbstractParser {

    private static final Set<MediaType> SUPPORTED_TYPES = Collections.singleton(MediaType.parse("application/avro"));
    public static final String AVRO_MIME_TYPE = "application/avro";

    @Override
    public Set<MediaType> getSupportedTypes(ParseContext context) {
        return SUPPORTED_TYPES;
    }

    @Override
    public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context) throws SAXException {
        metadata.set(Metadata.CONTENT_TYPE, AVRO_MIME_TYPE);
        XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
        xhtml.startDocument();
        xhtml.endDocument();
    }
}
