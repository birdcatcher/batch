package batch;

import javax.sql.*;

import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.launch.support.*;
import org.springframework.batch.core.listener.*;
import org.springframework.batch.item.*;
import org.springframework.batch.item.database.*;
import org.springframework.batch.item.file.*;
import org.springframework.batch.item.file.mapping.*;
import org.springframework.batch.item.file.transform.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.context.annotation.*;
import org.springframework.core.io.*;
import org.springframework.jdbc.core.*;

import com.thoughtworks.xstream.converters.*;
import com.thoughtworks.xstream.io.*;
import org.springframework.batch.item.xml.*;
import org.springframework.oxm.xstream.*;



import org.slf4j.*;

import java.util.*;

@Configuration
@EnableBatchProcessing
@SpringBootApplication
public class BatchApp extends JobExecutionListenerSupport 
	implements ItemProcessor<FieldSet, FieldSet> {

    @Override
    public FieldSet process(FieldSet in) throws Exception {
    	return in;
    }    

	@Override
	public void afterJob(JobExecution jobExecution) {
        log.info("Job completed!");		
	}

	String inputType = "csv"; //csv, fl, db
	String outputType = "xml"; //csv, fl, db, xml, json
    String fieldNames = "firstName,lastName";
    String inputPath = "in.csv";
    String outputPath = "out.xml";
    String inputDelimiter = ",";
    String outputDelimiter = ",";
    String rootTagName = "Persons";
    String entryTagName = "Person";

    @Bean
    public FlatFileItemReader dbReader() {
        FlatFileItemReader reader = new FlatFileItemReader();
        reader.setResource(new FileSystemResource(inputPath));
        reader.setLineMapper(new DefaultLineMapper() {{
            setLineTokenizer(new DelimitedLineTokenizer(inputDelimiter) {{
                setNames(fieldNames.split(inputDelimiter));
            }});
            setFieldSetMapper(new PassThroughFieldSetMapper());
        }});
        return reader;
    }

    @Bean
    public FlatFileItemReader fixedLengthReader() {
        FlatFileItemReader reader = new FlatFileItemReader();
        reader.setResource(new FileSystemResource(inputPath));
        reader.setLineMapper(new DefaultLineMapper() {{
            setLineTokenizer(new DelimitedLineTokenizer(inputDelimiter) {{
                setNames(fieldNames.split(inputDelimiter));
            }});
            setFieldSetMapper(new PassThroughFieldSetMapper());
        }});
        return reader;
    }

    @Bean
    public FlatFileItemReader csvReader() {
        FlatFileItemReader reader = new FlatFileItemReader();
        reader.setResource(new FileSystemResource(inputPath));
        reader.setLineMapper(new DefaultLineMapper() {{
            setLineTokenizer(new DelimitedLineTokenizer(inputDelimiter) {{
                setNames(fieldNames.split(inputDelimiter));
            }});
            setFieldSetMapper(new PassThroughFieldSetMapper());
        }});
        return reader;
    }

    @Bean
    public FlatFileItemWriter csvWriter() {
        FlatFileItemWriter writer = new FlatFileItemWriter();
        writer.setResource(new FileSystemResource(outputPath));
        writer.setLineAggregator(new DelimitedLineAggregator() {{
        	setDelimiter(outputDelimiter);
        	setFieldExtractor(new PassThroughFieldExtractor());
        }});
        return writer;
    }

    @Bean
    public StaxEventItemWriter xmlWriter() {
        StaxEventItemWriter writer = new StaxEventItemWriter();
        writer.setResource(new FileSystemResource(outputPath));
        writer.setRootTagName(rootTagName);
        XStreamMarshaller marshaller = new XStreamMarshaller();
        marshaller.setConverters(new XMLMapConverter());	
        Map aliases =  new HashMap();
        aliases.put(entryTagName, 
        	"org.springframework.batch.item.file.transform.DefaultFieldSet");
        marshaller.setAliases(aliases);
        writer.setMarshaller(marshaller);
        return writer;
    }

    @Bean
    public Job importUserJob(JobExecutionListener listener) {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(this)
                .flow(step1())
                .end()
                .build();
    }

    @Bean
    public Step step1() {
    	ItemReader reader = csvReader();
    	if (inputType == "csv") reader = csvReader();
    	ItemWriter writer = csvWriter();
    	if (outputType == "csv") writer = csvWriter();
    	if (outputType == "xml") writer = xmlWriter();
        return stepBuilderFactory.get("step1")
                .<FieldSet, FieldSet>chunk(1)
                .reader(reader)
                .processor(this)
                .writer(writer)
                .build();
    }

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public DataSource dataSource;

    private static final Logger log = LoggerFactory.getLogger(BatchApp.class);

    public static void main(String[] args) throws Exception {
        SpringApplication.run(BatchApp.class, args);
    }

	public class XMLMapConverter implements Converter {
	   
	    public void marshal(Object source, HierarchicalStreamWriter writer, 
	    	MarshallingContext context) {
	        Map<?, ?> map = ((FieldSet)source).getProperties();
	        if(map == null) return;
	        map.forEach((k, v) -> {
	            writer.startNode(k.toString());
	            writer.setValue(v.toString());
	            writer.endNode(); 
	        });
	    }

	    public boolean canConvert(Class type) { return true; }
	    public Object unmarshal(HierarchicalStreamReader reader, 
	    	UnmarshallingContext context) {
	        return null;
	    }
	}    
}