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
import java.util.stream.*;

@Configuration
@EnableBatchProcessing
@SpringBootApplication
public class BatchApp extends JobExecutionListenerSupport 
	implements ItemProcessor<FieldSet, FieldSet> {

    @Override
    public FieldSet process(FieldSet in) throws Exception {
    	ArrayList<String> names = new ArrayList<String>();
    	ArrayList<String> values = new ArrayList<String>();
    	Arrays.stream(outputFieldNames.split(","))
		.forEach(x->{
			names.add(x.trim());
			values.add(in.readString(x.trim()));
		});
		FieldSet out = new DefaultFieldSet(values.toArray(new String[0]), 
			names.toArray(new String[0]));
    	log.info(Arrays.asList(out.getValues()).stream().collect(Collectors.joining(", ")));
    	return out;
    }    

	@Override
	public void afterJob(JobExecution jobExecution) {
        log.info("Job completed!");		
	}

    String inputFile = "in.csv";
	String inputFileType = "fl"; //csv, fl, db
    String inputFieldNames = "firstName,lastName";
    String inputFieldLengths = "1-3,4-6";
    String inputDelimiter = ",";

    String outputFile = "out.txt";
	String outputFileType = "txt"; //csv, fl, db, xml, json
    String outputFieldNames = "lastName,firstName";
    String outputFormat = "|%-5s:%5s|";
    String outputDelimiter = ",";
    String outputRootTagName = "Persons";
    String outputEntryTagName = "Person";

    @Bean
    public FlatFileItemReader dbReader() {
        FlatFileItemReader reader = new FlatFileItemReader();
        reader.setResource(new FileSystemResource(inputFile));
        reader.setLineMapper(new DefaultLineMapper() {{
            setLineTokenizer(new DelimitedLineTokenizer(inputDelimiter) {{
                setNames(inputFieldNames.split(inputDelimiter));
            }});
            setFieldSetMapper(new PassThroughFieldSetMapper());
        }});
        return reader;
    }

    @Bean
    public FlatFileItemReader fixedLengthReader() {
        FlatFileItemReader reader = new FlatFileItemReader();
        RangeArrayPropertyEditor pe = new RangeArrayPropertyEditor();
        pe.setAsText(inputFieldLengths);
        reader.setResource(new FileSystemResource(inputFile));
        reader.setLineMapper(new DefaultLineMapper() {{
            setLineTokenizer(new FixedLengthTokenizer() {{
                setNames(inputFieldNames.split(inputDelimiter));
                setColumns((Range[])pe.getValue());
            }});
            setFieldSetMapper(new PassThroughFieldSetMapper());
        }});
        return reader;
    }

    @Bean
    public FlatFileItemReader csvReader() {
        FlatFileItemReader reader = new FlatFileItemReader();
        reader.setResource(new FileSystemResource(inputFile));
        reader.setLineMapper(new DefaultLineMapper() {{
            setLineTokenizer(new DelimitedLineTokenizer(inputDelimiter) {{
                setNames(inputFieldNames.split(inputDelimiter));
            }});
            setFieldSetMapper(new PassThroughFieldSetMapper());
        }});
        return reader;
    }

    @Bean
    public FlatFileItemWriter csvWriter() {
        FlatFileItemWriter writer = new FlatFileItemWriter();
        writer.setResource(new FileSystemResource(outputFile));
        writer.setLineAggregator(new DelimitedLineAggregator() {{
        	setDelimiter(outputDelimiter);
        	setFieldExtractor(new PassThroughFieldExtractor());
        }});
        return writer;
    }

    @Bean
    public FlatFileItemWriter formatWriter() {
        FlatFileItemWriter writer = new FlatFileItemWriter();
        writer.setResource(new FileSystemResource(outputFile));
        writer.setLineAggregator(new FormatterLineAggregator() {{
        	setFormat(outputFormat);
        	setFieldExtractor(new PassThroughFieldExtractor());
        }});
        return writer;
    }

    @Bean
    public StaxEventItemWriter xmlWriter() {
        StaxEventItemWriter writer = new StaxEventItemWriter();
        writer.setResource(new FileSystemResource(outputFile));
        writer.setRootTagName(outputRootTagName);
        XStreamMarshaller marshaller = new XStreamMarshaller();
        marshaller.setConverters(new XMLMapConverter());	
        Map aliases =  new HashMap();
        aliases.put(outputEntryTagName, 
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
    	if (inputFileType == "csv") reader = csvReader();
    	if (inputFileType == "fl") reader = fixedLengthReader();
    	ItemWriter writer = csvWriter();
    	if (outputFileType == "csv") writer = csvWriter();
    	if (outputFileType == "txt") writer = formatWriter();
    	if (outputFileType == "xml") writer = xmlWriter();
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