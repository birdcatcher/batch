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

import org.slf4j.*;

import java.util.*;

@Configuration
@EnableBatchProcessing
@SpringBootApplication
public class BatchApp extends JobExecutionListenerSupport 
	implements ItemProcessor<Person, Person> {

    @Override
    public Person process(Person p) throws Exception {
        Person transformedPerson = new Person(
        	p.getFirstName().toUpperCase(), p.getLastName().toUpperCase());
        log.info("Converted Person: " + transformedPerson);
        return transformedPerson;
    }    

    @Bean
    public FlatFileItemReader<Person> reader() {
        FlatFileItemReader<Person> reader = new FlatFileItemReader<Person>();
        reader.setResource(new FileSystemResource("in.csv"));
        reader.setLineMapper(new DefaultLineMapper<Person>() {{
            setLineTokenizer(new DelimitedLineTokenizer(",") {{
                setNames(new String[] { "firstName", "lastName" });
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                setTargetType(Person.class);
            }});
        }});
        return reader;
    }

    @Bean
    public FlatFileItemWriter<Person> writer() {
        FlatFileItemWriter<Person> writer = new FlatFileItemWriter<Person>();
        writer.setResource(new FileSystemResource("out.csv"));
        writer.setLineAggregator(new DelimitedLineAggregator<Person>() {{
        	setDelimiter(",");
        	setFieldExtractor(new BeanWrapperFieldExtractor<Person>() {{
        		setNames(new String[] {"lastName", "firstName"});
        	}});
        }});
        return writer;
    }

    @Bean
    public org.springframework.batch.item.xml.StaxEventItemWriter<Person> xmlWriter() {
        org.springframework.batch.item.xml.StaxEventItemWriter<Person> writer = 
        	new org.springframework.batch.item.xml.StaxEventItemWriter<Person>();
        writer.setResource(new FileSystemResource("out.xml"));
        writer.setRootTagName("Persons");
        org.springframework.oxm.xstream.XStreamMarshaller marshaller = 
        	new org.springframework.oxm.xstream.XStreamMarshaller();
        Map aliases =  new HashMap();
        aliases.put("Person", "batch.Person");
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
        return stepBuilderFactory.get("step1")
                .<Person, Person>chunk(10)
                .reader(reader())
                .processor(this)
                .writer(xmlWriter())
                .build();
    }

	@Override
	public void afterJob(JobExecution jobExecution) {
        log.info("Job completed!");		
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
}