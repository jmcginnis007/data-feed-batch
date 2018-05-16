package com.example.datafeed.producer;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.batch.item.file.transform.PassThroughLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Bean
    public FlatFileItemReader<String> reader() {
        FlatFileItemReader<String> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource("stream.json"));
        reader.setLineMapper(new PassThroughLineMapper());
        return reader;
    }

    @Bean
    public PayloadItemProcessor processor() {
        return new PayloadItemProcessor();
    }

    @Bean
    public FlatFileItemWriter<String> writer() {
    	//writer will just be used for errors
    	FlatFileItemWriter<String> writer = new FlatFileItemWriter<>();
    	writer.setName("test");
        writer.setResource(new FileSystemResource("c:/temp/errors.json"));
        writer.setLineAggregator(new PassThroughLineAggregator<>());
        return writer;
    }

    @Bean
    public Job importPayloadJob(JobCompletionNotificationListener listener) {
        return jobBuilderFactory.get("importPayloadJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1())
                .end()
                .build();
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<String, String> chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }
}
