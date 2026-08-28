package com.devansh.messagequeue;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class MessagequeueApplication {

	public static void main(String[] args) {
		SpringApplication.run(MessagequeueApplication.class, args);
	}

}
