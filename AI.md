# Tools
* VSCode
* Claude Code (Pro plan)
* ChatGPT
* Postman (for manual testing of requests — the only tool used without AI)

# Using AI

0. Discussed with ChatGPT which options are available for building data pipelines. I chose the simplest approach based on REST/HTTP requests. I considered using MQTT for all stages as the transport, but the architecture diagram showed the MQTT broker only between the producer and ingestion, so I decided MQTT should be used in only one place. Adding additional queues based on Redis, RabbitMQ, or Kafka would mean "extending the tool zoo".

1. Asked to create MQTT and PostgreSQL services and fill in `docker-compose.yml`.
  - Succeeded.

2. Asked to create the producer with configurable parameters.
  - Partially succeeded. Polished it later.

3. Asked to create the sink service.
  - Succeeded after some clarification.

4. Debugging and polishing the sink.
  - Succeeded.

5. Asked to write a draft of the transformation service.
  - Partially succeeded. Polished it later.

6. Polished the producer. Added logic for randomisation and jitter.
  - Succeeded.

7. Added a draft of ingestion. Ingestion just logs values at this stage.
  - Succeeded.

8. Producer and ingestion were tested together. No AI step :-)

9. Improved ingestion. Added forwarding to the transformation service.
  - Succeeded.

10. Asked to polish the transformation service and simplify the internal logic.
  - Succeeded.

11. Tested the full pipeline. The happy path works. Not an AI step :-)

12. Asked to add a notebook with customisation and debugging.
  - Partially succeeded. It was interesting because I had not worked with Marimo before, so I spent some time getting it working.

So at this point we have a working pipeline for the happy path.

13. Added the mechanism for saving unsent data in the transformation service.
  - Succeeded.

14. Added a retry mechanism to the ingestion service.
  - Succeeded.

15. Added a retry mechanism to the transformation service and a mechanism for limiting the buffer size.
  - Succeeded.

16. Asked to add an update in the sink for late-arriving metrics.
  - Succeeded.

17. Brainstormed what to do when the transformation buffer size is too small and it gets stuck.
  - AI suggested using the current time for eviction. I developed the idea further and chose a combined solution: store the moment of insertion and apply the current wall-clock time only for that field.

18. Added persistence to MQTT and extended the MQTT queue.
  - Succeeded.

19. Writing `README.md`. Asked AI to write the main concepts, then made some changes and wrote the testing strategy, known issues, and improvement ideas. Fixed grammar and spelling.
  - Succeeded.

20. Final polishing of the solution.
  - Succeeded.

21. I asked for criticising the result and finding corner cases.
  - It detected some undocumented corner cases and potential issues. I asked for fixing them.

22. I asked for criticising the result a second time.
  - Fail. 5/8 suggestions and found corner cases were incorrect. 2/8 were minor. So I fixed only 1.

It seems that's time to finish this task. :-) 

# Summary

I used Claude Code for short, focused tasks within a limited context. Claude typically generates small incremental improvements that are easy to review, test, and fix, which increases the speed of iterative development. It is important to note that I prefer to stay in control of the result because I am responsible for it.

## Pros
* Generating small, focused services with limited scope and functionality worked very well.
* Using AI to explain non-trivial solutions or unfamiliar technologies (such as Marimo) was also very helpful.

## Cons
* Function and variable names — Claude tended to generate overly domain-specific names.
* Claude tends to over-complicate solutions. I had to ask for simplification or fix it manually several times.
* Almost failed in the second round of criticising.