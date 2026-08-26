const fs = require('fs');

const content = `

## 4. Advanced System Design (Idempotency)

> **'How did you prevent double-charging users?'**
> *Answer:* 'I designed the architecture around **Idempotency**. When users checkout, the API Gateway binds their static \`userId\` to a uniquely generated request ID (\`orderId\`). By persisting this to Neon PostgreSQL using Drizzle ORM immediately via the Outbox pattern, the Gateway can instantly reject any duplicate retries if a user accidentally double-clicks the button or the network hangs.'

## 5. ATS-Optimized Resume Bullets
*Copy and paste these directly into your resume under this project. They use the Google XYZ framework (Accomplished [X], as measured by [Y], by doing [Z]) to trigger ATS keyword scanners.*

- **Eliminated distributed data-loss** and guaranteed exactly-once message delivery by engineering the **Transactional Outbox Pattern** using **Drizzle ORM** to execute localized atomic PostgreSQL commits.
- **Architected an event-driven decoupled backend** using **Apache Kafka** to asynchronously offload payment queues, preventing synchronous HTTP bottlenecks and improving checkout latency.
- **Orchestrated a fault-tolerant microservice cluster** (API Gateway, Next.js Client, Zookeeper) leveraging **Docker Compose**, injecting native environment interpolations and rigid container restart policies for maximum uptime.
`;

fs.appendFileSync('interview.md', content);
console.log('Appended successfully');
