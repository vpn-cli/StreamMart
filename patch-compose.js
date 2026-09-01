const fs = require('fs');

// Fix mechanism to 'plain' across Node services
const files = ['order-service/index.js', 'payment-service/worker.js', 'analytic-service/index.js', 'email-service/index.js'];
files.forEach(f => {
  if (fs.existsSync(f)) {
    let content = fs.readFileSync(f, 'utf8');
    content = content.replace(/mechanism: ['"]scram-sha-256['"]/g, "mechanism: 'plain'");
    fs.writeFileSync(f, content);
  }
});
console.log('Fixed SASL mechanisms.');

// Update docker-compose.yml
let composeStr = fs.readFileSync('docker-compose.yml', 'utf8');

const services = ['order-service', 'email-service', 'analytic-service'];

services.forEach(svc => {
  const targetEnv = `      - KAFKA_BROKERS=kafka:9092`;
  const replacement = `      - KAFKA_BROKERS=\${KAFKA_BROKERS:-kafka:9092}
      - KAFKA_SASL_USERNAME=\${KAFKA_SASL_USERNAME}
      - KAFKA_SASL_PASSWORD=\${KAFKA_SASL_PASSWORD}
      - NODE_EXTRA_CA_CERTS=/app/ca.pem
    volumes:
      - ./ca.pem:/app/ca.pem`;
      
  // Look for the specific service block
  const regex = new RegExp(`(  ${svc}:[\\s\\S]*?)${targetEnv}`, 'g');
  composeStr = composeStr.replace(regex, `$1${replacement}`);
});

// payment-service logic is slightly different because it has more env vars
const paymentTarget = `      - KAFKA_BROKERS=kafka:9092
    depends_on:`;
const paymentReplacement = `      - KAFKA_BROKERS=\${KAFKA_BROKERS:-kafka:9092}
      - KAFKA_SASL_USERNAME=\${KAFKA_SASL_USERNAME}
      - KAFKA_SASL_PASSWORD=\${KAFKA_SASL_PASSWORD}
      - NODE_EXTRA_CA_CERTS=/app/ca.pem
    volumes:
      - ./ca.pem:/app/ca.pem
    depends_on:`;
composeStr = composeStr.replace(paymentTarget, paymentReplacement);

// Completely remove "kafka" and "zookeeper" from docker-compose.yml to force it to use Aiven natively!
// Wait - the user might still want exactly backward compatibility to test locally without Internet!
// I will just leave them there, they don't consume much if we don't connect to them, but wait they DO consume RAM (OOM limit).
// Let's remove kafka and zookeeper blocks!

composeStr = composeStr.replace(/  zookeeper:[\s\S]*?(?=  kafka:)/, '');
composeStr = composeStr.replace(/  kafka:[\s\S]*?(?=  payment-service:)/, '');

// Strip 'depends_on: - kafka' from everything because they don't exist anymore!
composeStr = composeStr.replace(/    depends_on:\s+- kafka/g, '');


fs.writeFileSync('docker-compose.yml', composeStr);
console.log('Patched docker-compose.yml');
