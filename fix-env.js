const fs = require('fs');
fs.writeFileSync('.env', `DATABASE_URL=postgresql://neondb_owner:npg_WZyLXSF6Orf8@ep-polished-art-axlsmvao.c-4.us-east-2.aws.neon.tech/neondb?sslmode=require`);
if (fs.existsSync('./payment-service/.env')) fs.unlinkSync('./payment-service/.env');
if (fs.existsSync('./order-service/.env')) fs.unlinkSync('./order-service/.env');
