const MIN_JWT_SECRET_LENGTH = 32;

const secret = process.env.JWT_SECRET;

if (typeof secret !== "string" || secret.trim().length < MIN_JWT_SECRET_LENGTH) {
  throw new Error(
    `Missing or invalid JWT_SECRET: must be set and at least ${MIN_JWT_SECRET_LENGTH} characters long (generate one with: openssl rand -hex 32)`
  );
}

export const JWT_SECRET = secret;
