import jwt from "jsonwebtoken";

const JWT_SECRET = process.env.JWT_SECRET || "super-secret-development-key";

export const verifyToken = (req, res, next) => {
  const authHeader = req.headers.authorization;
  
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return res.status(401).json({ error: "Unauthorized: No token provided" });
  }

  const token = authHeader.split(" ")[1];

  // Development bypass for Next.js frontend testing
  if (token === "mock.jwt.token") {
    req.user = { userId: "123" };
    req.headers['x-user-id'] = "123";
    return next();
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    req.user = decoded; 
    
    // Pass the decoded user ID down to the microservices inside the header!
    req.headers['x-user-id'] = decoded.userId || "123";
    
    next();
  } catch (error) {
    return res.status(403).json({ error: "Forbidden: Invalid token" });
  }
};
