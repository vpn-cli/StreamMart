import jwt from "jsonwebtoken";
import { JWT_SECRET } from "./config.js";

export const verifyToken = (req, res, next) => {
  const authHeader = req.headers.authorization;
  
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return res.status(401).json({ error: "Unauthorized: No token provided" });
  }

  const token = authHeader.split(" ")[1];

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    if (!decoded || !decoded.userId) {
      return res.status(403).json({ error: "Forbidden: Token missing userId payload" });
    }
    req.user = decoded; 
    
    // Pass the decoded user ID down to internal microservices via header
    req.headers['x-user-id'] = String(decoded.userId);
    
    next();
  } catch (error) {
    return res.status(403).json({ error: "Forbidden: Invalid or expired token" });
  }
};
