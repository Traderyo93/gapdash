const jwt = require('jsonwebtoken');

const JWT_SECRET = process.env.JWT_SECRET || 'aequitas-secret-key-2025';

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];

    if (!token) {
      return res.status(401).json({ error: 'No token provided' });
    }

    const decoded = jwt.verify(token, JWT_SECRET);
    
    res.status(200).json({
      valid: true,
      user: { id: decoded.id, username: decoded.username }
    });

  } catch (error) {
    res.status(401).json({ error: 'Invalid token' });
  }
};
