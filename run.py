#!/usr/bin/env python3
"""
Script to run the FastAPI application using uvicorn
"""

import uvicorn
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

if __name__ == "__main__":
    # Get configuration from environment variables
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", 8000))
    debug = os.getenv("DEBUG", "True").lower() == "true"
    
    print(f"🚀 Starting Retail Plan Visualizer Backend...")
    print(f"📍 Server will be available at: http://{host}:{port}")
    print(f"📚 API Documentation: http://{host}:{port}/docs")
    print(f"🔧 Debug Mode: {debug}")
    
    # Run the application
    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        reload=debug,
        reload_includes=["*.py"],
        reload_excludes=["tests/*"]
    ) 